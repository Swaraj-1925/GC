/**
 * API Service - REST API calls to backend
 */

const API_BASE = 'http://localhost:8080/api';

class ApiService {
    async get(endpoint) {
        const response = await fetch(`${API_BASE}${endpoint}`);
        if (!response.ok) {
            throw new Error(`API Error: ${response.status} ${response.statusText}`);
        }
        return response.json();
    }

    async post(endpoint, data) {
        const response = await fetch(`${API_BASE}${endpoint}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });
        if (!response.ok) {
            throw new Error(`API Error: ${response.status} ${response.statusText}`);
        }
        return response.json();
    }

    async delete(endpoint) {
        const response = await fetch(`${API_BASE}${endpoint}`, { method: 'DELETE' });
        if (!response.ok) {
            throw new Error(`API Error: ${response.status} ${response.statusText}`);
        }
        return response.json();
    }

    // Symbol endpoints
    getSymbols() {
        return this.get('/symbols');
    }

    // Analytics endpoints
    getAnalytics(symbol) {
        return this.get(`/analytics/${symbol}`);
    }

    getPairAnalytics(symbolA, symbolB) {
        return this.get(`/analytics/pair/${symbolA}/${symbolB}`);
    }

    getPairAnalyticsHistory(symbolA, symbolB, fromTime, toTime, limit = 1000) {
        const params = new URLSearchParams();
        if (fromTime) params.append('from_time', fromTime);
        if (toTime) params.append('to_time', toTime);
        params.append('limit', limit);
        return this.get(`/analytics/history/${symbolA}/${symbolB}?${params}`);
    }

    computeAnalytics(symbolA, symbolB, windowSize = 100, regressionType = 'ols', zScoreWindow = 20) {
        return this.post('/analytics/compute', {
            symbol_a: symbolA,
            symbol_b: symbolB,
            window_size: windowSize,
            regression_type: regressionType,
            z_score_window: zScoreWindow,
        });
    }

    // OHLC endpoints
    getOHLC(symbol, interval = '1m', limit = 100, fromTime, toTime) {
        const params = new URLSearchParams({ interval, limit: String(limit) });
        if (fromTime) params.append('from_time', fromTime);
        if (toTime) params.append('to_time', toTime);
        return this.get(`/ohlc/${symbol}?${params}`);
    }

    getTickHistory(symbol, limit = 100, fromTime, toTime) {
        const params = new URLSearchParams({ limit: String(limit) });
        if (fromTime) params.append('from_time', fromTime);
        if (toTime) params.append('to_time', toTime);
        return this.get(`/history/${symbol}?${params}`);
    }

    // Alert endpoints
    getAlertRules() {
        return this.get('/alerts/rules');
    }

    createAlertRule(rule) {
        return this.post('/alerts/rules', rule);
    }

    deleteAlertRule(ruleId) {
        return this.delete(`/alerts/rules/${ruleId}`);
    }

    getAlertsHistory(symbol, limit = 50) {
        const params = new URLSearchParams({ limit: String(limit) });
        if (symbol) params.append('symbol', symbol);
        return this.get(`/alerts/history?${params}`);
    }

    // Export endpoints
    exportData(symbol, dataType = 'ohlc', format = 'csv', interval = '1m', hours = 1) {
        const params = new URLSearchParams({
            data_type: dataType,
            format,
            interval,
            hours: String(hours),
        });
        return `${API_BASE}/export/${symbol}?${params}`;
    }

    // Summary endpoint
    getSummary() {
        return this.get('/summary');
    }

    // ==================== Upload Endpoints ====================

    /**
     * Upload an NDJSON file with tick data
     * @param {File} file - The NDJSON file
     * @param {string} symbolName - Unique symbol name for the upload
     * @returns {Promise<{session_id, symbol, tick_count, message}>}
     */
    async uploadFile(file, symbolName) {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('symbol_name', symbolName);

        const response = await fetch(`${API_BASE}/upload`, {
            method: 'POST',
            body: formData,
        });

        if (!response.ok) {
            const error = await response.json().catch(() => ({ detail: response.statusText }));
            throw new Error(error.detail || `Upload failed: ${response.status}`);
        }

        return response.json();
    }

    /**
     * Get list of uploaded symbols
     * @returns {Promise<Array<{symbol, display_name, tick_count, uploaded_at}>>}
     */
    getUploadedSymbols() {
        return this.get('/upload/symbols');
    }

    /**
     * Delete an uploaded symbol
     * @param {string} symbol - Symbol to delete
     */
    deleteUploadedSymbol(symbol) {
        return this.delete(`/upload/${encodeURIComponent(symbol)}`);
    }

    /**
     * Get OHLC data for an uploaded symbol
     * @param {string} symbol - The uploaded symbol
     * @param {string} interval - Candle interval (1s, 1m, 5m)
     */
    getUploadOHLC(symbol, interval = '1m') {
        return this.get(`/upload/${encodeURIComponent(symbol)}/ohlc?interval=${interval}`);
    }

    /**
     * Subscribe to upload analytics stream (SSE)
     * @param {string} symbol - The uploaded symbol
     * @param {function} onData - Callback for each data event
     * @param {function} onError - Callback for errors
     * @returns {function} - Cleanup function to close connection
     */
    subscribeUploadStream(symbol, onData, onError) {
        const encodedSymbol = encodeURIComponent(symbol);
        const eventSource = new EventSource(`${API_BASE}/upload/${encodedSymbol}/stream`);

        eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                onData(data);
            } catch (e) {
                console.error('[SSE] Parse error:', e);
            }
        };

        eventSource.onerror = (error) => {
            console.error('[SSE] Error:', error);
            onError?.(error);
            eventSource.close();
        };

        // Return cleanup function
        return () => {
            eventSource.close();
        };
    }
}

export const api = new ApiService();
export default api;

