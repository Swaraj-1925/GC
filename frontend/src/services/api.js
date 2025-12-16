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
}

export const api = new ApiService();
export default api;
