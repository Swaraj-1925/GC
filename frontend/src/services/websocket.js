/**
 * WebSocket Service - Real-time data streaming
 */

const WS_BASE = 'ws://localhost:8080/ws';
const MAX_RECONNECT_ATTEMPTS = 3;
const RECONNECT_DELAY_MS = 3000;

class WebSocketManager {
    constructor() {
        this.connections = new Map();
        this.listeners = new Map();
        this.reconnectAttempts = new Map();
    }

    /**
     * Connect to a WebSocket endpoint
     */
    connect(endpoint, onMessage, onStatus) {
        const key = endpoint;

        // Close existing connection if any
        if (this.connections.has(key)) {
            this.disconnect(endpoint);
        }

        // Reset reconnect attempts for new connections
        this.reconnectAttempts.set(key, 0);
        this.listeners.set(key, { onMessage, onStatus });

        this._doConnect(endpoint, onMessage, onStatus);

        return () => this.disconnect(endpoint);
    }

    _doConnect(endpoint, onMessage, onStatus) {
        const key = endpoint;
        const attempts = this.reconnectAttempts.get(key) || 0;

        if (attempts >= MAX_RECONNECT_ATTEMPTS) {
            console.log(`[WS] Max reconnect attempts (${MAX_RECONNECT_ATTEMPTS}) reached for ${endpoint}`);
            onStatus?.('failed');
            this.listeners.delete(key);
            return;
        }

        onStatus?.('connecting');

        let ws;
        try {
            ws = new WebSocket(`${WS_BASE}${endpoint}`);
        } catch (e) {
            console.error(`[WS] Failed to create WebSocket for ${endpoint}:`, e);
            onStatus?.('failed');
            this.listeners.delete(key);
            return;
        }

        this.connections.set(key, ws);

        ws.onopen = () => {
            console.log(`[WS] Connected: ${endpoint}`);
            this.reconnectAttempts.set(key, 0);
            onStatus?.('connected');
        };

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                onMessage?.(data);
            } catch (e) {
                console.error('[WS] Parse error:', e);
            }
        };

        ws.onerror = (error) => {
            console.error(`[WS] Error on ${endpoint}:`, error);
            onStatus?.('error');
        };

        ws.onclose = (event) => {
            console.log(`[WS] Closed: ${endpoint}`, event.code);
            this.connections.delete(key);

            // Only attempt reconnect if not intentional disconnect and listeners still exist
            if (event.code !== 1000 && this.listeners.has(key)) {
                const currentAttempts = (this.reconnectAttempts.get(key) || 0) + 1;
                this.reconnectAttempts.set(key, currentAttempts);

                if (currentAttempts < MAX_RECONNECT_ATTEMPTS) {
                    onStatus?.('reconnecting');
                    console.log(`[WS] Reconnecting ${endpoint} (attempt ${currentAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);
                    setTimeout(() => {
                        if (this.listeners.has(key)) {
                            const { onMessage: om, onStatus: os } = this.listeners.get(key);
                            this._doConnect(endpoint, om, os);
                        }
                    }, RECONNECT_DELAY_MS);
                } else {
                    onStatus?.('failed');
                    this.listeners.delete(key);
                }
            } else {
                onStatus?.('disconnected');
            }
        };
    }

    disconnect(endpoint) {
        const ws = this.connections.get(endpoint);
        if (ws) {
            ws.close(1000, 'Client disconnect');
            this.connections.delete(endpoint);
        }
        this.listeners.delete(endpoint);
        this.reconnectAttempts.delete(endpoint);
    }

    disconnectAll() {
        for (const [endpoint] of this.connections) {
            this.disconnect(endpoint);
        }
    }

    // Convenience methods
    connectTicks(symbol, onMessage, onStatus) {
        return this.connect(`/ticks/${symbol}`, onMessage, onStatus);
    }

    connectOHLC(symbol, interval, onMessage, onStatus) {
        return this.connect(`/ohlc/${symbol}?interval=${interval}`, onMessage, onStatus);
    }

    connectAnalytics(symbol, onMessage, onStatus) {
        return this.connect(`/analytics/${symbol}`, onMessage, onStatus);
    }

    connectPairAnalytics(symbolA, symbolB, onMessage, onStatus) {
        return this.connect(`/pair/${symbolA}/${symbolB}`, onMessage, onStatus);
    }

    connectAlerts(onMessage, onStatus) {
        return this.connect('/alerts', onMessage, onStatus);
    }
}

export const wsManager = new WebSocketManager();
export default wsManager;

