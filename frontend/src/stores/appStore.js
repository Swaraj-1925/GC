/**
 * Global App Store - Zustand
 */
import { create } from 'zustand';

export const useAppStore = create((set, get) => ({
    // Symbols
    symbols: [],
    selectedSymbol: 'BTCUSDT',
    pairSymbol: 'ETHUSDT',

    // Chart settings
    timeframe: '1m',
    chartType: 'candles',

    // OHLC data
    ohlcData: [],
    currentCandle: null,
    crosshairOhlc: null,  // OHLC values at crosshair position

    // Analytics
    analytics: null,
    pairAnalytics: null,
    analyticsHistory: [],

    // Connection status
    wsStatus: 'disconnected',

    // Alerts
    alertRules: [],
    recentAlerts: [],

    // UI state
    rightPanelTab: 'analytics', // 'analytics' | 'alerts' | 'watchlist'
    showAlertModal: false,

    // ==================== Upload State ====================
    uploadedSymbols: [],  // List of uploaded symbol info
    showUploadModal: false,
    uploadStatus: 'idle',  // idle | uploading | streaming | complete | error
    uploadProgress: '',  // Current processing step
    uploadOhlc: [],  // OHLC candles for uploaded data
    uploadStats: null,  // Summary statistics
    uploadSpread: [],  // Spread data
    uploadZscore: [],  // Z-score data
    uploadVolatility: [],  // Volatility data

    // Actions
    setSymbols: (symbols) => set({ symbols }),
    setSelectedSymbol: (symbol) => set({ selectedSymbol: symbol }),
    setPairSymbol: (symbol) => set({ pairSymbol: symbol }),

    setTimeframe: (timeframe) => set({ timeframe }),
    setChartType: (chartType) => set({ chartType }),

    setOhlcData: (data) => {
        console.log('[Store] setOhlcData called with', data?.length || 0, 'bars');
        set({ ohlcData: data });
    },
    updateOhlcData: (candle) => {
        const { ohlcData } = get();
        const lastIndex = ohlcData.length - 1;

        if (lastIndex >= 0 && ohlcData[lastIndex].time === candle.time) {
            // Update existing candle with same timestamp
            const updated = [...ohlcData];
            updated[lastIndex] = candle;
            set({ ohlcData: updated, currentCandle: candle });
        } else if (lastIndex < 0 || candle.time > ohlcData[lastIndex].time) {
            // New candle (either first one or newer timestamp) - add to array
            console.log('[Store] Adding new candle at time:', candle.time);
            set({ ohlcData: [...ohlcData, candle], currentCandle: candle });
        } else {
            // Candle is older than existing data, ignore or log warning
            console.warn('[Store] Ignoring out-of-order candle:', candle.time, 'vs last:', ohlcData[lastIndex]?.time);
        }
    },

    setAnalytics: (analytics) => set({ analytics }),
    setPairAnalytics: (pairAnalytics) => set({ pairAnalytics }),
    setAnalyticsHistory: (history) => set({ analyticsHistory: history }),

    setWsStatus: (status) => set({ wsStatus: status }),

    // Live candle from WebSocket
    setLiveCandle: (candle) => {
        console.log(`[WS OHLC ${get().timeframe}] Candle:`,
            candle.time,
            `O: ${candle.open?.toFixed(2)}`,
            `H: ${candle.high?.toFixed(2)}`,
            `L: ${candle.low?.toFixed(2)}`,
            `C: ${candle.close?.toFixed(2)}`,
            candle.trade_count || 0
        );

        // Validate candle data - reject candles with zero low (invalid)
        if (!candle.low || candle.low === 0 || candle.low === Infinity) {
            console.warn('[WS OHLC] Ignoring candle with invalid low:', candle);
            return;
        }

        // Use updateOhlcData to properly merge with existing data
        get().updateOhlcData(candle);
    },

    setAlertRules: (rules) => set({ alertRules: rules }),
    addAlert: (alert) => set((state) => ({
        recentAlerts: [alert, ...state.recentAlerts].slice(0, 50)
    })),

    setRightPanelTab: (tab) => set({ rightPanelTab: tab }),
    setShowAlertModal: (show) => set({ showAlertModal: show }),
    setCrosshairOhlc: (ohlc) => set({ crosshairOhlc: ohlc }),

    // ==================== Upload Actions ====================
    setShowUploadModal: (show) => set({ showUploadModal: show }),

    addUploadedSymbol: (symbolInfo) => set((state) => ({
        uploadedSymbols: [...state.uploadedSymbols, symbolInfo]
    })),

    removeUploadedSymbol: (symbol) => set((state) => ({
        uploadedSymbols: state.uploadedSymbols.filter(s => s.symbol !== symbol)
    })),

    setUploadedSymbols: (symbols) => set({ uploadedSymbols: symbols }),

    setUploadStatus: (status) => set({ uploadStatus: status }),
    setUploadProgress: (progress) => set({ uploadProgress: progress }),

    // Clear upload data when switching away from uploaded symbol
    clearUploadData: () => set({
        uploadOhlc: [],
        uploadStats: null,
        uploadSpread: [],
        uploadZscore: [],
        uploadVolatility: [],
        uploadStatus: 'idle',
        uploadProgress: ''
    }),

    // Append data progressively from SSE stream
    appendUploadOhlc: (candles) => set((state) => ({
        uploadOhlc: [...state.uploadOhlc, ...candles]
    })),

    setUploadStats: (stats) => set({ uploadStats: stats }),

    appendUploadSpread: (data) => set((state) => ({
        uploadSpread: [...state.uploadSpread, ...data]
    })),

    appendUploadZscore: (data) => set((state) => ({
        uploadZscore: [...state.uploadZscore, ...data]
    })),

    appendUploadVolatility: (data) => set((state) => ({
        uploadVolatility: [...state.uploadVolatility, ...data]
    })),

    // Check if current symbol is an uploaded symbol
    isUploadedSymbol: () => {
        const { selectedSymbol, uploadedSymbols } = get();
        return uploadedSymbols.some(s => s.symbol === selectedSymbol);
    },
}));

export default useAppStore;

