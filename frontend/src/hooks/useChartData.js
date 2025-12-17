import { useEffect, useRef, useCallback } from 'react';
import { createChart, CandlestickSeries, HistogramSeries } from 'lightweight-charts';
import useAppStore from '../stores/appStore';
import api from '../services/api';
import wsManager from '../services/websocket';

/**
 * Check if a symbol is an uploaded symbol (has UPLOAD: prefix)
 */
function isUploadedSymbol(symbol) {
    return symbol && symbol.startsWith('UPLOAD:');
}

/**
 * Custom hook for chart data management
 * Handles both live and uploaded symbol data
 */
export function useChartData() {

    const {
        selectedSymbol,
        timeframe,
        setOhlcData,
        updateOhlcData,
        setLiveCandle,
        setWsStatus,
        // Upload-specific state
        clearUploadData,
        setUploadStatus,
        setUploadProgress,
        appendUploadOhlc,
        setUploadStats,
        appendUploadSpread,
        appendUploadZscore,
        appendUploadVolatility
    } = useAppStore();

    // Load initial OHLC data for LIVE symbols
    const loadLiveData = useCallback(async () => {
        if (isUploadedSymbol(selectedSymbol)) return;

        try {
            console.log('[Chart] Loading OHLC data for', selectedSymbol, timeframe);
            const data = await api.getOHLC(selectedSymbol, timeframe, 500);
            console.log('[Chart] Received OHLC data:', data?.length || 0, 'bars');
            if (data && data.length > 0) {
                console.log('[Chart] First bar:', data[0]);
                console.log('[Chart] Last bar:', data[data.length - 1]);
            }
            setOhlcData(data || []);
        } catch (error) {
            console.error('[Chart] Failed to load OHLC data:', error);
            setOhlcData([]);
        }
    }, [selectedSymbol, timeframe, setOhlcData]);

    // Load data for UPLOADED symbols via SSE
    const loadUploadedData = useCallback(() => {
        if (!isUploadedSymbol(selectedSymbol)) return null;

        console.log('[Upload] Starting SSE stream for', selectedSymbol);
        clearUploadData();
        setUploadStatus('streaming');

        const cleanup = api.subscribeUploadStream(
            selectedSymbol,
            (event) => {
                console.log('[SSE] Received:', event.type);

                switch (event.type) {
                    case 'status':
                        setUploadProgress(event.message);
                        break;
                    case 'ohlc':
                        appendUploadOhlc(event.data);
                        break;
                    case 'stats':
                        setUploadStats(event.data);
                        break;
                    case 'spread':
                        appendUploadSpread(event.data);
                        break;
                    case 'zscore':
                        appendUploadZscore(event.data);
                        break;
                    case 'volatility':
                        appendUploadVolatility(event.data);
                        break;
                    case 'complete':
                        setUploadStatus('complete');
                        setUploadProgress('All analytics computed');
                        break;
                    default:
                        console.log('[SSE] Unknown event type:', event.type);
                }
            },
            (error) => {
                console.error('[SSE] Stream error:', error);
                setUploadStatus('error');
                setUploadProgress('Stream error');
            }
        );

        return cleanup;
    }, [selectedSymbol, clearUploadData, setUploadStatus, setUploadProgress,
        appendUploadOhlc, setUploadStats, appendUploadSpread, appendUploadZscore, appendUploadVolatility]);

    // Connect WebSocket for live updates (only for live symbols)
    useEffect(() => {
        if (isUploadedSymbol(selectedSymbol)) {
            // For uploaded symbols, use SSE stream
            const cleanup = loadUploadedData();
            return cleanup || (() => { });
        } else {
            // For live symbols, use WebSocket
            loadLiveData();

            const disconnect = wsManager.connectOHLC(
                selectedSymbol,
                timeframe,
                (candle) => setLiveCandle(candle),
                (status) => setWsStatus(status)
            );

            return () => disconnect();
        }
    }, [selectedSymbol, timeframe, loadLiveData, loadUploadedData, setLiveCandle, setWsStatus]);

    return { reload: loadLiveData };
}

/**
 * Custom hook for analytics data
 */
export function useAnalytics() {
    const {
        selectedSymbol,
        pairSymbol,
        setAnalytics,
        setPairAnalytics,
        setAnalyticsHistory
    } = useAppStore();

    // Load analytics on symbol change
    useEffect(() => {
        const loadAnalytics = async () => {
            try {
                const [single, pair] = await Promise.all([
                    api.getAnalytics(selectedSymbol),
                    api.getPairAnalytics(selectedSymbol, pairSymbol)
                ]);
                setAnalytics(single);
                setPairAnalytics(pair);
            } catch (error) {
                console.error('Failed to load analytics:', error);
            }
        };

        loadAnalytics();

        // Connect WebSocket for live analytics
        const disconnect = wsManager.connectPairAnalytics(
            selectedSymbol,
            pairSymbol,
            (data) => setPairAnalytics(data),
            () => { }
        );

        return () => disconnect();
    }, [selectedSymbol, pairSymbol, setAnalytics, setPairAnalytics]);

    // Load analytics history for charts
    const loadHistory = useCallback(async () => {
        try {
            const history = await api.getPairAnalyticsHistory(selectedSymbol, pairSymbol);
            setAnalyticsHistory(history);
        } catch (error) {
            console.error('Failed to load analytics history:', error);
        }
    }, [selectedSymbol, pairSymbol, setAnalyticsHistory]);

    return { loadHistory };
}

/**
 * Custom hook for Lightweight Charts (v5 API)
 */
export function useLightweightChart(containerRef) {
    const chartRef = useRef(null);
    const candleSeriesRef = useRef(null);
    const volumeSeriesRef = useRef(null);

    const {
        ohlcData,
        uploadOhlc,
        selectedSymbol,
        chartType,
        setCrosshairOhlc
    } = useAppStore();

    // Select appropriate data source
    const displayData = isUploadedSymbol(selectedSymbol) ? uploadOhlc : ohlcData;

    // Initialize chart
    useEffect(() => {
        if (!containerRef.current) return;

        const chart = createChart(containerRef.current, {
            layout: {
                background: { type: 'solid', color: '#131722' },
                textColor: '#d1d4dc',
            },
            grid: {
                vertLines: { color: '#1e222d' },
                horzLines: { color: '#1e222d' },
            },
            crosshair: {
                mode: 0,  // 0 = Normal (free movement), 1 = Magnet (snaps to candles)
                vertLine: {
                    color: '#758696',
                    width: 1,
                    style: 3,
                    labelBackgroundColor: '#2a2e39',
                },
                horzLine: {
                    color: '#758696',
                    width: 1,
                    style: 3,
                    labelBackgroundColor: '#2a2e39',
                },
            },
            rightPriceScale: {
                borderColor: '#363a45',
                autoScale: true,
                scaleMargins: {
                    top: 0.05,
                    bottom: 0.25,  // Leave room for volume
                },
            },
            timeScale: {
                borderColor: '#363a45',
                timeVisible: true,
                secondsVisible: false,
                barSpacing: 10,      // Wider candles for better visibility
                minBarSpacing: 3,    // Min width when zoomed out
                tickMarkFormatter: (time) => {
                    // Format time as HH:MM for every candle
                    const date = new Date(time * 1000);
                    return date.toLocaleTimeString('en-US', {
                        hour: '2-digit',
                        minute: '2-digit',
                        hour12: false
                    });
                },
            },
            // Drag on chart to pan horizontally
            handleScroll: {
                mouseWheel: false,    // Don't use wheel on chart area
                pressedMouseMove: true,
            },
            // Scroll on axes to zoom
            handleScale: {
                mouseWheel: false,    // Don't use wheel on chart area
                axisPressedMouseMove: {
                    time: true,       // Drag on time axis = zoom X (candle width)
                    price: true,      // Drag on price axis = zoom Y (candle height)
                },
            },
        });

        // Create candlestick series (v5 API)
        const candleSeries = chart.addSeries(CandlestickSeries, {
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderUpColor: '#26a69a',
            borderDownColor: '#ef5350',
            wickUpColor: '#26a69a',
            wickDownColor: '#ef5350',
        });

        // Create volume series on separate overlay scale (v5 API)
        const volumeSeries = chart.addSeries(HistogramSeries, {
            priceFormat: { type: 'volume' },
            priceScaleId: 'volume',  // Use separate price scale
        });

        // Configure volume price scale as overlay at bottom
        chart.priceScale('volume').applyOptions({
            scaleMargins: { top: 0.8, bottom: 0 },  // Volume takes bottom 20%
            borderVisible: false,
            visible: false,  // Hide the volume axis
        });

        chartRef.current = chart;
        candleSeriesRef.current = candleSeries;
        volumeSeriesRef.current = volumeSeries;

        // Handle crosshair move to show OHLC values on hover
        chart.subscribeCrosshairMove((param) => {
            if (!param.time || !param.seriesData.size) {
                setCrosshairOhlc(null);
                return;
            }

            // Get candle data at crosshair position
            const candleData = param.seriesData.get(candleSeries);
            if (candleData) {
                setCrosshairOhlc({
                    time: param.time,
                    open: candleData.open,
                    high: candleData.high,
                    low: candleData.low,
                    close: candleData.close,
                });
            }
        });

        // Handle resize
        const handleResize = () => {
            if (containerRef.current && chart) {
                chart.applyOptions({
                    width: containerRef.current.clientWidth,
                    height: containerRef.current.clientHeight,
                });
            }
        };

        window.addEventListener('resize', handleResize);
        handleResize();

        return () => {
            window.removeEventListener('resize', handleResize);
            chart.remove();
        };
    }, [containerRef]);

    // Update data
    useEffect(() => {
        if (!candleSeriesRef.current || !volumeSeriesRef.current) {
            console.log('[Chart] Series not ready yet');
            return;
        }

        if (!displayData || displayData.length === 0) {
            console.log('[Chart] No data to render');
            return;
        }

        console.log('[Chart] Rendering', displayData.length, 'candles');

        // Format data for chart - ensure proper numeric types
        // Filter out candles with invalid low values (0, null, infinity)
        const candleData = displayData
            .filter(bar => bar.low > 0 && isFinite(Number(bar.low)))
            .map(bar => ({
                time: Number(bar.time),
                open: Number(bar.open),
                high: Number(bar.high),
                low: Number(bar.low),
                close: Number(bar.close),
            }))
            .sort((a, b) => a.time - b.time);  // Ensure sorted by time

        const volumeData = displayData
            .filter(bar => bar.low > 0 && isFinite(Number(bar.low)))
            .map(bar => ({
                time: Number(bar.time),
                value: Number(bar.volume) || 0,
                color: Number(bar.close) >= Number(bar.open)
                    ? 'rgba(38, 166, 154, 0.5)'
                    : 'rgba(239, 83, 80, 0.5)',
            }))
            .sort((a, b) => a.time - b.time);

        try {
            candleSeriesRef.current.setData(candleData);
            volumeSeriesRef.current.setData(volumeData);
            console.log('[Chart] Data set successfully');
        } catch (err) {
            console.error('[Chart] Error setting data:', err);
        }
    }, [displayData, chartRef]);

    return { chart: chartRef, candleSeries: candleSeriesRef, volumeSeries: volumeSeriesRef };
}
