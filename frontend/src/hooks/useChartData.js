import { useEffect, useRef, useCallback } from 'react';
import { createChart, CandlestickSeries, HistogramSeries } from 'lightweight-charts';
import useAppStore from '../stores/appStore';
import api from '../services/api';
import wsManager from '../services/websocket';

/**
 * Custom hook for chart data management
 */
export function useChartData() {

    const {
        selectedSymbol,
        timeframe,
        setOhlcData,
        updateOhlcData,
        setLiveCandle,
        setWsStatus
    } = useAppStore();

    // Load initial OHLC data
    const loadData = useCallback(async () => {
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

    // Connect WebSocket for live updates
    useEffect(() => {
        loadData();

        const disconnect = wsManager.connectOHLC(
            selectedSymbol,
            timeframe,
            (candle) => setLiveCandle(candle),
            (status) => setWsStatus(status)
        );

        return () => disconnect();
    }, [selectedSymbol, timeframe, loadData, updateOhlcData, setWsStatus]);

    return { reload: loadData };
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

    const { ohlcData, chartType } = useAppStore();


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
                mode: 1,
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
            },
            timeScale: {
                borderColor: '#363a45',
                timeVisible: true,
                secondsVisible: false,
            },
            handleScroll: { vertTouchDrag: true },
            handleScale: { axisPressedMouseMove: true },
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

        if (!ohlcData || ohlcData.length === 0) {
            console.log('[Chart] No OHLC data to render');
            return;
        }

        console.log('[Chart] Rendering', ohlcData.length, 'candles');
        console.log('[Chart] Sample candle:', ohlcData[ohlcData.length - 1]);

        // Format data for chart - ensure proper numeric types
        // Filter out candles with invalid low values (0, null, infinity)
        const candleData = ohlcData
            .filter(bar => bar.low > 0 && isFinite(Number(bar.low)))
            .map(bar => ({
                time: Number(bar.time),
                open: Number(bar.open),
                high: Number(bar.high),
                low: Number(bar.low),
                close: Number(bar.close),
            }))
            .sort((a, b) => a.time - b.time);  // Ensure sorted by time

        const volumeData = ohlcData
            .filter(bar => bar.low > 0 && isFinite(Number(bar.low)))
            .map(bar => ({
                time: Number(bar.time),
                value: Number(bar.volume) || 0,
                color: Number(bar.close) >= Number(bar.open)
                    ? 'rgba(38, 166, 154, 0.5)'
                    : 'rgba(239, 83, 80, 0.5)',
            }))
            .sort((a, b) => a.time - b.time);

        console.log('[Chart] First candle:', candleData[0]);
        console.log('[Chart] Last candle:', candleData[candleData.length - 1]);

        try {
            candleSeriesRef.current.setData(candleData);
            volumeSeriesRef.current.setData(volumeData);
            console.log('[Chart] Data set successfully');
        } catch (err) {
            console.error('[Chart] Error setting data:', err);
        }
    }, [ohlcData]);

    return { chart: chartRef, candleSeries: candleSeriesRef, volumeSeries: volumeSeriesRef };
}
