import { useState } from 'react';
import useAppStore from '../stores/appStore';

const TIMEFRAMES = [
    { value: '1s', label: '1s' },
    { value: '1m', label: '1m' },
    { value: '5m', label: '5m' },
];

const CHART_TYPES = [
    { value: 'candles', label: 'Candles', icon: '📊' },
    { value: 'bars', label: 'Bars', icon: '📈' },
    { value: 'line', label: 'Line', icon: '📉' },
];

export function Header() {
    const {
        selectedSymbol,
        pairSymbol,
        analytics,
        wsStatus,
        setShowAlertModal,
        crosshairOhlc,
        currentCandle
    } = useAppStore();

    const [showSymbolDropdown, setShowSymbolDropdown] = useState(false);

    // Use crosshair OHLC if hovering, otherwise use current candle
    const displayOhlc = crosshairOhlc || currentCandle;

    // Format price with commas
    const formatPrice = (price) => {
        if (!price && price !== 0) return '--';
        return price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };

    // Calculate price change
    const priceChange = analytics?.price_change_pct || 0;
    const isPositive = priceChange >= 0;

    return (
        <header className="header">
            <div className="header-left">
                {/* Logo */}
                <div style={{ fontWeight: 700, fontSize: 16, color: '#2962ff' }}>
                    GemScap
                </div>

                {/* Symbol selector */}
                <div
                    className="symbol-selector"
                    onClick={() => setShowSymbolDropdown(!showSymbolDropdown)}
                >
                    <span className="symbol-name">{selectedSymbol}</span>
                    <span style={{ color: 'var(--text-secondary)', fontSize: 11 }}>
                        / {pairSymbol}
                    </span>
                    <span style={{ fontSize: 10 }}>▼</span>
                </div>

                {/* Connection status */}
                <div className="flex items-center gap-1">
                    <div className={`status-indicator ${wsStatus}`} />
                    <span className="text-muted" style={{ fontSize: 11 }}>
                        {wsStatus === 'connected' ? 'Live' : wsStatus}
                    </span>
                </div>
            </div>

            <div className="header-center">
                {/* OHLC Display */}
                <div className="ohlc-display">
                    <div className="ohlc-item">
                        <span className="ohlc-label">O</span>
                        <span className="ohlc-value">{formatPrice(displayOhlc?.open)}</span>
                    </div>
                    <div className="ohlc-item">
                        <span className="ohlc-label">H</span>
                        <span className="ohlc-value" style={{ color: 'var(--accent-green)' }}>
                            {formatPrice(displayOhlc?.high)}
                        </span>
                    </div>
                    <div className="ohlc-item">
                        <span className="ohlc-label">L</span>
                        <span className="ohlc-value" style={{ color: 'var(--accent-red)' }}>
                            {formatPrice(displayOhlc?.low)}
                        </span>
                    </div>
                    <div className="ohlc-item">
                        <span className="ohlc-label">C</span>
                        <span className="ohlc-value">{formatPrice(displayOhlc?.close)}</span>
                    </div>

                    {/* Price change badge */}
                    <div className={`price-change ${isPositive ? 'positive' : 'negative'}`}>
                        {isPositive ? '+' : ''}{priceChange.toFixed(2)}%
                    </div>
                </div>
            </div>

            <div className="header-right">
                {/* Alert button */}
                <button className="alert-btn" onClick={() => setShowAlertModal(true)}>
                    🔔 Alert
                </button>
            </div>
        </header>
    );
}

export function Sidebar() {
    const { chartType, setChartType } = useAppStore();

    return (
        <aside className="sidebar">
            <button className="sidebar-btn" title="Crosshair">
                ✚
            </button>
            <button className="sidebar-btn" title="Trend Line">
                📐
            </button>
            <button className="sidebar-btn" title="Fibonacci">
                🔶
            </button>

            <div className="sidebar-divider" />

            <button className="sidebar-btn" title="Zoom In">
                🔍+
            </button>
            <button className="sidebar-btn" title="Zoom Out">
                🔍-
            </button>

            <div className="sidebar-divider" />

            {CHART_TYPES.map(type => (
                <button
                    key={type.value}
                    className={`sidebar-btn ${chartType === type.value ? 'active' : ''}`}
                    title={type.label}
                    onClick={() => setChartType(type.value)}
                >
                    {type.icon}
                </button>
            ))}
        </aside>
    );
}

export function ChartToolbar() {
    const { timeframe, setTimeframe, chartType, setChartType } = useAppStore();
    const [showChartTypeMenu, setShowChartTypeMenu] = useState(false);

    const currentChartType = CHART_TYPES.find(t => t.value === chartType);

    return (
        <div className="chart-toolbar">
            {/* Timeframe selector */}
            <div className="timeframe-group">
                {TIMEFRAMES.map(tf => (
                    <button
                        key={tf.value}
                        className={`timeframe-btn ${timeframe === tf.value ? 'active' : ''}`}
                        onClick={() => setTimeframe(tf.value)}
                    >
                        {tf.label}
                    </button>
                ))}
            </div>

            {/* Chart type dropdown */}
            <div className="chart-type-dropdown">
                <button
                    className="dropdown-btn"
                    onClick={() => setShowChartTypeMenu(!showChartTypeMenu)}
                >
                    {currentChartType?.icon} {currentChartType?.label}
                    <span style={{ fontSize: 10 }}>▼</span>
                </button>

                {showChartTypeMenu && (
                    <div className="dropdown-menu">
                        {CHART_TYPES.map(type => (
                            <div
                                key={type.value}
                                className="dropdown-item"
                                onClick={() => {
                                    setChartType(type.value);
                                    setShowChartTypeMenu(false);
                                }}
                            >
                                {type.icon} {type.label}
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* Indicators button */}
            <button className="dropdown-btn">
                📈 Indicators
            </button>
        </div>
    );
}

export default Header;
