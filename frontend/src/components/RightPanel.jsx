import { useEffect } from 'react';
import useAppStore from '../stores/appStore';
import { useAnalytics } from '../hooks/useChartData';
import api from '../services/api';
import ExportButtons from './ExportButtons';

function AnalyticsWidget({ title, value, subtitle, color }) {
    return (
        <div className="analytics-widget">
            <div className="analytics-title">{title}</div>
            <div className="analytics-value" style={{ color }}>
                {value}
            </div>
            {subtitle && (
                <div className="text-muted" style={{ fontSize: 11, marginTop: 4 }}>
                    {subtitle}
                </div>
            )}
        </div>
    );
}

function StatRow({ label, value, color }) {
    return (
        <div className="analytics-row">
            <span className="stat-label">{label}</span>
            <span className="stat-value" style={{ color }}>{value}</span>
        </div>
    );
}

export function AnalyticsPanel() {
    const {
        pairAnalytics,
        selectedSymbol,
        pairSymbol,
        uploadedSymbols,
        uploadStats,
        uploadProgress,
        uploadStatus
    } = useAppStore();

    useAnalytics();

    const isUploaded = uploadedSymbols.some(s => s.symbol === selectedSymbol);

    // Format values
    const formatNumber = (num, decimals = 4) => {
        if (num === null || num === undefined) return '--';
        return Number(num).toFixed(decimals);
    };

    if (isUploaded) {
        return (
            <div className="panel-content">
                <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginBottom: 12 }}>
                    Uploaded File: {selectedSymbol.replace('UPLOAD:', '')}
                </div>

                {/* Upload Status */}
                {uploadStatus !== 'complete' && uploadStatus !== 'idle' && (
                    <div className="analytics-widget" style={{ borderColor: 'var(--accent-blue)', borderStyle: 'solid', borderWidth: 1 }}>
                        <div className="analytics-title" style={{ color: 'var(--accent-blue)' }}>Processing</div>
                        <div style={{ fontSize: 13, marginBottom: 8 }}>{uploadProgress}</div>
                        <div className="loading-spinner" style={{ width: 16, height: 16 }} />
                    </div>
                )}

                {/* Price Stats */}
                <div className="analytics-widget">
                    <div className="analytics-title">Price Summary</div>
                    <StatRow label="Min Price" value={formatNumber(uploadStats?.price_min, 2)} />
                    <StatRow label="Max Price" value={formatNumber(uploadStats?.price_max, 2)} />
                    <StatRow label="Mean Price" value={formatNumber(uploadStats?.price_mean, 2)} />
                    <StatRow
                        label="Change %"
                        value={`${formatNumber(uploadStats?.price_change_pct, 2)}%`}
                        color={uploadStats?.price_change_pct >= 0 ? 'var(--accent-green)' : 'var(--accent-red)'}
                    />
                </div>

                {/* Volatility & Duration */}
                <div className="analytics-widget">
                    <div className="analytics-title">Metrics</div>
                    <StatRow label="Tick Count" value={uploadStats?.tick_count} />
                    <StatRow label="Duration (m)" value={formatNumber(uploadStats?.duration_minutes, 1)} />
                    <StatRow label="VWAP" value={formatNumber(uploadStats?.vwap, 2)} />
                    <StatRow label="Std Dev" value={formatNumber(uploadStats?.price_std, 4)} />
                </div>
            </div>
        );
    }

    const zScore = pairAnalytics?.z_score;
    const zScoreColor = !zScore ? 'var(--text-primary)' :
        Math.abs(zScore) > 2 ? 'var(--accent-red)' :
            Math.abs(zScore) > 1 ? 'var(--accent-orange)' :
                'var(--accent-green)';

    const isStationary = pairAnalytics?.is_stationary;
    const stationaryColor = isStationary === null ? 'var(--text-secondary)' :
        isStationary ? 'var(--accent-green)' : 'var(--accent-red)';

    return (
        <div className="panel-content">
            <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginBottom: 12 }}>
                Pair: {selectedSymbol} / {pairSymbol}
            </div>

            {/* Z-Score - Most important metric */}
            <AnalyticsWidget
                title="Z-Score"
                value={formatNumber(zScore, 2)}
                subtitle={Math.abs(zScore) > 2 ? '⚠️ Signal Zone' : 'Normal'}
                color={zScoreColor}
            />

            {/* Spread */}
            <AnalyticsWidget
                title="Spread"
                value={formatNumber(pairAnalytics?.spread, 2)}
                color="var(--text-primary)"
            />

            {/* Detailed Stats */}
            <div className="analytics-widget">
                <div className="analytics-title">Statistics</div>

                <StatRow
                    label="Hedge Ratio (β)"
                    value={formatNumber(pairAnalytics?.hedge_ratio, 4)}
                />
                <StatRow
                    label="Correlation"
                    value={formatNumber(pairAnalytics?.correlation, 3)}
                />
                <StatRow
                    label="ADF p-value"
                    value={formatNumber(pairAnalytics?.adf_pvalue, 4)}
                />
                <StatRow
                    label="Stationary"
                    value={isStationary === null ? '--' : isStationary ? 'Yes' : 'No'}
                    color={stationaryColor}
                />
                <StatRow
                    label="Data Points"
                    value={pairAnalytics?.tick_count || '--'}
                />
            </div>

            {/* Validity Status */}
            <div className="analytics-widget">
                <div className="analytics-title">Data Quality</div>
                <StatRow
                    label="Status"
                    value={pairAnalytics?.validity_status || 'Unknown'}
                    color={pairAnalytics?.validity_status === 'valid' ? 'var(--accent-green)' : 'var(--accent-orange)'}
                />
                <StatRow
                    label="Freshness"
                    value={`${pairAnalytics?.data_freshness_ms || '--'} ms`}
                />
            </div>

            {/* Export Options */}
            <div className="analytics-widget">
                <div className="analytics-title">Export Data</div>
                <ExportButtons />
            </div>
        </div>
    );
}

export function AlertsPanel() {
    const { alertRules, recentAlerts, setShowAlertModal } = useAppStore();

    useEffect(() => {
        // Load alert rules
        api.getAlertRules()
            .then(rules => useAppStore.setState({ alertRules: rules }))
            .catch(console.error);
    }, []);

    return (
        <div className="panel-content">
            <button
                className="btn btn-primary"
                style={{ width: '100%', marginBottom: 16 }}
                onClick={() => setShowAlertModal(true)}
            >
                + Create Alert
            </button>

            <div className="analytics-title">Active Rules</div>
            {alertRules.length === 0 ? (
                <div className="text-muted" style={{ padding: '12px 0' }}>
                    No active alert rules
                </div>
            ) : (
                alertRules.map(rule => (
                    <div key={rule.id} className="analytics-widget">
                        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                            <span>{rule.symbol}</span>
                            <span className="text-muted">{rule.metric}</span>
                        </div>
                        <div style={{ fontSize: 12, marginTop: 4 }}>
                            {rule.operator} {rule.threshold}
                        </div>
                    </div>
                ))
            )}

            <div className="analytics-title" style={{ marginTop: 16 }}>Recent Alerts</div>
            {recentAlerts.length === 0 ? (
                <div className="text-muted" style={{ padding: '12px 0' }}>
                    No recent alerts
                </div>
            ) : (
                recentAlerts.slice(0, 5).map((alert, i) => (
                    <div key={i} className="analytics-widget">
                        <div style={{ fontSize: 12 }}>{alert.message}</div>
                    </div>
                ))
            )}
        </div>
    );
}

export function WatchlistPanel() {
    const {
        symbols,
        selectedSymbol,
        setSelectedSymbol,
        uploadedSymbols,
        setShowUploadModal,
        clearUploadData
    } = useAppStore();

    useEffect(() => {
        api.getSymbols()
            .then(data => useAppStore.setState({ symbols: data }))
            .catch(console.error);

        // Also load any uploaded symbols
        api.getUploadedSymbols()
            .then(data => {
                if (data && data.length > 0) {
                    useAppStore.setState({
                        uploadedSymbols: data.map(s => ({
                            symbol: s.symbol,
                            displayName: s.display_name,
                            tickCount: s.tick_count,
                            uploadedAt: s.uploaded_at,
                            isUploaded: true
                        }))
                    });
                }
            })
            .catch(console.error);
    }, []);

    const handleUploadSelect = (symbol) => {
        clearUploadData();
        setSelectedSymbol(symbol);
    };

    return (
        <div className="panel-content">
            {/* Upload Button */}
            <button
                className="btn btn-primary"
                style={{ width: '100%', marginBottom: 16 }}
                onClick={() => setShowUploadModal(true)}
            >
                📤 Upload Data
            </button>

            {/* Live Symbols */}
            <div className="analytics-title">Symbols</div>
            {symbols.map(sym => (
                <div
                    key={sym.symbol}
                    className="analytics-widget"
                    style={{
                        cursor: 'pointer',
                        background: sym.symbol === selectedSymbol ? 'var(--accent-blue)' : undefined
                    }}
                    onClick={() => setSelectedSymbol(sym.symbol)}
                >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span style={{ fontWeight: 500 }}>{sym.symbol}</span>
                        <div className={`status-indicator ${sym.is_active ? 'connected' : 'disconnected'}`} />
                    </div>
                </div>
            ))}

            {/* Uploaded Symbols */}
            {uploadedSymbols.length > 0 && (
                <>
                    <div className="analytics-title" style={{ marginTop: 16 }}>Uploaded Data</div>
                    {uploadedSymbols.map(sym => (
                        <div
                            key={sym.symbol}
                            className="analytics-widget"
                            style={{
                                cursor: 'pointer',
                                background: sym.symbol === selectedSymbol ? 'var(--accent-blue)' : undefined
                            }}
                            onClick={() => handleUploadSelect(sym.symbol)}
                        >
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <span style={{ fontWeight: 500 }}>📁 {sym.displayName}</span>
                                <span className="text-muted" style={{ fontSize: 11 }}>
                                    {sym.tickCount} ticks
                                </span>
                            </div>
                        </div>
                    ))}
                </>
            )}
        </div>
    );
}

export function RightPanel() {
    const { rightPanelTab, setRightPanelTab } = useAppStore();

    const tabs = [
        { id: 'analytics', label: 'Analytics' },
        { id: 'alerts', label: 'Alerts' },
        { id: 'watchlist', label: 'Watch' },
    ];

    return (
        <aside className="right-panel">
            <div className="panel-header">
                {tabs.map(tab => (
                    <button
                        key={tab.id}
                        className={`timeframe-btn ${rightPanelTab === tab.id ? 'active' : ''}`}
                        onClick={() => setRightPanelTab(tab.id)}
                    >
                        {tab.label}
                    </button>
                ))}
            </div>

            {rightPanelTab === 'analytics' && <AnalyticsPanel />}
            {rightPanelTab === 'alerts' && <AlertsPanel />}
            {rightPanelTab === 'watchlist' && <WatchlistPanel />}
        </aside>
    );
}

export default RightPanel;
