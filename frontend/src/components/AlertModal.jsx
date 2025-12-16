import { useState } from 'react';
import useAppStore from '../stores/appStore';
import api from '../services/api';

const METRICS = [
    { value: 'z_score', label: 'Z-Score' },
    { value: 'spread', label: 'Spread' },
    { value: 'correlation', label: 'Correlation' },
    { value: 'hedge_ratio', label: 'Hedge Ratio' },
    { value: 'price', label: 'Price' },
];

const OPERATORS = [
    { value: 'gt', label: '>' },
    { value: 'lt', label: '<' },
    { value: 'gte', label: '>=' },
    { value: 'lte', label: '<=' },
];

export function AlertModal() {
    const { showAlertModal, setShowAlertModal, selectedSymbol, alertRules } = useAppStore();

    const [metric, setMetric] = useState('z_score');
    const [operator, setOperator] = useState('gt');
    const [threshold, setThreshold] = useState('2');
    const [severity, setSeverity] = useState('warning');
    const [loading, setLoading] = useState(false);

    if (!showAlertModal) return null;

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);

        try {
            const rule = await api.createAlertRule({
                symbol: selectedSymbol,
                metric,
                operator,
                threshold: parseFloat(threshold),
                severity,
            });

            useAppStore.setState({
                alertRules: [...alertRules, rule],
                showAlertModal: false
            });
        } catch (error) {
            console.error('Failed to create alert:', error);
            alert('Failed to create alert rule');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="modal-overlay" onClick={() => setShowAlertModal(false)}>
            <div className="modal" onClick={e => e.stopPropagation()}>
                <div className="modal-header">
                    <h3 className="modal-title">Create Alert</h3>
                    <button className="modal-close" onClick={() => setShowAlertModal(false)}>
                        ✕
                    </button>
                </div>

                <form onSubmit={handleSubmit}>
                    <div className="modal-body">
                        <div className="form-group">
                            <label className="form-label">Symbol</label>
                            <input
                                type="text"
                                className="form-input"
                                value={selectedSymbol}
                                disabled
                            />
                        </div>

                        <div className="form-group">
                            <label className="form-label">Metric</label>
                            <select
                                className="form-select"
                                value={metric}
                                onChange={e => setMetric(e.target.value)}
                            >
                                {METRICS.map(m => (
                                    <option key={m.value} value={m.value}>{m.label}</option>
                                ))}
                            </select>
                        </div>

                        <div style={{ display: 'flex', gap: 12 }}>
                            <div className="form-group" style={{ flex: 1 }}>
                                <label className="form-label">Condition</label>
                                <select
                                    className="form-select"
                                    value={operator}
                                    onChange={e => setOperator(e.target.value)}
                                >
                                    {OPERATORS.map(op => (
                                        <option key={op.value} value={op.value}>{op.label}</option>
                                    ))}
                                </select>
                            </div>

                            <div className="form-group" style={{ flex: 2 }}>
                                <label className="form-label">Threshold</label>
                                <input
                                    type="number"
                                    step="any"
                                    className="form-input"
                                    value={threshold}
                                    onChange={e => setThreshold(e.target.value)}
                                    required
                                />
                            </div>
                        </div>

                        <div className="form-group">
                            <label className="form-label">Severity</label>
                            <select
                                className="form-select"
                                value={severity}
                                onChange={e => setSeverity(e.target.value)}
                            >
                                <option value="info">Info</option>
                                <option value="warning">Warning</option>
                                <option value="critical">Critical</option>
                            </select>
                        </div>

                        {/* Preview */}
                        <div style={{
                            background: 'var(--bg-tertiary)',
                            padding: 12,
                            borderRadius: 4,
                            fontSize: 13
                        }}>
                            <strong>Rule Preview:</strong><br />
                            Alert when {METRICS.find(m => m.value === metric)?.label} {' '}
                            {OPERATORS.find(o => o.value === operator)?.label} {threshold}
                        </div>
                    </div>

                    <div className="modal-footer">
                        <button
                            type="button"
                            className="btn btn-secondary"
                            onClick={() => setShowAlertModal(false)}
                        >
                            Cancel
                        </button>
                        <button
                            type="submit"
                            className="btn btn-primary"
                            disabled={loading}
                        >
                            {loading ? 'Creating...' : 'Create Alert'}
                        </button>
                    </div>
                </form>
            </div>
        </div>
    );
}

export default AlertModal;
