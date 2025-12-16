import api from '../services/api';
import useAppStore from '../stores/appStore';

export function ExportButtons() {
    const { selectedSymbol, timeframe } = useAppStore();

    const handleExport = (format) => {
        const url = api.exportData(selectedSymbol, 'ohlc', format, timeframe, 24);
        window.open(url, '_blank');
    };

    return (
        <div style={{ display: 'flex', gap: 8, padding: '12px 0' }}>
            <button className="btn btn-secondary" onClick={() => handleExport('csv')}>
                📥 CSV
            </button>
            <button className="btn btn-secondary" onClick={() => handleExport('json')}>
                📥 JSON
            </button>
            <button className="btn btn-secondary" onClick={() => handleExport('parquet')}>
                📥 Parquet
            </button>
        </div>
    );
}

export default ExportButtons;
