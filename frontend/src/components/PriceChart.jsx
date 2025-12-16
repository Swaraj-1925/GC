import { useRef, useEffect } from 'react';
import { useLightweightChart, useChartData } from '../hooks/useChartData';
import { ChartToolbar } from './Header';

export function PriceChart() {
    const containerRef = useRef(null);

    // Load data and setup WebSocket
    useChartData();

    // Initialize chart
    useLightweightChart(containerRef);

    return (
        <div className="chart-area">
            <ChartToolbar />
            <div ref={containerRef} className="chart-container" />
        </div>
    );
}

export default PriceChart;
