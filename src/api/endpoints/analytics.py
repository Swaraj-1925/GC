import io
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query, Response, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ...shared.config import get_settings, RedisKeys
from ...shared.models import AlertRule, AlertSeverity, DataValidityStatus
from ...shared.db.redis_client import RedisClient
from ...shared.db.timescale_client import TimescaleClient
from .response_models import *

router = APIRouter()

# ==================== DI ====================
async def get_redis(request: Request) -> RedisClient:
    return request.app.state.redis

async def get_timescale(request: Request) -> TimescaleClient:
    return request.app.state.timescale

@router.get("/symbols", response_model=List[SymbolInfo])
async def get_symbols(redis: RedisClient = Depends(get_redis)) -> List[SymbolInfo]:
    """
    Get list of available trading symbols.

    Returns list of symbols with their current status.
    """
    settings = get_settings()

    symbols = []
    for symbol in settings.SYMBOLS:
        symbol_upper = symbol.upper()

        # Check if we have recent data
        analytics_key = RedisKeys.analytics_state(symbol_upper)
        data = await redis.hash_get_all(analytics_key)

        last_update = None
        if data and "timestamp" in data:
            last_update = int(data["timestamp"])

        # Consider active if updated in last 10 seconds
        is_active = last_update is not None and (
            int(time.time() * 1000) - last_update < 10000
        )

        symbols.append(SymbolInfo(
            symbol=symbol_upper,
            is_active=is_active,
            last_update=last_update
        ))

    return symbols


@router.get("/analytics/{symbol}", response_model=AnalyticsResponse)
async def get_analytics(
    symbol: str,
    redis: RedisClient = Depends(get_redis)
) -> AnalyticsResponse:
    """
    Get latest analytics for a symbol.

    Returns the most recent analytics snapshot from Redis.
    """
    analytics_key = RedisKeys.analytics_state(symbol.upper())

    data = await redis.hash_get_all(analytics_key)

    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No analytics data for symbol {symbol}"
        )

    # Parse Redis hash data
    try:
        return AnalyticsResponse(
            symbol=data.get("symbol", symbol.upper()),
            pair_symbol=data.get("pair_symbol"),
            timestamp=int(data.get("timestamp", 0)),
            last_price=float(data["last_price"]) if data.get("last_price") else None,
            price_change_pct=float(data["price_change_pct"]) if data.get("price_change_pct") else None,
            vwap=float(data["vwap"]) if data.get("vwap") else None,
            spread=float(data["spread"]) if data.get("spread") else None,
            hedge_ratio=float(data["hedge_ratio"]) if data.get("hedge_ratio") else None,
            z_score=float(data["z_score"]) if data.get("z_score") else None,
            correlation=float(data["correlation"]) if data.get("correlation") else None,
            adf_statistic=float(data["adf_statistic"]) if data.get("adf_statistic") else None,
            adf_pvalue=float(data["adf_pvalue"]) if data.get("adf_pvalue") else None,
            is_stationary=data.get("is_stationary") == "1" if data.get("is_stationary") else None,
            data_freshness_ms=int(data.get("data_freshness_ms", 0)),
            validity_status=data.get("validity_status", "unknown"),
            tick_count=int(data.get("tick_count", 0))
        )
    except (ValueError, KeyError) as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error parsing analytics data: {e}"
        )


@router.get("/analytics/pair/{symbol_a}/{symbol_b}", response_model=AnalyticsResponse)
async def get_pair_analytics(
    symbol_a: str,
    symbol_b: str,
    redis: RedisClient = Depends(get_redis)
) -> AnalyticsResponse:
    """
    Get pair analytics (spread, hedge ratio, z-score, correlation).

    Returns analytics for the trading pair.
    """
    pair_key = f"{symbol_a.upper()}:{symbol_b.upper()}"
    analytics_key = RedisKeys.analytics_state(pair_key)

    data = await redis.hash_get_all(analytics_key)

    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No pair analytics for {pair_key}"
        )

    try:
        return AnalyticsResponse(
            symbol=data.get("symbol", symbol_a.upper()),
            pair_symbol=data.get("pair_symbol", symbol_b.upper()),
            timestamp=int(data.get("timestamp", 0)),
            last_price=float(data["last_price"]) if data.get("last_price") else None,
            price_change_pct=None,
            vwap=None,
            spread=float(data["spread"]) if data.get("spread") else None,
            hedge_ratio=float(data["hedge_ratio"]) if data.get("hedge_ratio") else None,
            z_score=float(data["z_score"]) if data.get("z_score") else None,
            correlation=float(data["correlation"]) if data.get("correlation") else None,
            adf_statistic=float(data["adf_statistic"]) if data.get("adf_statistic") else None,
            adf_pvalue=float(data["adf_pvalue"]) if data.get("adf_pvalue") else None,
            is_stationary=data.get("is_stationary") == "1" if data.get("is_stationary") else None,
            data_freshness_ms=int(data.get("data_freshness_ms", 0)),
            validity_status=data.get("validity_status", "unknown"),
            tick_count=int(data.get("tick_count", 0))
        )
    except (ValueError, KeyError) as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error parsing pair analytics: {e}"
        )

@router.get("/analytics/history/{symbol_a}/{symbol_b}", response_model=List[PairAnalyticsHistoryPoint])
async def get_pair_analytics_history(
    symbol_a: str,
    symbol_b: str,
    from_time: Optional[int] = Query(None, description="Start timestamp (unix seconds)"),
    to_time: Optional[int] = Query(None, description="End timestamp (unix seconds)"),
    limit: int = Query(1000, ge=1, le=5000, description="Maximum data points to return"),
    timescale: TimescaleClient = Depends(get_timescale)
) -> List[PairAnalyticsHistoryPoint]:
    """
    Get historical pair analytics time series for charting.

    Returns time series of spread, z_score, hedge_ratio, correlation
    for plotting on charts.
    """
    now = datetime.utcnow()
    end_time = datetime.fromtimestamp(to_time) if to_time else now
    start_time = datetime.fromtimestamp(from_time) if from_time else now - timedelta(hours=24)

    try:
        data = await timescale.get_pair_analytics_history(
            symbol_a.upper(),
            symbol_b.upper(),
            start_time,
            end_time,
            limit
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching analytics history: {e}"
        )

    # Convert to response format
    return [
        PairAnalyticsHistoryPoint(
            time=int(row["time"].timestamp()) if hasattr(row["time"], "timestamp") else int(row["time"]),
            spread=row.get("spread"),
            hedge_ratio=row.get("hedge_ratio"),
            z_score=row.get("z_score"),
            correlation=row.get("correlation"),
            adf_pvalue=row.get("adf_pvalue"),
            is_stationary=row.get("is_stationary")
        )
        for row in data
    ]


class OnDemandAnalyticsRequest(BaseModel):
    """Request for on-demand analytics computation."""
    symbol_a: str
    symbol_b: str
    window_size: int = 100
    regression_type: str = "ols"  # ols or tls
    z_score_window: int = 20


class OnDemandAnalyticsResponse(BaseModel):
    """Response for on-demand analytics computation."""
    symbol: str
    pair_symbol: str
    timestamp: int
    window_size: int
    regression_type: str
    hedge_ratio: Optional[float] = None
    spread: Optional[float] = None
    z_score: Optional[float] = None
    correlation: Optional[float] = None
    adf_statistic: Optional[float] = None
    adf_pvalue: Optional[float] = None
    is_stationary: Optional[bool] = None
    data_points: int
    message: Optional[str] = None


@router.post("/analytics/compute", response_model=OnDemandAnalyticsResponse)
async def compute_analytics_on_demand(
    request: OnDemandAnalyticsRequest,
    timescale: TimescaleClient = Depends(get_timescale)
) -> OnDemandAnalyticsResponse:
    """
    Compute pair analytics on-demand with custom parameters.

    Allows frontend to request analytics with specific:
    - window_size: Number of data points for calculations (20-500)
    - regression_type: 'ols' (Ordinary Least Squares) or 'tls' (Total Least Squares)
    - z_score_window: Lookback window for z-score calculation

    This reads historical ticks and computes analytics on the fly.
    """
    import numpy as np

    # Validate parameters
    window_size = max(20, min(500, request.window_size))
    z_score_window = max(5, min(100, request.z_score_window))

    if request.regression_type not in ("ols", "tls"):
        raise HTTPException(400, "regression_type must be 'ols' or 'tls'")

    now = datetime.utcnow()
    start_time = now - timedelta(hours=1)  # Get last hour of data

    try:
        # Fetch recent ticks for both symbols
        ticks_a = await timescale.get_ticks(request.symbol_a.upper(), start_time, now, window_size * 2)
        ticks_b = await timescale.get_ticks(request.symbol_b.upper(), start_time, now, window_size * 2)
    except Exception as e:
        raise HTTPException(500, f"Error fetching tick data: {e}")

    if len(ticks_a) < 20 or len(ticks_b) < 20:
        return OnDemandAnalyticsResponse(
            symbol=request.symbol_a.upper(),
            pair_symbol=request.symbol_b.upper(),
            timestamp=int(time.time() * 1000),
            window_size=window_size,
            regression_type=request.regression_type,
            data_points=min(len(ticks_a), len(ticks_b)),
            message="Insufficient data for analytics computation"
        )

    # Align data by taking most recent N points
    min_len = min(len(ticks_a), len(ticks_b), window_size)
    prices_a = np.array([t["price"] for t in ticks_a[:min_len]])
    prices_b = np.array([t["price"] for t in ticks_b[:min_len]])

    # Compute hedge ratio
    if request.regression_type == "ols":
        # OLS: y = beta * x + alpha
        x_mean = np.mean(prices_b)
        y_mean = np.mean(prices_a)
        numerator = np.sum((prices_b - x_mean) * (prices_a - y_mean))
        denominator = np.sum((prices_b - x_mean) ** 2)
        hedge_ratio = float(numerator / denominator) if denominator != 0 else 0.0
    else:
        # TLS (Total Least Squares / Orthogonal Regression)
        x_centered = prices_b - np.mean(prices_b)
        y_centered = prices_a - np.mean(prices_a)

        # SVD-based TLS
        data_matrix = np.vstack([x_centered, y_centered]).T
        _, _, Vt = np.linalg.svd(data_matrix)
        hedge_ratio = float(-Vt[-1, 0] / Vt[-1, 1]) if Vt[-1, 1] != 0 else 0.0

    # Compute spread
    spread_series = prices_a - hedge_ratio * prices_b
    current_spread = float(spread_series[-1])

    # Compute z-score
    window = min(z_score_window, len(spread_series))
    recent = spread_series[-window:]
    mean = np.mean(recent)
    std = np.std(recent)
    z_score = float((spread_series[-1] - mean) / std) if std != 0 else 0.0

    # Correlation
    corr = np.corrcoef(prices_a, prices_b)[0, 1]
    correlation = float(corr) if not np.isnan(corr) else 0.0

    # ADF test (if statsmodels available)
    adf_statistic = None
    adf_pvalue = None
    is_stationary = None
    try:
        from statsmodels.tsa.stattools import adfuller
        if len(spread_series) >= 50:
            result = adfuller(spread_series, autolag='AIC')
            adf_statistic = float(result[0])
            adf_pvalue = float(result[1])
            is_stationary = adf_pvalue < 0.05
    except ImportError:
        pass
    except Exception:
        pass

    return OnDemandAnalyticsResponse(
        symbol=request.symbol_a.upper(),
        pair_symbol=request.symbol_b.upper(),
        timestamp=int(time.time() * 1000),
        window_size=window_size,
        regression_type=request.regression_type,
        hedge_ratio=hedge_ratio,
        spread=current_spread,
        z_score=z_score,
        correlation=correlation,
        adf_statistic=adf_statistic,
        adf_pvalue=adf_pvalue,
        is_stationary=is_stationary,
        data_points=min_len
    )

@router.get("/ohlc/{symbol}", response_model=List[OHLCBar])
async def get_ohlc(
    symbol: str,
    interval: str = Query("1m", description="Candle interval (1s, 1m, 5m)"),
    limit: int = Query(500, ge=1, le=1500, description="Number of bars to return"),
    from_time: Optional[int] = Query(None, description="Start timestamp (seconds)"),
    to_time: Optional[int] = Query(None, description="End timestamp (seconds)"),
    redis: RedisClient = Depends(get_redis),
    timescale: TimescaleClient = Depends(get_timescale)
) -> List[OHLCBar]:
    """
    Get OHLC candlestick data for charting.

    Priority:
    1. Redis TimeSeries (hot storage, last 24h of data)
    2. TimescaleDB pre-computed OHLC
    3. TimescaleDB computed from raw ticks

    Returns bars in TradingView-compatible format.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Interval mapping
    interval_map = {"1s": 1, "1m": 60, "5m": 300}
    interval_secs = interval_map.get(interval, 60)
    interval_ms = interval_secs * 1000

    # Default time range: last 24 hours
    now_ms = int(time.time() * 1000)
    to_ts_ms = (to_time * 1000) if to_time else now_ms
    from_ts_ms = (from_time * 1000) if from_time else now_ms - (24 * 60 * 60 * 1000)
    
    logger.info(f"[OHLC] Request: {symbol} interval={interval} from={from_ts_ms} to={to_ts_ms}")

    bars = []

    # ===== Option 0: Try Redis Streams (tick stream) - Best coverage =====
    try:
        stream_key = RedisKeys.tick_stream(symbol.upper())
        logger.info(f"[OHLC] Trying Redis Stream: {stream_key}")
        
        # Read tick stream using XRANGE with timestamp-based IDs
        start_id = f"{from_ts_ms}-0"
        end_id = f"{to_ts_ms}-0"
        
        ticks = await redis.stream_xrange(stream_key, start_id, end_id)
        logger.info(f"[OHLC] Redis Stream returned {len(ticks) if ticks else 0} ticks")
        
        if ticks and len(ticks) > 0:
            # Group ticks into OHLC buckets
            buckets = {}
            for entry_id, tick_data in ticks:
                # Parse tick data
                ts = int(tick_data.get('timestamp', entry_id.split('-')[0]))
                price = float(tick_data.get('price', 0))
                qty = float(tick_data.get('qty', 0))
                
                if price <= 0:
                    continue
                
                # Calculate bucket start time (seconds)
                bucket_time = (ts // 1000 // interval_secs) * interval_secs
                
                if bucket_time not in buckets:
                    buckets[bucket_time] = {
                        'time': bucket_time,
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': qty,
                        'first_ts': ts,
                        'last_ts': ts,
                        'count': 1
                    }
                else:
                    b = buckets[bucket_time]
                    b['high'] = max(b['high'], price)
                    b['low'] = min(b['low'], price)
                    b['volume'] = b.get('volume', 0) + qty
                    # Update open if this is earlier
                    if ts < b['first_ts']:
                        b['open'] = price
                        b['first_ts'] = ts
                    # Update close if this is later
                    if ts >= b['last_ts']:
                        b['close'] = price
                        b['last_ts'] = ts
                    b['count'] += 1
            
            # Sort by time and take last N bars
            sorted_bars = sorted(buckets.values(), key=lambda x: x['time'])
            bars = sorted_bars[-limit:] if len(sorted_bars) > limit else sorted_bars
            logger.info(f"[OHLC] Redis Stream computed {len(bars)} bars from {len(buckets)} buckets")
            
    except Exception as e:
        logger.warning(f"[OHLC] Redis Stream failed: {e}")

    # ===== Option 1: Try Redis TimeSeries (fallback) =====
    if not bars:
        try:
            ts_key = RedisKeys.price_timeseries(symbol.upper())
            logger.info(f"[OHLC] Trying Redis TimeSeries: {ts_key}")
            
            # Get raw price data from Redis TimeSeries
            raw_data = await redis.ts_range(ts_key, from_ts_ms, to_ts_ms)
            
            logger.info(f"[OHLC] Redis returned {len(raw_data) if raw_data else 0} samples")
            
            if raw_data and len(raw_data) > 0:
                # Group prices into time buckets to compute OHLC
                buckets = {}
                for point in raw_data:
                    ts = int(point[0])  # timestamp in ms
                    price = float(point[1])
                    
                    # Calculate bucket start time (seconds)
                    bucket_time = (ts // 1000 // interval_secs) * interval_secs
                    
                    if bucket_time not in buckets:
                        buckets[bucket_time] = {
                            'time': bucket_time,
                            'open': price,
                            'high': price,
                            'low': price,
                            'close': price,
                            'first_ts': ts,
                            'last_ts': ts,
                            'count': 1
                        }
                    else:
                        b = buckets[bucket_time]
                        b['high'] = max(b['high'], price)
                        b['low'] = min(b['low'], price)
                        # Update open if this is earlier
                        if ts < b['first_ts']:
                            b['open'] = price
                            b['first_ts'] = ts
                        # Update close if this is later
                        if ts >= b['last_ts']:
                            b['close'] = price
                            b['last_ts'] = ts
                        b['count'] += 1
                
                # Sort by time and take last N bars
                sorted_bars = sorted(buckets.values(), key=lambda x: x['time'])
                bars = sorted_bars[-limit:] if len(sorted_bars) > limit else sorted_bars
                logger.info(f"[OHLC] Redis TS computed {len(bars)} bars from {len(buckets)} buckets")
                
        except Exception as e:
            logger.warning(f"[OHLC] Redis TimeSeries failed: {e}")

    # ===== Option 2: Try TimescaleDB pre-computed OHLC =====
    if not bars:
        now = datetime.utcnow()
        end_time = datetime.fromtimestamp(to_time) if to_time else now
        start_time = datetime.fromtimestamp(from_time) if from_time else now - timedelta(hours=24)
        
        try:
            bars = await timescale.get_ohlc(
                symbol.upper(),
                interval,
                start_time,
                end_time,
                limit
            )
        except Exception as e:
            pass

    # ===== Option 3: Compute from raw ticks in TimescaleDB =====
    if not bars:
        now = datetime.utcnow()
        end_time = datetime.fromtimestamp(to_time) if to_time else now
        start_time = datetime.fromtimestamp(from_time) if from_time else now - timedelta(hours=24)
        
        try:
            bars = await timescale.compute_ohlc_from_ticks(
                symbol.upper(),
                interval_secs,
                start_time,
                end_time,
                limit
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Could not get OHLC data: {e}"
            )

    # Convert to TradingView format (time in seconds)
    return [
        OHLCBar(
            time=int(bar["time"].timestamp()) if hasattr(bar.get("time"), "timestamp") else int(bar.get("time", 0)),
            open=float(bar["open"]),
            high=float(bar["high"]),
            low=float(bar["low"]),
            close=float(bar["close"]),
            volume=float(bar.get("volume", bar.get("count", 0)))
        )
        for bar in bars
        if bar.get("low", 0) > 0  # Filter invalid bars
    ]


@router.get("/history/{symbol}")
async def get_tick_history(
    symbol: str,
    limit: int = Query(100, ge=1, le=10000),
    from_time: Optional[int] = Query(None, description="Start timestamp (ms)"),
    to_time: Optional[int] = Query(None, description="End timestamp (ms)"),
    timescale: TimescaleClient = Depends(get_timescale)
) -> List[Dict[str, Any]]:
    """
    Get historical tick data.

    Returns raw tick data from TimescaleDB.
    """

    now = datetime.utcnow()
    end_time = datetime.fromtimestamp(to_time / 1000) if to_time else now
    start_time = datetime.fromtimestamp(from_time / 1000) if from_time else now - timedelta(hours=1)

    try:
        ticks = await timescale.get_ticks(symbol.upper(), start_time, end_time, limit)
        return ticks
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching tick history: {e}"
        )


# ==================== Alert Management ====================

# In-memory alert rules store (would use Redis in production)
_alert_rules: Dict[str, AlertRule] = {}


@router.get("/alerts/rules", response_model=List[AlertRuleResponse])
async def get_alert_rules() -> List[AlertRuleResponse]:
    """Get all configured alert rules."""
    return [
        AlertRuleResponse(
            id=rule.id,
            symbol=rule.symbol,
            metric=rule.metric,
            operator=rule.operator,
            threshold=rule.threshold,
            severity=rule.severity.value,
            enabled=rule.enabled
        )
        for rule in _alert_rules.values()
    ]


@router.post("/alerts/rules", response_model=AlertRuleResponse)
async def create_alert_rule(rule: AlertRuleCreate) -> AlertRuleResponse:
    """Create a new alert rule."""
    import uuid

    # Validate operator
    if rule.operator not in ("gt", "lt", "gte", "lte", "eq"):
        raise HTTPException(400, "Invalid operator")

    # Validate metric
    valid_metrics = {"z_score", "correlation", "spread", "hedge_ratio", "price", "data_freshness_ms"}
    if rule.metric not in valid_metrics:
        raise HTTPException(400, f"Invalid metric. Must be one of: {valid_metrics}")

    # Create rule
    rule_id = str(uuid.uuid4())[:8]
    alert_rule = AlertRule(
        id=rule_id,
        symbol=rule.symbol.upper(),
        metric=rule.metric,
        operator=rule.operator,
        threshold=rule.threshold,
        severity=AlertSeverity(rule.severity),
        enabled=True
    )

    _alert_rules[rule_id] = alert_rule

    return AlertRuleResponse(
        id=alert_rule.id,
        symbol=alert_rule.symbol,
        metric=alert_rule.metric,
        operator=alert_rule.operator,
        threshold=alert_rule.threshold,
        severity=alert_rule.severity.value,
        enabled=alert_rule.enabled
    )


@router.delete("/alerts/rules/{rule_id}")
async def delete_alert_rule(rule_id: str) -> Dict[str, str]:
    """Delete an alert rule."""
    if rule_id not in _alert_rules:
        raise HTTPException(404, "Rule not found")

    del _alert_rules[rule_id]
    return {"message": f"Rule {rule_id} deleted"}


@router.get("/alerts/history")
async def get_alerts_history(
    symbol: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    timescale: TimescaleClient = Depends(get_timescale)
) -> List[Dict[str, Any]]:
    """Get alert history from TimescaleDB."""

    try:
        alerts = await timescale.get_alerts_history(
            symbol=symbol.upper() if symbol else None,
            limit=limit
        )
        return alerts
    except Exception as e:
        raise HTTPException(500, f"Error fetching alerts: {e}")


# ==================== Data Export ====================

@router.get("/export/{symbol}")
async def export_data(
    symbol: str,
    data_type: str = Query("ticks", description="Data type: ticks or ohlc"),
    format: str = Query("csv", description="Export format: csv, json, or parquet"),
    interval: str = Query("1m", description="OHLC interval (for ohlc data type)"),
    hours: int = Query(1, ge=1, le=24, description="Hours of data to export"),
    timescale: TimescaleClient = Depends(get_timescale)
) -> StreamingResponse:
    """
    Export historical data in various formats.

    Returns data as a downloadable file.
    """

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)

    # Fetch data
    if data_type == "ticks":
        data = await timescale.get_ticks(symbol.upper(), start_time, end_time)
    elif data_type == "ohlc":
        data = await timescale.get_ohlc(symbol.upper(), interval, start_time, end_time)
    else:
        raise HTTPException(400, "Invalid data_type. Use 'ticks' or 'ohlc'")

    if not data:
        raise HTTPException(404, "No data found for the specified range")

    # Convert to DataFrame and export
    import pandas as pd
    df = pd.DataFrame(data)

    buffer = io.BytesIO()

    if format == "csv":
        df.to_csv(buffer, index=False)
        media_type = "text/csv"
        extension = "csv"
    elif format == "json":
        buffer.write(df.to_json(orient="records", date_format="iso").encode())
        media_type = "application/json"
        extension = "json"
    elif format == "parquet":
        df.to_parquet(buffer, index=False)
        media_type = "application/octet-stream"
        extension = "parquet"
    else:
        raise HTTPException(400, "Invalid format. Use 'csv', 'json', or 'parquet'")

    buffer.seek(0)

    filename = f"{symbol.upper()}_{data_type}_{hours}h.{extension}"

    return StreamingResponse(
        buffer,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/summary")
async def get_summary(
    redis: RedisClient = Depends(get_redis)
) -> Dict[str, Any]:
    """
    Get summary of all symbols and their current state.

    Useful for dashboard overview.
    """
    settings = get_settings()

    summary = {
        "timestamp": int(time.time() * 1000),
        "symbols": {}
    }

    for symbol in settings.SYMBOLS:
        symbol_upper = symbol.upper()
        analytics_key = RedisKeys.analytics_state(symbol_upper)
        data = await redis.hash_get_all(analytics_key)

        if data:
            summary["symbols"][symbol_upper] = {
                "last_price": float(data.get("last_price", 0)) if data.get("last_price") else None,
                "z_score": float(data.get("z_score", 0)) if data.get("z_score") else None,
                "validity": data.get("validity_status", "unknown"),
                "freshness_ms": int(data.get("data_freshness_ms", 0))
            }
        else:
            summary["symbols"][symbol_upper] = {
                "last_price": None,
                "z_score": None,
                "validity": "no_data",
                "freshness_ms": -1
            }

    return summary
