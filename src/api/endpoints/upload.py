"""
Upload API Endpoint - NDJSON tick data upload with progressive chart streaming.

Handles file upload, data parsing, and SSE streaming of computed analytics.
Uploaded data is isolated from live data using UPLOAD: symbol prefix.
"""

import asyncio
import json
import time
import uuid
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import math

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import numpy as np

from ...shared.config import get_settings, RedisKeys
from ...shared.db.redis_client import RedisClient
from ...shared.db.timescale_client import TimescaleClient

logger = logging.getLogger(__name__)
router = APIRouter()

# ==================== Constants ====================
UPLOAD_SYMBOL_PREFIX = "UPLOAD:"  # Prefix to isolate from live data
MAX_FILE_SIZE_MB = 50
SESSION_TTL_HOURS = 24


# ==================== DI ====================
async def get_redis(request: Request) -> RedisClient:
    return request.app.state.redis


async def get_timescale(request: Request) -> TimescaleClient:
    return request.app.state.timescale


# ==================== Models ====================
class UploadResponse(BaseModel):
    session_id: str
    symbol: str
    tick_count: int
    message: str


class UploadedSymbolInfo(BaseModel):
    symbol: str
    display_name: str
    tick_count: int
    first_ts: Optional[int]
    last_ts: Optional[int]
    uploaded_at: int
    is_uploaded: bool = True


# ==================== Storage for uploaded sessions ====================
# In-memory storage for simplicity (could move to Redis for persistence)
_upload_sessions: Dict[str, Dict[str, Any]] = {}


# ==================== Endpoints ====================

@router.post("/upload", response_model=UploadResponse)
async def upload_ndjson(
    file: UploadFile = File(...),
    symbol_name: str = Form(..., description="Unique symbol name for uploaded data"),
    redis: RedisClient = Depends(get_redis)
):
    """
    Upload NDJSON tick data file.
    
    Expected format per line:
    {"symbol": "btcusdt", "ts": "2025-12-16T07:09:59.999000Z", "price": 86537.16, "size": 6.01269}
    
    The symbol_name you provide will be prefixed with UPLOAD: to prevent
    conflicts with live data.
    """
    # Validate symbol name
    if not symbol_name or len(symbol_name) < 2:
        raise HTTPException(400, "Symbol name must be at least 2 characters")
    
    # Check for conflicts with existing uploads
    full_symbol = f"{UPLOAD_SYMBOL_PREFIX}{symbol_name.upper()}"
    if full_symbol in _upload_sessions:
        raise HTTPException(400, f"Symbol '{symbol_name}' already exists. Choose a different name.")
    
    # Read and parse file
    content = await file.read()
    if len(content) > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(400, f"File too large. Max {MAX_FILE_SIZE_MB}MB allowed.")
    
    try:
        lines = content.decode('utf-8').strip().split('\n')
    except UnicodeDecodeError:
        raise HTTPException(400, "File must be UTF-8 encoded")
    
    # Parse NDJSON lines
    ticks = []
    errors = []
    
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            
            # Parse timestamp
            ts_str = data.get('ts') or data.get('timestamp')
            
            if ts_str is None:
                errors.append(f"Line {i+1}: Missing timestamp")
                continue

            if isinstance(ts_str, str):
                # Parse ISO format
                try:
                    dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    ts_ms = int(dt.timestamp() * 1000)
                except ValueError:
                    errors.append(f"Line {i+1}: Invalid timestamp format")
                    continue
            else:
                ts_ms = int(ts_str)
            
            # Get price and size
            price = float(data.get('price', 0))
            size = float(data.get('size') or data.get('qty', 0))
            
            if price <= 0:
                errors.append(f"Line {i+1}: Invalid price")
                continue
            
            ticks.append({
                'timestamp': ts_ms,
                'price': price,
                'qty': size,
                'symbol': full_symbol,
                'trade_id': i  # Use line number as trade ID
            })
            
        except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
            errors.append(f"Line {i+1}: {str(e)}")
            if len(errors) > 10:
                raise HTTPException(400, f"Too many parsing errors: {errors[:10]}")
    
    if not ticks:
        raise HTTPException(400, f"No valid ticks found. Errors: {errors[:5]}")
    
    # Sort by timestamp
    ticks.sort(key=lambda x: x['timestamp'])
    
    # Store in session
    session_id = uuid.uuid4().hex[:12]
    _upload_sessions[full_symbol] = {
        'session_id': session_id,
        'symbol': full_symbol,
        'display_name': symbol_name.upper(),
        'ticks': ticks,
        'tick_count': len(ticks),
        'first_ts': ticks[0]['timestamp'],
        'last_ts': ticks[-1]['timestamp'],
        'uploaded_at': int(time.time() * 1000),
        'ohlc_computed': False,
        'analytics_computed': False
    }
    
    logger.info(f"[Upload] Stored {len(ticks)} ticks for {full_symbol}")
    
    return UploadResponse(
        session_id=session_id,
        symbol=full_symbol,
        tick_count=len(ticks),
        message=f"Successfully uploaded {len(ticks)} ticks. Errors: {len(errors)}"
    )


@router.get("/upload/symbols", response_model=List[UploadedSymbolInfo])
async def get_uploaded_symbols():
    """Get list of all uploaded symbols for watchlist."""
    symbols = []
    for symbol, session in _upload_sessions.items():
        symbols.append(UploadedSymbolInfo(
            symbol=session['symbol'],
            display_name=session['display_name'],
            tick_count=session['tick_count'],
            first_ts=session.get('first_ts'),
            last_ts=session.get('last_ts'),
            uploaded_at=session['uploaded_at'],
            is_uploaded=True
        ))
    return symbols


@router.delete("/upload/{symbol}")
async def delete_uploaded_symbol(symbol: str):
    """Delete an uploaded symbol and its data."""
    full_symbol = symbol if symbol.startswith(UPLOAD_SYMBOL_PREFIX) else f"{UPLOAD_SYMBOL_PREFIX}{symbol}"
    
    if full_symbol not in _upload_sessions:
        raise HTTPException(404, f"Symbol {symbol} not found")
    
    del _upload_sessions[full_symbol]
    logger.info(f"[Upload] Deleted uploaded symbol: {full_symbol}")
    
    return {"message": f"Deleted {symbol}"}


@router.get("/upload/{symbol}/stream")
async def stream_analytics(symbol: str):
    """
    SSE endpoint to stream computed analytics progressively.
    
    Streams:
    1. OHLC candles (in batches)
    2. Price statistics
    3. Rolling metrics (if enough data)
    """
    full_symbol = symbol if symbol.startswith(UPLOAD_SYMBOL_PREFIX) else f"{UPLOAD_SYMBOL_PREFIX}{symbol}"
    
    if full_symbol not in _upload_sessions:
        raise HTTPException(404, f"Symbol {symbol} not found")
    
    session = _upload_sessions[full_symbol]
    
    def safe_json_dumps(obj: Any) -> str:
        """JSON serializer that handles NaN/Infinity by converting to null."""
        def clean_floats(o):
            if isinstance(o, float):
                if math.isnan(o) or math.isinf(o):
                    return None
                return o
            if isinstance(o, dict):
                return {k: clean_floats(v) for k, v in o.items()}
            if isinstance(o, list):
                return [clean_floats(v) for v in o]
            return o
        
        return json.dumps(clean_floats(obj))

    async def event_generator():
        """Generate SSE events for progressive chart loading."""
        try:
            ticks = session['ticks']
            
            # ===== 1. Compute and stream OHLC candles =====
            yield f"data: {safe_json_dumps({'type': 'status', 'message': 'Computing OHLC candles...'})}\n\n"
            await asyncio.sleep(0.05)  # Small delay to let frontend render
            
            ohlc = compute_ohlc_from_ticks(ticks, interval_seconds=60)
            
            # Stream OHLC in batches for progressive rendering
            batch_size = 50
            for i in range(0, len(ohlc), batch_size):
                batch = ohlc[i:i+batch_size]
                yield f"data: {safe_json_dumps({'type': 'ohlc', 'data': batch, 'complete': i + batch_size >= len(ohlc)})}\n\n"
                await asyncio.sleep(0.02)
            
            # ===== 2. Compute and stream price statistics =====
            yield f"data: {safe_json_dumps({'type': 'status', 'message': 'Computing statistics...'})}\n\n"
            await asyncio.sleep(0.05)
            
            prices = np.array([t['price'] for t in ticks])
            volumes = np.array([t['qty'] for t in ticks])
            
            stats = {
                'tick_count': len(ticks),
                'price_min': float(np.min(prices)),
                'price_max': float(np.max(prices)),
                'price_mean': float(np.mean(prices)),
                'price_std': float(np.std(prices)),
                'price_first': float(prices[0]),
                'price_last': float(prices[-1]),
                'price_change_pct': float((prices[-1] - prices[0]) / prices[0] * 100) if prices[0] != 0 else 0,
                'volume_total': float(np.sum(volumes)),
                'volume_mean': float(np.mean(volumes)),
                'vwap': float(np.sum(prices * volumes) / np.sum(volumes)) if np.sum(volumes) > 0 else float(np.mean(prices)),
                'time_start': ticks[0]['timestamp'],
                'time_end': ticks[-1]['timestamp'],
                'duration_minutes': (ticks[-1]['timestamp'] - ticks[0]['timestamp']) / 60000
            }
            
            yield f"data: {safe_json_dumps({'type': 'stats', 'data': stats})}\n\n"
            
            # ===== 3. Compute spread (price deviation from mean) =====
            yield f"data: {safe_json_dumps({'type': 'status', 'message': 'Computing spread...'})}\n\n"
            await asyncio.sleep(0.05)
            
            # Compute rolling mean and spread
            window = min(20, len(prices) // 2) if len(prices) > 20 else max(2, len(prices) // 2)
            spread_data = []
            
            for i in range(window, len(ticks)):
                window_prices = prices[i-window:i]
                mean = np.mean(window_prices)
                spread = prices[i] - mean
                spread_data.append({
                    'time': ticks[i]['timestamp'] // 1000,  # Convert to seconds
                    'value': float(spread),
                    'mean': float(mean)
                })
            
            # Stream in batches
            for i in range(0, len(spread_data), batch_size):
                batch = spread_data[i:i+batch_size]
                yield f"data: {safe_json_dumps({'type': 'spread', 'data': batch, 'complete': i + batch_size >= len(spread_data)})}\n\n"
                await asyncio.sleep(0.02)
            
            # ===== 4. Compute Z-score =====
            yield f"data: {safe_json_dumps({'type': 'status', 'message': 'Computing Z-score...'})}\n\n"
            await asyncio.sleep(0.05)
            
            zscore_data = []
            for i in range(window, len(ticks)):
                window_prices = prices[i-window:i]
                mean = np.mean(window_prices)
                std = np.std(window_prices)
                z = (prices[i] - mean) / std if std > 0 else 0
                zscore_data.append({
                    'time': ticks[i]['timestamp'] // 1000,
                    'value': float(z)
                })
            
            for i in range(0, len(zscore_data), batch_size):
                batch = zscore_data[i:i+batch_size]
                yield f"data: {safe_json_dumps({'type': 'zscore', 'data': batch, 'complete': i + batch_size >= len(zscore_data)})}\n\n"
                await asyncio.sleep(0.02)
            
            # ===== 5. Compute rolling volatility =====
            yield f"data: {safe_json_dumps({'type': 'status', 'message': 'Computing volatility...'})}\n\n"
            await asyncio.sleep(0.05)
            
            volatility_data = []
            for i in range(window, len(ticks)):
                window_prices = prices[i-window:i]
                mean_val = np.mean(window_prices)
                
                # Check for div by zero
                if abs(mean_val) < 1e-9:
                    vol = 0.0
                else:
                    vol = np.std(window_prices) / mean_val * 100  # As percentage
                    
                volatility_data.append({
                    'time': ticks[i]['timestamp'] // 1000,
                    'value': float(vol)
                })
            
            for i in range(0, len(volatility_data), batch_size):
                batch = volatility_data[i:i+batch_size]
                yield f"data: {safe_json_dumps({'type': 'volatility', 'data': batch, 'complete': i + batch_size >= len(volatility_data)})}\n\n"
                await asyncio.sleep(0.02)
            
            # ===== Done =====
            yield f"data: {safe_json_dumps({'type': 'complete', 'message': 'All analytics computed'})}\n\n"
            
            # Mark session as computed
            session['ohlc_computed'] = True
            session['analytics_computed'] = True
            
        except Exception as e:
            logger.error(f"[Upload] Stream error for {symbol}: {e}", exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/upload/{symbol}/ohlc")
async def get_upload_ohlc(
    symbol: str,
    interval: str = "1m"
):
    """Get OHLC data for uploaded symbol (for chart refresh)."""
    full_symbol = symbol if symbol.startswith(UPLOAD_SYMBOL_PREFIX) else f"{UPLOAD_SYMBOL_PREFIX}{symbol}"
    
    if full_symbol not in _upload_sessions:
        raise HTTPException(404, f"Symbol {symbol} not found")
    
    session = _upload_sessions[full_symbol]
    ticks = session['ticks']
    
    interval_map = {"1s": 1, "1m": 60, "5m": 300}
    interval_secs = interval_map.get(interval, 60)
    
    ohlc = compute_ohlc_from_ticks(ticks, interval_secs)
    return ohlc


# ==================== Helper Functions ====================

def compute_ohlc_from_ticks(ticks: List[Dict], interval_seconds: int = 60) -> List[Dict]:
    """Compute OHLC candles from tick data."""
    if not ticks:
        return []
    
    buckets = {}
    
    for tick in ticks:
        ts_seconds = tick['timestamp'] // 1000
        bucket_time = (ts_seconds // interval_seconds) * interval_seconds
        
        price = tick['price']
        qty = tick['qty']
        
        if bucket_time not in buckets:
            buckets[bucket_time] = {
                'time': bucket_time,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': qty,
                'first_ts': tick['timestamp'],
                'last_ts': tick['timestamp'],
                'trade_count': 1
            }
        else:
            b = buckets[bucket_time]
            b['high'] = max(b['high'], price)
            b['low'] = min(b['low'], price)
            b['volume'] += qty
            b['trade_count'] += 1
            
            if tick['timestamp'] < b['first_ts']:
                b['open'] = price
                b['first_ts'] = tick['timestamp']
            if tick['timestamp'] >= b['last_ts']:
                b['close'] = price
                b['last_ts'] = tick['timestamp']
    
    # Sort and clean up
    candles = []
    for bucket_time in sorted(buckets.keys()):
        b = buckets[bucket_time]
        candles.append({
            'time': b['time'],
            'open': b['open'],
            'high': b['high'],
            'low': b['low'],
            'close': b['close'],
            'volume': b['volume']
        })
    
    return candles
