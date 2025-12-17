import asyncio
import json
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ...shared.config import get_settings, RedisKeys
from ...shared.db.redis_client import RedisClient
from .websocket_connection_manager import manager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ticks/{symbol}")
async def websocket_ticks(websocket: WebSocket, symbol: str):
    """
    WebSocket endpoint for live tick updates.

    Streams real-time trade data for a symbol.
    """
    await manager.connect_ticks(websocket, symbol.upper())

    # Create Redis client for this connection
    redis = RedisClient(f"ws_ticks_{symbol}")
    try:
        await redis.connect()
    except Exception as e:
        logger.error(f"Failed to connect Redis for ticks WebSocket: {e}")
        await websocket.close(code=1011, reason="Backend service unavailable")
        return

    stream_key = RedisKeys.tick_stream(symbol.upper())
    last_id = "$"  # Start from newest messages

    try:
        while True:
            try:
                # Non-blocking check for incoming messages (like ping/pong)
                await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.01
                )
            except asyncio.TimeoutError:
                pass
            except (WebSocketDisconnect, RuntimeError):
                break

            # Read new ticks from Redis stream
            try:
                results = await redis.stream_read(
                    {stream_key: last_id},
                    count=500,
                    block=500  # 500ms block
                )

                if results:
                    for stream_name, entries in results:
                        for entry_id, data in entries:
                            # Send tick to client
                            tick_data = {
                                "symbol": data.get("symbol", symbol.upper()),
                                "trade_id": data.get("trade_id"),
                                "price": float(data.get("price", 0)),
                                "qty": float(data.get("qty", 0)),
                                "timestamp": int(data.get("timestamp", 0)),
                                "is_buyer_maker": data.get("is_buyer_maker") == "1"
                            }
                            await websocket.send_json(tick_data)
                            last_id = entry_id
            except Exception as e:
                logger.error(f"Error reading stream: {e}")
                await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"Critical error in Ticks WS: {e}")
    finally:
        manager.disconnect_ticks(websocket, symbol.upper())
        await redis.disconnect()


@router.websocket("/analytics/{symbol}")
async def websocket_analytics(websocket: WebSocket, symbol: str):
    """
    WebSocket endpoint for live analytics updates.

    Streams analytics snapshots as they're computed.
    """
    await manager.connect_analytics(websocket, symbol.upper())

    # Create Redis client for this connection
    redis = RedisClient(f"ws_analytics_{symbol}")
    try:
        await redis.connect()
    except Exception as e:
        logger.error(f"Failed to connect Redis for analytics WebSocket: {e}")
        await websocket.close(code=1011, reason="Backend service unavailable")
        return

    analytics_key = RedisKeys.analytics_state(symbol.upper())
    last_timestamp = 0

    try:
        while True:
            # Check for client disconnect
            try:
                await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.01
                )
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            except RuntimeError as e:
                # Catch Starlette "WebSocket is not connected" error
                if "not connected" in str(e):
                    break
                raise e

            # Poll for analytics updates
            try:
                data = await redis.hash_get_all(analytics_key)

                if data:
                    current_ts = int(data.get("timestamp", 0))

                    # Only send if updated
                    if current_ts > last_timestamp:
                        analytics = {
                            "symbol": data.get("symbol", symbol.upper()),
                            "pair_symbol": data.get("pair_symbol"),
                            "timestamp": current_ts,
                            "last_price": float(data["last_price"]) if data.get("last_price") else None,
                            "z_score": float(data["z_score"]) if data.get("z_score") else None,
                            "spread": float(data["spread"]) if data.get("spread") else None,
                            "hedge_ratio": float(data["hedge_ratio"]) if data.get("hedge_ratio") else None,
                            "correlation": float(data["correlation"]) if data.get("correlation") else None,
                            "validity_status": data.get("validity_status"),
                            "data_freshness_ms": int(data.get("data_freshness_ms", 0)),
                            "tick_count": int(data.get("tick_count", 0))
                        }
                        await websocket.send_json(analytics)
                        last_timestamp = current_ts

                # Poll every 500ms
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Error reading analytics: {e}")
                await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect_analytics(websocket, symbol.upper())
        await redis.disconnect()


@router.websocket("/alerts")
async def websocket_alerts(websocket: WebSocket):
    """
    WebSocket endpoint for alert notifications.

    Streams alerts as they're triggered.
    """
    await manager.connect_alerts(websocket)

    # Create Redis client for Pub/Sub
    redis = RedisClient("ws_alerts")
    try:
        await redis.connect()
        await redis.subscribe(RedisKeys.CHANNEL_ALERTS)
    except Exception as e:
        logger.error(f"Failed to connect Redis for alerts WebSocket: {e}")
        await websocket.close(code=1011, reason="Backend service unavailable")
        return

    try:
        while True:
            # Check for client disconnect
            try:
                await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.01
                )
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            except RuntimeError as e:
                # Catch Starlette "WebSocket is not connected" error
                if "not connected" in str(e):
                    break
                raise e

            # Check for new alerts
            try:
                message = await redis.get_message(timeout=0.5)

                if message and message.get("type") == "message":
                    data = message["data"]
                    if isinstance(data, str):
                        alert = json.loads(data)
                    else:
                        alert = data

                    await websocket.send_json(alert)

            except Exception as e:
                logger.error(f"Error reading alerts: {e}")
                await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect_alerts(websocket)
        await redis.disconnect()


@router.websocket("/pair/{symbol_a}/{symbol_b}")
async def websocket_pair_analytics(
    websocket: WebSocket,
    symbol_a: str,
    symbol_b: str
):
    """
    WebSocket endpoint for pair analytics.

    Streams spread, z-score, and correlation for a trading pair.
    """
    pair_key = f"{symbol_a.upper()}:{symbol_b.upper()}"
    await manager.connect_analytics(websocket, pair_key)

    redis = RedisClient(f"ws_pair_{pair_key}")
    try:
        await redis.connect()
    except Exception as e:
        logger.error(f"Failed to connect Redis for pair WebSocket: {e}")
        await websocket.close(code=1011, reason="Redis connection failed")
        return

    analytics_key = RedisKeys.analytics_state(pair_key)
    last_timestamp = 0

    try:
        while True:
            try:
                await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.01
                )
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            except RuntimeError as e:
                # Catch Starlette "WebSocket is not connected" error
                if "not connected" in str(e):
                    break
                raise e

            try:
                data = await redis.hash_get_all(analytics_key)

                if data:
                    current_ts = int(data.get("timestamp", 0))

                    if current_ts > last_timestamp:
                        pair_analytics = {
                            "symbol": symbol_a.upper(),
                            "pair_symbol": symbol_b.upper(),
                            "timestamp": current_ts,
                            "spread": float(data["spread"]) if data.get("spread") else None,
                            "hedge_ratio": float(data["hedge_ratio"]) if data.get("hedge_ratio") else None,
                            "z_score": float(data["z_score"]) if data.get("z_score") else None,
                            "correlation": float(data["correlation"]) if data.get("correlation") else None,
                            "adf_statistic": float(data["adf_statistic"]) if data.get("adf_statistic") else None,
                            "adf_pvalue": float(data["adf_pvalue"]) if data.get("adf_pvalue") else None,
                            "is_stationary": data.get("is_stationary") == "1" if data.get("is_stationary") else None,
                            "validity_status": data.get("validity_status"),
                            "tick_count": int(data.get("tick_count", 0))
                        }
                        await websocket.send_json(pair_analytics)
                        last_timestamp = current_ts

                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Error reading pair analytics: {e}")
                await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect_analytics(websocket, pair_key)
        await redis.disconnect()


@router.websocket("/ohlc/{symbol}")
async def websocket_ohlc(
    websocket: WebSocket,
    symbol: str,
    interval: str = "1m"
):
    """
    WebSocket endpoint for live OHLC candle updates.

    Streams candle updates at the specified interval.
    Candles are updated in real-time as trades come in,
    and a final candle is sent when each period completes.

    Args:
        symbol: Trading pair symbol
        interval: Candle interval (1s, 1m, 5m)
    """
    # Validate interval
    valid_intervals = {"1s": 1, "1m": 60, "5m": 300}
    if interval not in valid_intervals:
        await websocket.close(code=1008, reason=f"Invalid interval. Use: {list(valid_intervals.keys())}")
        return

    interval_seconds = valid_intervals[interval]

    await websocket.accept()
    logger.info(f"Client connected to ohlc/{symbol.upper()}?interval={interval}")

    # Create Redis client for this connection
    redis = RedisClient(f"ws_ohlc_{symbol}_{interval}")
    try:
        await redis.connect()
    except Exception as e:
        logger.error(f"Failed to connect Redis for OHLC WebSocket: {e}")
        await websocket.close(code=1011, reason="Backend service unavailable")
        return

    stream_key = RedisKeys.tick_stream(symbol.upper())
    last_id = "$"  # Start from newest messages

    # Current candle state
    current_candle = {
        "time": 0,
        "open": 0.0,
        "high": 0.0,
        "low": float('inf'),
        "close": 0.0,
        "volume": 0.0,
        "trade_count": 0
    }
    last_candle_time = 0

    try:
        while True:
            # Check for client disconnect
            try:
                await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=0.01
                )
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            except RuntimeError as e:
                # Catch Starlette "WebSocket is not connected" error
                if "not connected" in str(e):
                    logger.warning(f"WebSocket disconnected (RuntimeError): {e}")
                    break
                raise e

            # Read new ticks from Redis stream
            try:
                results = await redis.stream_read(
                    {stream_key: last_id},
                    count=1000,
                    block=200  # 200ms block
                )

                if results:
                    for stream_name, entries in results:
                        for entry_id, data in entries:
                            last_id = entry_id

                            # Parse tick data
                            tick_ts = int(data.get("timestamp", 0))
                            price = float(data.get("price", 0))
                            qty = float(data.get("qty", 0))

                            # Calculate candle time bucket
                            tick_time_seconds = tick_ts // 1000
                            candle_time = (tick_time_seconds // interval_seconds) * interval_seconds

                            # Check if we need to start a new candle
                            if candle_time > last_candle_time:
                                # initialize if first run
                                if last_candle_time == 0:
                                    last_candle_time = candle_time
                                    current_candle = {
                                        "time": candle_time,
                                        "open": price,
                                        "high": price,
                                        "low": price,
                                        "close": price,
                                        "volume": qty,
                                        "trade_count": 1
                                    }
                                else:
                                    # Send completed candle
                                    if (current_candle["trade_count"] > 0 and 
                                        current_candle["low"] > 0 and 
                                        current_candle["low"] != float('inf')):
                                        await websocket.send_json({
                                            **current_candle,
                                            "complete": True
                                        })
                                    
                                    # Fill gaps if we skipped intervals
                                    next_expected_time = last_candle_time + interval_seconds
                                    while next_expected_time < candle_time:
                                        # Emit flat candle for missing interval
                                        flat_price = current_candle["close"]
                                        await websocket.send_json({
                                            "time": next_expected_time,
                                            "open": flat_price,
                                            "high": flat_price,
                                            "low": flat_price,
                                            "close": flat_price,
                                            "volume": 0,
                                            "trade_count": 0,
                                            "complete": True
                                        })
                                        next_expected_time += interval_seconds

                                    # Start new candle
                                    last_candle_time = candle_time
                                    current_candle = {
                                        "time": candle_time,
                                        "open": price,
                                        "high": price,
                                        "low": price,
                                        "close": price,
                                        "volume": qty,
                                        "trade_count": 1
                                    }
                            else:
                                # Update current candle
                                current_candle["high"] = max(current_candle["high"], price)
                                current_candle["low"] = min(current_candle["low"], price)
                                current_candle["close"] = price
                                current_candle["volume"] += qty
                                current_candle["trade_count"] += 1

                    # Send partial candle update (for live updates)
                    # Only send if candle has valid data (low > 0 and not infinity)
                    if (current_candle["trade_count"] > 0 and 
                        current_candle["low"] > 0 and 
                        current_candle["low"] != float('inf')):
                        await websocket.send_json({
                            **current_candle,
                            "complete": False
                        })

            except Exception as e:
                logger.error(f"Error in OHLC stream: {e}")
                await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
    finally:
        logger.info(f"Client disconnected from ohlc/{symbol.upper()}?interval={interval}")
        await redis.disconnect()



