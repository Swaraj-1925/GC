import asyncio
import json
import time
from datetime import datetime, timezone
import logging
from typing import Dict, Optional, List, Set
from dataclasses import dataclass, field

import websockets
from websockets.exceptions import ConnectionClosed

from ..shared.config import get_settings, RedisKeys
from ..shared.models import TickData
from ..shared.db.redis_client import RedisClient

logger = logging.getLogger(__name__)


@dataclass
class GatewayState:
    """Shared state for the gateway service."""
    running: bool = True
    sockets: Dict[str, websockets.WebSocketClientProtocol] = field(default_factory=dict)
    reconnect_delay: float = 1.0
    max_delay: float = 30.0
    last_tick_time: Dict[str, int] = field(default_factory=dict)
    tick_count: Dict[str, int] = field(default_factory=dict)


class MarketGateway:
    """
    Binance WebSocket ingestion service.

    Connects to Binance futures WebSocket streams for configured symbols,
    normalizes tick data, and publishes to Redis for downstream processing.

    Features:
    - Async WebSocket handling with reconnection logic
    - Exponential backoff on connection failures
    - Built-in latency measurement
    - Heartbeat logging
    """

    SERVICE_NAME = "market_gateway"

    def __init__(self, symbols: Optional[List[str]] = None):
        """
        Initialize MarketGateway.

        Args:
            symbols: List of trading pairs (lowercase). Uses config default if None.
        """
        self.settings = get_settings()
        self.symbols = symbols or self.settings.SYMBOLS
        self.state = GatewayState()
        self.redis: Optional[RedisClient] = None

        # Initialize tick counters
        for symbol in self.symbols:
            self.state.tick_count[symbol] = 0
            self.state.last_tick_time[symbol] = 0

    async def start(self) -> None:
        """Start the gateway service."""
        logger.info(f"Starting MarketGateway for symbols: {self.symbols}")

        # Initialize Redis connection
        self.redis = RedisClient(self.SERVICE_NAME, self._log_callback)
        await self.redis.connect()

        # Create tasks for each symbol
        tasks = [
            asyncio.create_task(self._binance_listener(symbol))
            for symbol in self.symbols
        ]

        # Add heartbeat task
        tasks.append(asyncio.create_task(self._heartbeat()))

        # Wait for all tasks (they run until stopped)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("MarketGateway tasks cancelled")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the gateway service."""
        logger.info("Stopping MarketGateway...")
        self.state.running = False

        # Close all WebSocket connections
        for symbol, ws in self.state.sockets.items():
            try:
                await ws.close()
            except Exception as e:
                logger.warning(f"Error closing WebSocket for {symbol}: {e}")

        # Close Redis connection
        if self.redis:
            await self.redis.disconnect()

        logger.info("MarketGateway stopped")

    async def _binance_listener(self, symbol: str) -> None:
        """
        Listen to Binance WebSocket for a single symbol.

        Implements reconnection with exponential backoff.

        Args:
            symbol: Trading pair symbol (lowercase, e.g., 'btcusdt')
        """
        url = f"{self.settings.BINANCE_WS_URL}/{symbol}@trade"
        reconnect_delay = 1.0

        while self.state.running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5
                ) as ws:
                    self.state.sockets[symbol] = ws
                    logger.info(f"Connected to WebSocket: {symbol}")
                    reconnect_delay = 1.0  # Reset on successful connection

                    while self.state.running:
                        try:
                            # Use timeout to allow checking running state
                            message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            await self._process_trade(symbol, message)
                        except asyncio.TimeoutError:
                            continue  # Loop and check running state
                        except ConnectionClosed:
                            logger.warning(f"WebSocket closed for {symbol}")
                            break

            except ConnectionClosed:
                logger.warning(f"WebSocket closed for {symbol}, reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.state.max_delay)

            except Exception as e:
                logger.error(f"WebSocket error for {symbol}: {e}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.state.max_delay)

    async def _process_trade(self, symbol: str, message: str) -> None:
        """
        Process a trade message from Binance.

        Args:
            symbol: Symbol this trade belongs to
            message: Raw JSON message from WebSocket
        """
        try:
            data = json.loads(message)

            # Binance trade event format:
            # {
            #   "e": "trade",
            #   "E": event_time,
            #   "s": symbol,
            #   "t": trade_id,
            #   "p": price,
            #   "q": quantity,
            #   "T": trade_time,
            #   "m": is_buyer_maker
            # }

            if data.get("e") != "trade":
                return

            # Create normalized tick
            tick = TickData(
                symbol=data["s"],
                tradeId=data["t"],
                price=float(data["p"]),
                qty=float(data["q"]),
                timestamp=data["T"],
                isBuyerMaker=data["m"]
            )

            now = int(time.time() * 1000)
            latency_ms = now - tick.timestamp

            # Publish to Redis Stream
            stream_key = RedisKeys.tick_stream(tick.symbol)
            await self.redis.stream_add(stream_key, tick.to_redis_dict())

            # Publish to Redis TimeSeries (for OHLC aggregation)
            ts_key = RedisKeys.price_timeseries(tick.symbol)
            try:
                await self.redis.ts_add(ts_key, tick.timestamp, tick.price)
            except Exception as e:
                # TimeSeries might not be available, log and continue
                logger.debug(f"TimeSeries write failed (module may not be loaded): {e}")

            # Update stats
            self.state.tick_count[symbol] = self.state.tick_count.get(symbol, 0) + 1
            self.state.last_tick_time[symbol] = now

            if latency_ms > 5000:
                logger.warning(f"High latency for {symbol}: {latency_ms}ms")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error processing trade for {symbol}: {e}")

    async def _heartbeat(self) -> None:
        """Periodic heartbeat logging."""
        while self.state.running:
            await asyncio.sleep(30)  # Log every 30 seconds

            for symbol in self.symbols:
                count = self.state.tick_count.get(symbol, 0)
                last_time = self.state.last_tick_time.get(symbol, 0)
                freshness = int(time.time() * 1000) - last_time if last_time else -1

                logger.info(
                    f"Heartbeat [{symbol}]: "
                    f"ticks={count}, "
                    f"freshness={freshness}ms"
                )

                # Publish log to channel
                if self.redis:
                    await self.redis.publish(RedisKeys.CHANNEL_LOGS, {
                        "timestamp": int(time.time() * 1000),
                        "service": self.SERVICE_NAME,
                        "level": "INFO",
                        "operation": "heartbeat",
                        "message": f"Symbol {symbol}: {count} ticks, freshness {freshness}ms"
                    })

    def _log_callback(self, log_entry: dict) -> None:
        """Callback for Redis client logging."""
        # Avoid logging every single operation to prevent log spam
        # Only log significant operations
        if log_entry.get("operation") in ["connect", "disconnect", "stream_create_group"]:
            logger.info(f"Redis {log_entry['operation']}: {log_entry['message']}")


async def run_market_gateway(symbols: Optional[List[str]] = None) -> None:
    """
    Entry point to run MarketGateway as a standalone service.

    Args:
        symbols: Optional list of symbols to subscribe to
    """
    gateway = MarketGateway(symbols)

    try:
        await gateway.start()
    except KeyboardInterrupt:
        pass
    finally:
        await gateway.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    asyncio.run(run_market_gateway())
