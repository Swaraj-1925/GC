import asyncio
import time
import logging
import io
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from ..shared.config import get_settings, RedisKeys
from ..shared.db.redis_client import RedisClient
from ..shared.db.timescale_client import TimescaleClient


logger = logging.getLogger(__name__)


class Archivist:
    """
    Hot-to-cold storage archival service.

    Periodically reads data from Redis TimeSeries and Streams,
    then batch inserts into TimescaleDB for long-term storage.

    Features:
    - Configurable archive intervals
    - Batch inserts for efficiency
    - Duplicate prevention via timestamp tracking
    - Export to CSV, JSON, or Parquet on demand
    """

    SERVICE_NAME = "archivist"

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        archive_interval: int = 60,  # seconds
        batch_size: int = 1000
    ):
        """
        Initialize Archivist.

        Args:
            symbols: List of symbols to archive
            archive_interval: Seconds between archive runs
            batch_size: Max records per batch insert
        """
        self.settings = get_settings()
        self.symbols = [s.upper() for s in (symbols or self.settings.SYMBOLS)]
        self.archive_interval = archive_interval
        self.batch_size = batch_size

        # Track last archived timestamps per symbol
        self.last_archived_ts: Dict[str, int] = {}

        self.redis: Optional[RedisClient] = None
        self.timescale: Optional[TimescaleClient] = None
        self.running = True

    async def start(self) -> None:
        """Start the archivist service."""
        logger.info(f"Starting Archivist for symbols: {self.symbols}")
        logger.info(f"Archive interval: {self.archive_interval}s, batch size: {self.batch_size}")

        # Initialize database connections
        self.redis = RedisClient(self.SERVICE_NAME, self._log_callback)
        await self.redis.connect()

        self.timescale = TimescaleClient(self.SERVICE_NAME, self._log_callback)
        await self.timescale.connect()

        # Run main loop
        try:
            await self._main_loop()
        except asyncio.CancelledError:
            logger.info("Archivist cancelled")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the archivist service."""
        logger.info("Stopping Archivist...")
        self.running = False

        if self.redis:
            await self.redis.disconnect()
        if self.timescale:
            await self.timescale.disconnect()

        logger.info("Archivist stopped")

    async def _main_loop(self) -> None:
        """Main loop - periodic archive runs."""
        while self.running:
            try:
                await self._archive_all_symbols()

                # Wait for next interval
                await asyncio.sleep(self.archive_interval)

            except Exception as e:
                logger.error(f"Error in archive loop: {e}")
                await asyncio.sleep(10)  # Wait before retry

    async def _archive_all_symbols(self) -> None:
        """Archive data for all configured symbols."""
        now = int(time.time() * 1000)

        for symbol in self.symbols:
            try:
                await self._archive_ticks(symbol, now)
                await self._archive_analytics(symbol, now)
            except Exception as e:
                logger.error(f"Error archiving {symbol}: {e}")

    async def _archive_ticks(self, symbol: str, now: int) -> None:
        """
        Archive ticks from Redis Stream to TimescaleDB.

        Args:
            symbol: Symbol to archive
            now: Current timestamp in ms
        """
        stream_key = RedisKeys.tick_stream(symbol)

        # Get last archived position or start from latest ($ = only new messages)
        # This prevents re-archiving data from previous runs
        last_id = self.last_archived_ts.get(f"tick:{symbol}", "$")

        # Read from stream
        streams = {stream_key: last_id}
        results = await self.redis.stream_read(streams, count=self.batch_size, block=0)

        if not results:
            return

        ticks = []
        last_entry_id = last_id

        for stream_name, entries in results:
            for entry_id, data in entries:
                try:
                    ticks.append({
                        "timestamp": int(data.get("timestamp", 0)),
                        "symbol": data.get("symbol", symbol),
                        "trade_id": int(data.get("trade_id", 0)),
                        "price": float(data.get("price", 0)),
                        "qty": float(data.get("qty", 0)),
                        "is_buyer_maker": data.get("is_buyer_maker") == "1"
                    })
                    last_entry_id = entry_id
                except (ValueError, KeyError) as e:
                    logger.warning(f"Invalid tick data: {e}")

        if ticks:
            # Batch insert to TimescaleDB
            inserted = await self.timescale.insert_ticks_batch(ticks)
            logger.info(f"Archived {inserted} ticks for {symbol}")

            # Update last archived position
            self.last_archived_ts[f"tick:{symbol}"] = last_entry_id

    async def _archive_analytics(self, symbol: str, now: int) -> None:
        """
        Archive current analytics snapshot to TimescaleDB.

        Args:
            symbol: Symbol to archive
            now: Current timestamp in ms
        """
        analytics_key = RedisKeys.analytics_state(symbol)

        # Get current analytics state
        data = await self.redis.hash_get_all(analytics_key)

        if not data:
            return

        try:
            snapshot = {
                "timestamp": int(data.get("timestamp", now)),
                "symbol": data.get("symbol", symbol),
                "pair_symbol": data.get("pair_symbol"),
                "last_price": float(data["last_price"]) if data.get("last_price") else None,
                "spread": float(data["spread"]) if data.get("spread") else None,
                "hedge_ratio": float(data["hedge_ratio"]) if data.get("hedge_ratio") else None,
                "z_score": float(data["z_score"]) if data.get("z_score") else None,
                "correlation": float(data["correlation"]) if data.get("correlation") else None,
                "adf_statistic": float(data["adf_statistic"]) if data.get("adf_statistic") else None,
                "adf_pvalue": float(data["adf_pvalue"]) if data.get("adf_pvalue") else None,
                "is_stationary": data.get("is_stationary") == "1" if data.get("is_stationary") else None,
                "tick_count": int(data["tick_count"]) if data.get("tick_count") else None
            }

            await self.timescale.insert_analytics_snapshot(snapshot)
            logger.debug(f"Archived analytics snapshot for {symbol}")

        except (ValueError, KeyError) as e:
            logger.warning(f"Invalid analytics data for {symbol}: {e}")

    # ==================== Export Functions ====================

    async def export_ticks_csv(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        filepath: str
    ) -> int:
        """
        Export ticks to CSV file.

        Args:
            symbol: Symbol to export
            start_time: Start of range
            end_time: End of range
            filepath: Output file path

        Returns:
            Number of rows exported
        """
        ticks = await self.timescale.get_ticks(symbol, start_time, end_time)

        if not ticks:
            return 0

        df = pd.DataFrame(ticks)
        df.to_csv(filepath, index=False)
        logger.info(f"Exported {len(ticks)} ticks to {filepath}")
        return len(ticks)

    async def export_ticks_json(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        filepath: str
    ) -> int:
        """Export ticks to JSON file."""
        ticks = await self.timescale.get_ticks(symbol, start_time, end_time)

        if not ticks:
            return 0

        df = pd.DataFrame(ticks)
        df.to_json(filepath, orient="records", date_format="iso")
        logger.info(f"Exported {len(ticks)} ticks to {filepath}")
        return len(ticks)

    async def export_ticks_parquet(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        filepath: str
    ) -> int:
        """Export ticks to Parquet file."""
        ticks = await self.timescale.get_ticks(symbol, start_time, end_time)

        if not ticks:
            return 0

        df = pd.DataFrame(ticks)
        df.to_parquet(filepath, index=False, compression="snappy")
        logger.info(f"Exported {len(ticks)} ticks to {filepath}")
        return len(ticks)

    async def export_ohlc_csv(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        filepath: str
    ) -> int:
        """Export OHLC bars to CSV file."""
        bars = await self.timescale.get_ohlc(symbol, interval, start_time, end_time)

        if not bars:
            return 0

        df = pd.DataFrame(bars)
        df.to_csv(filepath, index=False)
        logger.info(f"Exported {len(bars)} OHLC bars to {filepath}")
        return len(bars)

    async def get_export_bytes(
        self,
        symbol: str,
        data_type: str,  # "ticks" or "ohlc"
        format: str,  # "csv", "json", or "parquet"
        start_time: datetime,
        end_time: datetime,
        interval: str = "1m"
    ) -> Optional[bytes]:
        """
        Get export data as bytes (for API streaming).

        Args:
            symbol: Symbol to export
            data_type: Type of data ("ticks" or "ohlc")
            format: Export format ("csv", "json", "parquet")
            start_time: Start of range
            end_time: End of range
            interval: OHLC interval (for ohlc data type)

        Returns:
            Bytes of exported data or None
        """
        # Fetch data
        if data_type == "ticks":
            data = await self.timescale.get_ticks(symbol, start_time, end_time)
        elif data_type == "ohlc":
            data = await self.timescale.get_ohlc(symbol, interval, start_time, end_time)
        else:
            return None

        if not data:
            return None

        df = pd.DataFrame(data)

        # Convert to bytes
        buffer = io.BytesIO()

        if format == "csv":
            df.to_csv(buffer, index=False)
        elif format == "json":
            buffer.write(df.to_json(orient="records", date_format="iso").encode())
        elif format == "parquet":
            df.to_parquet(buffer, index=False, compression="snappy")
        else:
            return None

        buffer.seek(0)
        return buffer.read()

    def _log_callback(self, log_entry: dict) -> None:
        """Callback for database client logging."""
        if log_entry.get("operation") in ["connect", "disconnect", "insert_batch"]:
            logger.info(f"{log_entry['operation']}: {log_entry['message']}")


async def run_archivist(
    symbols: Optional[List[str]] = None,
    archive_interval: int = 60
) -> None:
    """
    Entry point to run Archivist as a standalone service.

    Args:
        symbols: Optional list of symbols
        archive_interval: Seconds between archive runs
    """
    archivist = Archivist(symbols, archive_interval)

    try:
        await archivist.start()
    except KeyboardInterrupt:
        pass
    finally:
        await archivist.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    asyncio.run(run_archivist())
