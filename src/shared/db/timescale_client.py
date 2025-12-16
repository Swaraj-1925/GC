import asyncpg
import time
from typing import Optional, Dict, List, Any, Callable
from datetime import datetime

from ..config import get_settings


class TimescaleClient:
    """
    Async TimescaleDB client for archival and cold storage.

    Handles:
    - Connection pooling
    - Schema creation (hypertables)
    - Batch inserts for archival
    - Data export queries
    """

    def __init__(self, service_name: str, log_callback: Optional[Callable] = None):
        """
        Initialize TimescaleDB client.

        Args:
            service_name: Name of the service using this client
            log_callback: Optional callback for logging operations
        """
        self.service_name = service_name
        self.log_callback = log_callback
        self._pool: Optional[asyncpg.Pool] = None
        self.settings = get_settings()

    async def connect(self) -> None:
        """
        Establish connection pool to TimescaleDB.

        Supports both local TimescaleDB and Timescale Cloud.
        Uses TIMESCALE_URL if provided, otherwise builds DSN from components.
        """

        # Build SSL context for cloud connections

        self._pool = await asyncpg.create_pool(
            self.settings.TIMESCALE_URL,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )

        # Log connection (mask password in DSN for security)
        safe_dsn = self.settings.TIMESCALE_URL.split("@")[-1] if "@" in self.settings.TIMESCALE_URL else "timescale db"
        self._log("connect", None, f"Connected to TimescaleDB: {safe_dsn}")

        # Initialize schema
        await self._init_schema()
    async def disconnect(self) -> None:
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
        self._log("disconnect", None, "Disconnected from TimescaleDB")

    def _log(self, operation: str, table: Optional[str], message: str, duration_ms: float = 0) -> None:
        """Log operation if callback is set."""
        if self.log_callback:
            self.log_callback({
                "timestamp": int(time.time() * 1000),
                "service": self.service_name,
                "operation": operation,
                "key": table,
                "message": message,
                "duration_ms": duration_ms
            })

    async def _init_schema(self) -> None:
        """Initialize database schema with hypertables."""
        async with self._pool.acquire() as conn:
            # Create ticks table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    time TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    trade_id BIGINT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    qty DOUBLE PRECISION NOT NULL,
                    is_buyer_maker BOOLEAN NOT NULL
                );
            """)

            # Create OHLC table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ohlc (
                    time TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION NOT NULL,
                    trade_count INTEGER NOT NULL DEFAULT 0
                );
            """)

            # Create analytics snapshots table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS analytics_snapshots (
                    time TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    pair_symbol TEXT,
                    last_price DOUBLE PRECISION,
                    spread DOUBLE PRECISION,
                    hedge_ratio DOUBLE PRECISION,
                    z_score DOUBLE PRECISION,
                    correlation DOUBLE PRECISION,
                    adf_statistic DOUBLE PRECISION,
                    adf_pvalue DOUBLE PRECISION,
                    is_stationary BOOLEAN,
                    tick_count INTEGER
                );
            """)

            # Create alerts history table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts_history (
                    time TIMESTAMPTZ NOT NULL,
                    alert_id TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    message TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    value DOUBLE PRECISION,
                    threshold DOUBLE PRECISION,
                    acknowledged BOOLEAN DEFAULT FALSE
                );
            """)

            # Try to convert to hypertables (TimescaleDB specific)
            # These will silently fail if TimescaleDB extension is not installed
            try:
                await conn.execute(
                    "SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE);"
                )
                await conn.execute(
                    "SELECT create_hypertable('ohlc', 'time', if_not_exists => TRUE);"
                )
                await conn.execute(
                    "SELECT create_hypertable('analytics_snapshots', 'time', if_not_exists => TRUE);"
                )
                await conn.execute(
                    "SELECT create_hypertable('alerts_history', 'time', if_not_exists => TRUE);"
                )
                self._log("schema", None, "Hypertables created/verified")
            except Exception as e:
                # TimescaleDB extension might not be installed
                self._log("schema", None, f"Hypertable creation skipped: {e}")

            # Create indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time
                ON ticks (symbol, time DESC);
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ohlc_symbol_interval_time
                ON ohlc (symbol, interval, time DESC);
            """)

        self._log("schema", None, "Schema initialized")

    # ==================== Tick Operations ====================

    async def insert_ticks_batch(self, ticks: List[Dict]) -> int:
        """
        Batch insert ticks into TimescaleDB.

        Args:
            ticks: List of tick dictionaries

        Returns:
            Number of inserted rows
        """
        if not ticks:
            return 0

        start = time.time()

        # Prepare data for COPY
        records = [
            (
                datetime.fromtimestamp(t["timestamp"] / 1000),
                t["symbol"],
                t["trade_id"],
                t["price"],
                t["qty"],
                t["is_buyer_maker"]
            )
            for t in ticks
        ]

        async with self._pool.acquire() as conn:
            result = await conn.copy_records_to_table(
                "ticks",
                records=records,
                columns=["time", "symbol", "trade_id", "price", "qty", "is_buyer_maker"]
            )

        duration = (time.time() - start) * 1000
        self._log("insert_batch", "ticks", f"Inserted {len(ticks)} ticks", duration)
        return len(ticks)

    async def insert_ohlc_batch(self, bars: List[Dict]) -> int:
        """
        Batch insert OHLC bars into TimescaleDB.

        Args:
            bars: List of OHLC bar dictionaries

        Returns:
            Number of inserted rows
        """
        if not bars:
            return 0

        start = time.time()

        records = [
            (
                datetime.fromtimestamp(b["timestamp"] / 1000),
                b["symbol"],
                b["interval"],
                b["open"],
                b["high"],
                b["low"],
                b["close"],
                b["volume"],
                b.get("trade_count", 0)
            )
            for b in bars
        ]

        async with self._pool.acquire() as conn:
            await conn.copy_records_to_table(
                "ohlc",
                records=records,
                columns=["time", "symbol", "interval", "open", "high", "low", "close", "volume", "trade_count"]
            )

        duration = (time.time() - start) * 1000
        self._log("insert_batch", "ohlc", f"Inserted {len(bars)} bars", duration)
        return len(bars)

    async def insert_analytics_snapshot(self, snapshot: Dict) -> None:
        """Insert a single analytics snapshot."""
        start = time.time()

        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO analytics_snapshots
                (time, symbol, pair_symbol, last_price, spread, hedge_ratio,
                 z_score, correlation, adf_statistic, adf_pvalue, is_stationary, tick_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
                datetime.fromtimestamp(snapshot["timestamp"] / 1000),
                snapshot.get("symbol"),
                snapshot.get("pair_symbol"),
                snapshot.get("last_price"),
                snapshot.get("spread"),
                snapshot.get("hedge_ratio"),
                snapshot.get("z_score"),
                snapshot.get("correlation"),
                snapshot.get("adf_statistic"),
                snapshot.get("adf_pvalue"),
                snapshot.get("is_stationary"),
                snapshot.get("tick_count")
            )

        duration = (time.time() - start) * 1000
        self._log("insert", "analytics_snapshots", "Inserted snapshot", duration)

    async def archive_alert(self, alert: Dict) -> None:
        """
        Archive an alert to cold storage (historical record).

        Active alerts should be in Redis. This stores a copy for
        historical analysis and compliance.
        """
        start = time.time()

        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO alerts_history
                (time, alert_id, alert_type, symbol, message, severity, value, threshold, acknowledged)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                datetime.fromtimestamp(alert["timestamp"] / 1000),
                alert["id"],
                alert["alert_type"],
                alert["symbol"],
                alert["message"],
                alert["severity"],
                alert.get("value"),
                alert.get("threshold"),
                alert.get("acknowledged", False)
            )

        duration = (time.time() - start) * 1000
        self._log("archive", "alerts_history", "Archived alert", duration)

    # ==================== Query Operations ====================

    async def get_ticks(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 10000
    ) -> List[Dict]:
        """
        Query ticks for a symbol within time range.

        Args:
            symbol: Trading pair symbol
            start_time: Start of range
            end_time: End of range
            limit: Maximum rows to return

        Returns:
            List of tick dictionaries
        """
        start = time.time()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT time, symbol, trade_id, price, qty, is_buyer_maker
                FROM ticks
                WHERE symbol = $1 AND time >= $2 AND time <= $3
                ORDER BY time DESC
                LIMIT $4
            """, symbol.upper(), start_time, end_time, limit)

        result = [dict(row) for row in rows]
        duration = (time.time() - start) * 1000
        self._log("query", "ticks", f"Retrieved {len(result)} ticks", duration)
        return result

    async def get_ohlc(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000
    ) -> List[Dict]:
        """
        Query OHLC bars for a symbol.

        Args:
            symbol: Trading pair symbol
            interval: Candle interval (1s, 1m, 5m)
            start_time: Start of range
            end_time: End of range
            limit: Maximum rows to return

        Returns:
            List of OHLC bar dictionaries
        """
        start = time.time()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT time, symbol, interval, open, high, low, close, volume, trade_count
                FROM ohlc
                WHERE symbol = $1 AND interval = $2 AND time >= $3 AND time <= $4
                ORDER BY time ASC
                LIMIT $5
            """, symbol.upper(), interval, start_time, end_time, limit)

        result = [dict(row) for row in rows]
        duration = (time.time() - start) * 1000
        self._log("query", "ohlc", f"Retrieved {len(result)} bars", duration)
        return result

    async def compute_ohlc_from_ticks(
        self,
        symbol: str,
        interval_seconds: int,
        start_time: datetime,
        end_time: datetime,
        limit: int = 500
    ) -> List[Dict]:
        """
        Compute OHLC bars on-the-fly from raw ticks using SQL aggregation.

        This is a fallback when pre-computed OHLC data is not available.
        """
        from datetime import timedelta
        start = time.time()

        # Use timedelta for asyncpg - it converts to PostgreSQL interval correctly
        interval_td = timedelta(seconds=interval_seconds)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    time_bucket($1, time) AS time,
                    (array_agg(price ORDER BY time ASC))[1] AS open,
                    MAX(price) AS high,
                    MIN(price) AS low,
                    (array_agg(price ORDER BY time DESC))[1] AS close,
                    SUM(qty) AS volume,
                    COUNT(*) AS trade_count
                FROM ticks
                WHERE symbol = $2 AND time >= $3 AND time <= $4
                GROUP BY time_bucket($1, time)
                ORDER BY time ASC
                LIMIT $5
            """, interval_td, symbol.upper(), start_time, end_time, limit)

        result = [dict(row) for row in rows]
        duration = (time.time() - start) * 1000
        self._log("query", "ticks->ohlc", f"Computed {len(result)} bars from ticks", duration)
        return result
    async def get_analytics_history(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000
    ) -> List[Dict]:
        """Query historical analytics snapshots."""
        start = time.time()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT *
                FROM analytics_snapshots
                WHERE symbol = $1 AND time >= $2 AND time <= $3
                ORDER BY time DESC
                LIMIT $4
            """, symbol.upper(), start_time, end_time, limit)

        result = [dict(row) for row in rows]
        duration = (time.time() - start) * 1000
        self._log("query", "analytics_snapshots", f"Retrieved {len(result)} snapshots", duration)
        return result

    async def get_pair_analytics_history(
        self,
        symbol_a: str,
        symbol_b: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000
    ) -> List[Dict]:
        """
        Query historical pair analytics snapshots for charting.

        Args:
            symbol_a: First symbol of the pair
            symbol_b: Second symbol of the pair
            start_time: Start of range
            end_time: End of range
            limit: Maximum rows to return

        Returns:
            List of analytics dictionaries with time, spread, z_score, etc.
        """
        start = time.time()
        pair_symbol = symbol_b.upper()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    time,
                    symbol,
                    pair_symbol,
                    spread,
                    hedge_ratio,
                    z_score,
                    correlation,
                    adf_statistic,
                    adf_pvalue,
                    is_stationary,
                    tick_count
                FROM analytics_snapshots
                WHERE symbol = $1 AND pair_symbol = $2 AND time >= $3 AND time <= $4
                ORDER BY time ASC
                LIMIT $5
            """, symbol_a.upper(), pair_symbol, start_time, end_time, limit)

        result = [dict(row) for row in rows]
        duration = (time.time() - start) * 1000
        self._log("query", "analytics_snapshots", f"Retrieved {len(result)} pair snapshots", duration)
        return result
    async def get_alerts_history(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Query alert history with optional filters."""
        start = time.time()

        query = "SELECT * FROM alerts_history WHERE 1=1"
        params = []
        param_idx = 1

        if symbol:
            query += f" AND symbol = ${param_idx}"
            params.append(symbol.upper())
            param_idx += 1

        if start_time:
            query += f" AND time >= ${param_idx}"
            params.append(start_time)
            param_idx += 1

        if end_time:
            query += f" AND time <= ${param_idx}"
            params.append(end_time)
            param_idx += 1

        query += f" ORDER BY time DESC LIMIT ${param_idx}"
        params.append(limit)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        result = [dict(row) for row in rows]
        duration = (time.time() - start) * 1000
        self._log("query", "alerts_history", f"Retrieved {len(result)} alerts", duration)
        return result

    # ==================== Export Operations ====================

    async def export_to_csv(self, query: str, params: tuple, filepath: str) -> int:
        """
        Export query results to CSV file.

        Args:
            query: SQL query
            params: Query parameters
            filepath: Output file path

        Returns:
            Number of rows exported
        """
        start = time.time()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

            if not rows:
                return 0

            import csv
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                for row in rows:
                    writer.writerow(dict(row))

        duration = (time.time() - start) * 1000
        self._log("export", filepath, f"Exported {len(rows)} rows to CSV", duration)
        return len(rows)


# Factory function
_clients: Dict[str, TimescaleClient] = {}


async def get_timescale_client(service_name: str, log_callback: Optional[Callable] = None) -> TimescaleClient:
    """
    Get or create a TimescaleDB client for a service.

    Args:
        service_name: Unique service identifier
        log_callback: Optional logging callback

    Returns:
        Connected TimescaleClient instance
    """
    if service_name not in _clients:
        client = TimescaleClient(service_name, log_callback)
        await client.connect()
        _clients[service_name] = client
    return _clients[service_name]


async def close_all_clients() -> None:
    """Close all TimescaleDB client connections."""
    for client in _clients.values():
        await client.disconnect()
    _clients.clear()
