import redis.asyncio as redis
import json
import time
from typing import Optional, Dict, List, Any, Callable
from contextlib import asynccontextmanager

from ..config import get_settings

class RedisClient:
    """
    Async Redis client wrapper with observability hooks.

    Each service should create its own instance to maintain
    max 5 connections as per architecture requirements.
    """

    def __init__(self, service_name: str, log_callback: Optional[Callable] = None):
        """
        Initialize Redis client for a specific service.

        Args:
            service_name: Name of the service using this client
            log_callback: Optional callback for logging operations
        """
        self.service_name = service_name
        self.log_callback = log_callback
        self._client: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None
        self.settings = get_settings()

    async def connect(self) -> None:
        """
        Establish connection to Redis.

        Supports both local Redis and Redis Cloud (with SSL).
        Uses REDIS_URL if provided, otherwise builds URL from components.
        """
        self._client = redis.Redis.from_url(
            self.settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10,
            retry_on_timeout=True
        )

        # Test connection
        await self._client.ping()

        # Log connection (mask password in URL for security)
        safe_url = self.settings.REDIS_URL.split("@")[-1] if "@" in self.settings.REDIS_URL else "redis_url"
        self._log("connect", None, f"Connected to Redis: {safe_url}")

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._pubsub:
            await self._pubsub.close()
        if self._client:
            await self._client.close()
        self._log("disconnect", None, "Disconnected from Redis")

    def pipeline(self, transaction: bool = True) -> redis.client.Pipeline:
        """
        Create a Redis pipeline.

        Args:
            transaction: Whether to use MULTI/EXEC

        Returns:
            Redis pipeline object
        """
        return self._client.pipeline(transaction=transaction)

    def _log(self, operation: str, key: Optional[str], message: str, duration_ms: float = 0) -> None:
        """Log operation if callback is set."""
        if self.log_callback:
            self.log_callback({
                "timestamp": int(time.time() * 1000),
                "service": self.service_name,
                "operation": operation,
                "key": key,
                "message": message,
                "duration_ms": duration_ms
            })

    # ==================== Stream Operations ====================

    # 24 hours in milliseconds for stream retention
    STREAM_RETENTION_MS = 24 * 60 * 60 * 1000

    async def stream_add(
        self,
        stream_key: str,
        data: Dict[str, str],
        maxlen: Optional[int] = None,
        retention_hours: int = 24
    ) -> str:
        """
        Add entry to a Redis stream with automatic time-based trimming.

        Removes entries older than retention_hours to keep max 24h of data.

        Args:
            stream_key: Stream name
            data: Dictionary of field-value pairs
            maxlen: Optional max length (if None, uses time-based trimming)
            retention_hours: Hours of data to retain (default 24)

        Returns:
            Entry ID
        """
        start = time.time()

        # Calculate MINID for time-based trimming (remove entries older than retention)
        min_timestamp = int((time.time() - retention_hours * 3600) * 1000)
        minid = f"{min_timestamp}-0"

        # Add with MINID trimming to remove old entries
        entry_id = await self._client.xadd(
            stream_key,
            data,
            minid=minid,  # Remove entries older than this timestamp
            approximate=True
        )

        duration = (time.time() - start) * 1000
        self._log("stream_write", stream_key, f"Added entry {entry_id}", duration)
        return entry_id

    async def stream_read(
        self,
        streams: Dict[str, str],
        count: int = 100,
        block: int = 1000
    ) -> List:
        """
        Read from one or more streams.

        Args:
            streams: Dict of stream_name -> last_id (use '0' or '$' for start/end)
            count: Max entries to return per stream
            block: Block timeout in milliseconds (0 = no block)

        Returns:
            List of [stream_name, [(entry_id, data), ...]]
        """
        start = time.time()
        result = await self._client.xread(streams, count=count, block=block)
        duration = (time.time() - start) * 1000
        keys = list(streams.keys())
        self._log("stream_read", ",".join(keys), f"Read {len(result) if result else 0} entries", duration)
        return result or []

    async def stream_xrange(
        self,
        stream_key: str,
        start_id: str = "-",
        end_id: str = "+",
        count: Optional[int] = None
    ) -> List:
        """
        Read entries from a stream within a time range using XRANGE.

        Args:
            stream_key: Stream name
            start_id: Start ID (use '-' for oldest, or timestamp-0 for time-based)
            end_id: End ID (use '+' for newest, or timestamp-0 for time-based)
            count: Optional max entries to return

        Returns:
            List of (entry_id, data_dict) tuples
        """
        start = time.time()
        if count:
            result = await self._client.xrange(stream_key, start_id, end_id, count=count)
        else:
            result = await self._client.xrange(stream_key, start_id, end_id)
        duration = (time.time() - start) * 1000
        self._log("stream_xrange", stream_key, f"Read {len(result) if result else 0} entries", duration)
        return result or []

    async def stream_read_group(
        self,
        group_name: str,
        consumer_name: str,
        streams: Dict[str, str],
        count: int = 100,
        block: int = 1000
    ) -> List:
        """
        Read from streams using consumer group.

        Args:
            group_name: Consumer group name
            consumer_name: Consumer name within group
            streams: Dict of stream_name -> '>' for new messages
            count: Max entries per stream
            block: Block timeout in ms
        """
        start = time.time()
        result = await self._client.xreadgroup(
            group_name, consumer_name, streams, count=count, block=block
        )
        duration = (time.time() - start) * 1000
        self._log("stream_read_group", group_name, f"Read {len(result) if result else 0} entries", duration)
        return result or []

    async def stream_create_group(
        self,
        stream_key: str,
        group_name: str,
        start_id: str = "0"
    ) -> bool:
        """Create consumer group if it doesn't exist."""
        try:
            await self._client.xgroup_create(stream_key, group_name, start_id, mkstream=True)
            self._log("stream_create_group", stream_key, f"Created group {group_name}")
            return True
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                return False  # Group already exists
            raise

    # ==================== TimeSeries Operations ====================

    # 24 hours retention for TimeSeries
    TS_RETENTION_MS = 24 * 60 * 60 * 1000  # 24 hours

    async def ts_add(
        self,
        key: str,
        timestamp: int,
        value: float,
        retention_ms: int = 86400000  # 24 hours default
    ) -> int:
        """
        Add value to a time series.

        Args:
            key: TimeSeries key
            timestamp: Unix timestamp in milliseconds
            value: Numeric value
            retention_ms: Data retention period

        Returns:
            Timestamp of added sample
        """
        start = time.time()
        try:
            result = await self._client.execute_command(
                "TS.ADD", key, timestamp, value,
                "RETENTION", retention_ms,
                "ON_DUPLICATE", "LAST"
            )
        except redis.ResponseError as e:
            # If key doesn't exist, create it first
            if "TSDB" in str(e) or "ERR" in str(e):
                await self._client.execute_command(
                    "TS.CREATE", key,
                    "RETENTION", retention_ms,
                    "DUPLICATE_POLICY", "LAST"
                )
                result = await self._client.execute_command(
                    "TS.ADD", key, timestamp, value
                )
            else:
                raise
        duration = (time.time() - start) * 1000
        self._log("ts_write", key, f"Added value at {timestamp}", duration)
        return result

    async def ts_range(
        self,
        key: str,
        from_ts: int,
        to_ts: int,
        aggregation: Optional[str] = None,
        bucket_size_ms: int = 60000
    ) -> List:
        """
        Query time series range.

        Args:
            key: TimeSeries key
            from_ts: Start timestamp (use '-' for oldest)
            to_ts: End timestamp (use '+' for newest)
            aggregation: Optional aggregation (avg, sum, min, max, count)
            bucket_size_ms: Bucket size for aggregation

        Returns:
            List of [timestamp, value] pairs
        """
        start = time.time()
        cmd = ["TS.RANGE", key, from_ts, to_ts]
        if aggregation:
            cmd.extend(["AGGREGATION", aggregation, bucket_size_ms])

        try:
            result = await self._client.execute_command(*cmd)
        except redis.ResponseError:
            result = []

        duration = (time.time() - start) * 1000
        self._log("ts_read", key, f"Retrieved {len(result)} samples", duration)
        return result

    async def ts_mget(self, filter_expr: str) -> List:
        """
        Get latest values from multiple time series matching filter.

        Args:
            filter_expr: Label filter expression (e.g., "type=price")

        Returns:
            List of [key, labels, timestamp, value]
        """
        start = time.time()
        result = await self._client.execute_command("TS.MGET", "FILTER", filter_expr)
        duration = (time.time() - start) * 1000
        self._log("ts_mget", filter_expr, f"Retrieved {len(result)} series", duration)
        return result

    # ==================== Hash Operations ====================

    async def hash_set(self, key: str, mapping: Dict[str, str]) -> int:
        """
        Set multiple fields in a hash.

        Args:
            key: Hash key
            mapping: Field-value pairs

        Returns:
            Number of fields added
        """
        start = time.time()
        result = await self._client.hset(key, mapping=mapping)
        duration = (time.time() - start) * 1000
        self._log("hash_write", key, f"Set {len(mapping)} fields", duration)
        return result

    async def hash_get_all(self, key: str) -> Dict[str, str]:
        """
        Get all fields from a hash.

        Args:
            key: Hash key

        Returns:
            Dict of all field-value pairs
        """
        start = time.time()
        result = await self._client.hgetall(key)
        duration = (time.time() - start) * 1000
        self._log("hash_read", key, f"Got {len(result)} fields", duration)
        return result

    async def hash_get(self, key: str, field: str) -> Optional[str]:
        """Get single field from hash."""
        start = time.time()
        result = await self._client.hget(key, field)
        duration = (time.time() - start) * 1000
        self._log("hash_read", key, f"Got field {field}", duration)
        return result

    # ==================== Pub/Sub Operations ====================

    async def publish(self, channel: str, message: Any) -> int:
        """
        Publish message to a channel.

        Args:
            channel: Channel name
            message: Message (will be JSON serialized if not string)

        Returns:
            Number of subscribers that received the message
        """
        if not isinstance(message, str):
            message = json.dumps(message)

        start = time.time()
        result = await self._client.publish(channel, message)
        duration = (time.time() - start) * 1000
        self._log("pubsub_publish", channel, f"Published to {result} subscribers", duration)
        return result

    async def subscribe(self, *channels: str) -> redis.client.PubSub:
        """
        Subscribe to channels.

        Args:
            channels: Channel names to subscribe to

        Returns:
            PubSub object for receiving messages
        """
        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(*channels)
        self._log("pubsub_subscribe", ",".join(channels), "Subscribed to channels")
        return self._pubsub

    async def get_message(self, timeout: float = 1.0) -> Optional[Dict]:
        """
        Get next message from subscribed channels.

        Args:
            timeout: Timeout in seconds

        Returns:
            Message dict or None
        """
        if not self._pubsub:
            return None
        return await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)

    # ==================== Alert Operations (Hot Storage) ====================

    async def add_alert(self, alert: Dict[str, Any], ttl_hours: int = 24) -> str:
        """
        Add an alert to hot storage with TTL.

        Alerts are stored in Redis for real-time access.
        Older alerts should be archived to TimescaleDB.

        Args:
            alert: Alert dictionary with 'id' field
            ttl_hours: Hours to keep alert in hot storage

        Returns:
            Alert ID
        """
        start = time.time()
        alert_id = alert.get("id", str(int(time.time() * 1000)))
        key = f"alert:{alert_id}"

        # Store as hash
        await self._client.hset(key, mapping={k: str(v) for k, v in alert.items()})
        await self._client.expire(key, ttl_hours * 3600)

        # Add to sorted set for ordered retrieval (score = timestamp)
        timestamp = alert.get("timestamp", int(time.time() * 1000))
        await self._client.zadd("alerts:active", {alert_id: timestamp})

        duration = (time.time() - start) * 1000
        self._log("alert_write", key, f"Added alert {alert_id}", duration)
        return alert_id

    async def get_active_alerts(
        self,
        limit: int = 50,
        symbol: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """
        Get recent active alerts from hot storage.

        Args:
            limit: Max alerts to return
            symbol: Optional filter by symbol

        Returns:
            List of alert dictionaries
        """
        start = time.time()

        # Get recent alert IDs from sorted set (newest first)
        alert_ids = await self._client.zrevrange("alerts:active", 0, limit - 1)

        alerts = []
        for alert_id in alert_ids:
            alert_data = await self._client.hgetall(f"alert:{alert_id}")
            if alert_data:
                if symbol is None or alert_data.get("symbol", "").upper() == symbol.upper():
                    alerts.append(alert_data)

        duration = (time.time() - start) * 1000
        self._log("alert_read", "alerts:active", f"Retrieved {len(alerts)} alerts", duration)
        return alerts

    async def acknowledge_alert(self, alert_id: str) -> bool:
        """
        Mark an alert as acknowledged.

        Args:
            alert_id: Alert ID to acknowledge

        Returns:
            True if alert existed and was updated
        """
        key = f"alert:{alert_id}"
        exists = await self._client.exists(key)

        if exists:
            await self._client.hset(key, "acknowledged", "1")
            return True
        return False

    async def cleanup_old_alerts(self, max_age_hours: int = 24) -> int:
        """
        Remove alerts older than max_age_hours from the active set.

        Args:
            max_age_hours: Max age in hours

        Returns:
            Number of alerts removed
        """
        min_timestamp = int((time.time() - max_age_hours * 3600) * 1000)
        removed = await self._client.zremrangebyscore("alerts:active", "-inf", min_timestamp)
        self._log("alert_cleanup", "alerts:active", f"Removed {removed} old alerts")
        return removed

    # ==================== Utility Operations ====================

    async def set_with_expiry(self, key: str, value: str, ttl_seconds: int) -> bool:
        """Set a key with expiration."""
        start = time.time()
        result = await self._client.setex(key, ttl_seconds, value)
        duration = (time.time() - start) * 1000
        self._log("set_write", key, f"Set with TTL {ttl_seconds}s", duration)
        return result

    async def get(self, key: str) -> Optional[str]:
        """Get a string value."""
        start = time.time()
        result = await self._client.get(key)
        duration = (time.time() - start) * 1000
        self._log("get_read", key, "Retrieved value", duration)
        return result

    async def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        start = time.time()
        result = await self._client.delete(*keys)
        duration = (time.time() - start) * 1000
        self._log("delete", ",".join(keys), f"Deleted {result} keys", duration)
        return result

    async def keys(self, pattern: str) -> List[str]:
        """Get keys matching pattern."""
        start = time.time()
        result = await self._client.keys(pattern)
        duration = (time.time() - start) * 1000
        self._log("keys_read", pattern, f"Found {len(result)} keys", duration)
        return result


# Factory function for creating clients
_clients: Dict[str, RedisClient] = {}


async def get_redis_client(service_name: str, log_callback: Optional[Callable] = None) -> RedisClient:
    """
    Get or create a Redis client for a service.

    Args:
        service_name: Unique service identifier
        log_callback: Optional logging callback

    Returns:
        Connected RedisClient instance
    """
    if service_name not in _clients:
        client = RedisClient(service_name, log_callback)
        await client.connect()
        _clients[service_name] = client
    return _clients[service_name]


async def close_all_clients() -> None:
    """Close all Redis client connections."""
    for client in _clients.values():
        await client.disconnect()
    _clients.clear()
