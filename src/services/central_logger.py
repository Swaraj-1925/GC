import asyncio
import os
import time
import json
import logging
from typing import Optional
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from ..shared.config import get_settings, RedisKeys
from ..shared.db.redis_client import RedisClient


logger = logging.getLogger(__name__)


class CentralLogger:
    """
    Centralized logging via Redis Pub/Sub.

    Subscribes to the logs channel and persists all service logs
    to rotating files. Helps isolate disk I/O from hot path services.

    Features:
    - JSON structured logging
    - File rotation by size
    - Separate files for different log levels
    - Avoids overloading on high-frequency operations
    """

    SERVICE_NAME = "central_logger"

    def __init__(self, log_dir: Optional[str] = None):
        """
        Initialize CentralLogger.

        Args:
            log_dir: Directory for log files (uses config default if None)
        """
        self.settings = get_settings()
        self.log_dir = Path(log_dir or self.settings.LOG_DIR)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.redis: Optional[RedisClient] = None
        self.running = True

        # Setup file loggers
        self._setup_file_loggers()

        # Rate limiting for high-frequency operations
        self._last_operation_log: dict = {}
        self._operation_counts: dict = {}
        self._rate_limit_interval = 1000  # ms

    def _setup_file_loggers(self) -> None:
        """Setup rotating file handlers for different log levels."""
        max_bytes = self.settings.LOG_MAX_SIZE_MB * 1024 * 1024
        backup_count = self.settings.LOG_BACKUP_COUNT

        # All logs
        self.all_handler = RotatingFileHandler(
            self.log_dir / "all.log",
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        self.all_handler.setFormatter(logging.Formatter(
            "%(asctime)s %(message)s"
        ))

        # Error logs only
        self.error_handler = RotatingFileHandler(
            self.log_dir / "errors.log",
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        self.error_handler.setFormatter(logging.Formatter(
            "%(asctime)s %(message)s"
        ))

        # Access logs (Redis operations)
        self.access_handler = RotatingFileHandler(
            self.log_dir / "access.log",
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        self.access_handler.setFormatter(logging.Formatter(
            "%(asctime)s %(message)s"
        ))

    async def start(self) -> None:
        """Start the central logger service."""
        logger.info(f"Starting CentralLogger, log directory: {self.log_dir}")

        # Initialize Redis connection
        self.redis = RedisClient(self.SERVICE_NAME)
        await self.redis.connect()

        # Subscribe to logs channel
        await self.redis.subscribe(RedisKeys.CHANNEL_LOGS)

        # Run main loop
        try:
            await self._main_loop()
        except asyncio.CancelledError:
            logger.info("CentralLogger cancelled")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the logger service."""
        logger.info("Stopping CentralLogger...")
        self.running = False

        # Close handlers
        self.all_handler.close()
        self.error_handler.close()
        self.access_handler.close()

        if self.redis:
            await self.redis.disconnect()

        logger.info("CentralLogger stopped")

    async def _main_loop(self) -> None:
        """Main loop - receive and process log messages."""
        while self.running:
            try:
                message = await self.redis.get_message(timeout=1.0)

                if message and message.get("type") == "message":
                    await self._process_log_message(message["data"])

            except Exception as e:
                logger.error(f"Error in logger loop: {e}")
                await asyncio.sleep(1)

    async def _process_log_message(self, data: str) -> None:
        """
        Process a log message from the channel.

        Args:
            data: JSON string containing log entry
        """
        try:
            log_entry = json.loads(data) if isinstance(data, str) else data

            # Apply rate limiting for high-frequency operations
            if not self._should_log(log_entry):
                return

            # Format log line
            log_line = self._format_log_entry(log_entry)

            # Write to appropriate handlers
            level = log_entry.get("level", "INFO").upper()

            # All logs go to all.log
            record = logging.LogRecord(
                name="central",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=log_line,
                args=(),
                exc_info=None
            )
            self.all_handler.emit(record)

            # Errors go to errors.log
            if level in ("ERROR", "WARN", "WARNING"):
                record.levelno = logging.ERROR if level == "ERROR" else logging.WARNING
                self.error_handler.emit(record)

            # Redis operations go to access.log
            if log_entry.get("operation") in (
                "stream_write", "stream_read", "hash_write", "hash_read",
                "ts_write", "ts_read", "pubsub_publish"
            ):
                self.access_handler.emit(record)

        except json.JSONDecodeError:
            # Plain text log
            record = logging.LogRecord(
                name="central",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=str(data),
                args=(),
                exc_info=None
            )
            self.all_handler.emit(record)
        except Exception as e:
            logger.error(f"Error processing log message: {e}")

    def _should_log(self, log_entry: dict) -> bool:
        """
        Rate limit high-frequency operations to avoid log overload.

        Args:
            log_entry: Log entry dictionary

        Returns:
            True if should log, False if rate limited
        """
        operation = log_entry.get("operation", "")

        # Always log these operations
        if operation in ("connect", "disconnect", "error", "heartbeat"):
            return True

        # Rate limit stream/ts writes (very high frequency)
        if operation in ("stream_write", "ts_write"):
            key = f"{log_entry.get('service')}:{operation}"
            now = int(time.time() * 1000)
            last_time = self._last_operation_log.get(key, 0)

            # Only log once per interval, but track count
            self._operation_counts[key] = self._operation_counts.get(key, 0) + 1

            if now - last_time < self._rate_limit_interval:
                return False

            # Log with count
            log_entry["_aggregated_count"] = self._operation_counts[key]
            self._operation_counts[key] = 0
            self._last_operation_log[key] = now

        return True

    def _format_log_entry(self, entry: dict) -> str:
        """
        Format a log entry as a structured log line.

        Args:
            entry: Log entry dictionary

        Returns:
            Formatted log string
        """
        timestamp = entry.get("timestamp", int(time.time() * 1000))
        dt = datetime.fromtimestamp(timestamp / 1000).strftime("%H:%M:%S.%f")[:-3]

        service = entry.get("service", "unknown")
        level = entry.get("level", "INFO")
        operation = entry.get("operation", "-")
        key = entry.get("key", "-")
        message = entry.get("message", "")
        duration = entry.get("duration_ms", 0)
        count = entry.get("_aggregated_count")

        # Format: [time] [service] [level] op=operation key=key msg=message dur=Xms
        parts = [
            f"[{dt}]",
            f"[{service}]",
            f"[{level}]",
            f"op={operation}",
        ]

        if key and key != "-":
            parts.append(f"key={key}")

        if message:
            parts.append(f'msg="{message}"')

        if duration > 0:
            parts.append(f"dur={duration:.2f}ms")

        if count:
            parts.append(f"count={count}")

        return " ".join(parts)


async def run_central_logger(log_dir: Optional[str] = None) -> None:
    """
    Entry point to run CentralLogger as a standalone service.

    Args:
        log_dir: Optional log directory path
    """
    central_logger = CentralLogger(log_dir)

    try:
        await central_logger.start()
    except KeyboardInterrupt:
        pass
    finally:
        await central_logger.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    asyncio.run(run_central_logger())
