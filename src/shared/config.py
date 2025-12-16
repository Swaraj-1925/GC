from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List, Optional
from functools import lru_cache

class Settings(BaseSettings):

    APP_NAME: str = "GemScap Quant Analytics"
    DEBUG: bool = True

    REDIS_URL: Optional[str] = Field(
        default=None,
        description="Full Redis URL (takes precedence over individual settings)"
    )
    TIMESCALE_URL: Optional[str] = Field(
        default=None,
        description="Full PostgreSQL URL (takes precedence over individual settings)"
    )

    BINANCE_WS_URL: str = "wss://fstream.binance.com/ws"
    SYMBOLS: List[str] = ["btcusdt", "ethusdt"]

    # Analytics Configuration
    ROLLING_WINDOW_TICKS: int = 100  # Number of ticks for rolling calculations
    OHLC_INTERVALS: List[str] = ["1s", "1m", "5m"]
    Z_SCORE_ALERT_THRESHOLD: float = 2.0

    ARCHIVE_BATCH_SIZE: int = 1000
    ARCHIVE_INTERVAL_SECONDS: int = 60  # How often to archive to TimescaleDB

    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8080

    LOG_DIR: str = "logs"
    LOG_MAX_SIZE_MB: int = 10
    LOG_BACKUP_COUNT: int = 5

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()

class RedisKeys:
    """Centralized Redis key patterns."""

    @staticmethod
    def tick_stream(symbol: str) -> str:
        """Stream for real-time ticks: stream:ticks:BTCUSDT"""
        return f"stream:ticks:{symbol.upper()}"

    @staticmethod
    def price_timeseries(symbol: str) -> str:
        """TimeSeries for raw prices: ts:price:BTCUSDT"""
        return f"ts:price:{symbol.upper()}"

    @staticmethod
    def ohlc_timeseries(symbol: str, interval: str) -> str:
        """TimeSeries for OHLC: ts:ohlc:BTCUSDT:1m"""
        return f"ts:ohlc:{symbol.upper()}:{interval}"

    @staticmethod
    def analytics_state(symbol: str) -> str:
        """Hash for latest analytics: state:analytics:BTCUSDT"""
        return f"state:analytics:{symbol.upper()}"

    @staticmethod
    def pair_analytics_state(symbol_a: str, symbol_b: str) -> str:
        """Hash for pair analytics: state:pair:BTCUSDT:ETHUSDT"""
        return f"state:pair:{symbol_a.upper()}:{symbol_b.upper()}"

    # Pub/Sub channels
    CHANNEL_ALERTS = "channel:alerts"
    CHANNEL_LOGS = "channel:logs"
