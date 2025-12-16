from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime
from enum import Enum


class TickData(BaseModel):
    """
    Real-time tick data from Binance.
    Maps to Binance trade event fields.
    """
    symbol: str = Field(..., description="Trading pair symbol, e.g., BTCUSDT")
    trade_id: int = Field(..., alias="tradeId", description="Unique trade identifier")
    price: float = Field(..., description="Trade price")
    qty: float = Field(..., description="Trade quantity")
    timestamp: int = Field(..., description="Event timestamp in milliseconds")
    is_buyer_maker: bool = Field(..., alias="isBuyerMaker", description="True if buyer is market maker")

    class Config:
        populate_by_name = True

    def to_redis_dict(self) -> dict:
        """Convert to dict for Redis storage."""
        return {
            "symbol": self.symbol,
            "trade_id": str(self.trade_id),
            "price": str(self.price),
            "qty": str(self.qty),
            "timestamp": str(self.timestamp),
            "is_buyer_maker": "1" if self.is_buyer_maker else "0"
        }

    @classmethod
    def from_redis_dict(cls, data: dict) -> "TickData":
        """Create from Redis hash data."""
        return cls(
            symbol=data["symbol"],
            tradeId=int(data["trade_id"]),
            price=float(data["price"]),
            qty=float(data["qty"]),
            timestamp=int(data["timestamp"]),
            isBuyerMaker=data["is_buyer_maker"] == "1"
        )


class OHLCBar(BaseModel):
    """
    OHLC candlestick bar.
    """
    symbol: str = Field(..., description="Trading pair symbol")
    timestamp: int = Field(..., description="Bar open timestamp in milliseconds")
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="Highest price")
    low: float = Field(..., description="Lowest price")
    close: float = Field(..., description="Closing price")
    volume: float = Field(..., description="Total volume traded")
    trade_count: int = Field(default=0, description="Number of trades in bar")

    def to_redis_dict(self) -> dict:
        """Convert to dict for Redis storage."""
        return {
            "symbol": self.symbol,
            "timestamp": str(self.timestamp),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "trade_count": str(self.trade_count)
        }


class DataValidityStatus(str, Enum):
    """Indicates if enough data is present for reliable analytics."""
    INSUFFICIENT = "insufficient"  # Not enough data points
    WARMING_UP = "warming_up"      # Building up, but not yet reliable
    VALID = "valid"                # Sufficient data for valid analytics


class AnalyticsSnapshot(BaseModel):
    """
    Point-in-time analytics snapshot for a symbol or pair.
    """
    symbol: str = Field(..., description="Primary symbol")
    pair_symbol: Optional[str] = Field(None, description="Secondary symbol for pair analytics")
    timestamp: int = Field(..., description="Snapshot timestamp in milliseconds")

    # Price statistics
    last_price: float = Field(..., description="Most recent price")
    price_change_pct: Optional[float] = Field(None, description="Price change percentage")
    vwap: Optional[float] = Field(None, description="Volume-weighted average price")

    # Pair analytics (only for pair mode)
    spread: Optional[float] = Field(None, description="Price spread between pair")
    hedge_ratio: Optional[float] = Field(None, description="OLS hedge ratio")
    z_score: Optional[float] = Field(None, description="Z-score of spread")
    correlation: Optional[float] = Field(None, description="Rolling correlation")

    # Stationarity
    adf_statistic: Optional[float] = Field(None, description="ADF test statistic")
    adf_pvalue: Optional[float] = Field(None, description="ADF test p-value")
    is_stationary: Optional[bool] = Field(None, description="Spread is stationary")

    # Data quality indicators
    data_freshness_ms: int = Field(..., description="Time since last tick in ms")
    validity_status: DataValidityStatus = Field(..., description="Data validity status")
    tick_count: int = Field(..., description="Number of ticks in rolling window")

    def to_redis_dict(self) -> dict:
        """Convert to dict for Redis hash storage."""
        result = {}
        for key, value in self.model_dump().items():
            if value is not None:
                if isinstance(value, bool):
                    result[key] = "1" if value else "0"
                elif isinstance(value, Enum):
                    result[key] = value.value
                else:
                    result[key] = str(value)
        return result


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AlertType(str, Enum):
    """Types of alerts."""
    Z_SCORE_HIGH = "z_score_high"
    Z_SCORE_LOW = "z_score_low"
    CORRELATION_BREAK = "correlation_break"
    DATA_STALE = "data_stale"
    STATIONARITY_CHANGE = "stationarity_change"
    CUSTOM = "custom"


class Alert(BaseModel):
    """
    Alert notification.
    """
    id: str = Field(..., description="Unique alert identifier")
    alert_type: AlertType = Field(..., description="Type of alert")
    symbol: str = Field(..., description="Symbol triggering alert")
    message: str = Field(..., description="Human-readable alert message")
    timestamp: int = Field(..., description="Alert timestamp in milliseconds")
    severity: AlertSeverity = Field(default=AlertSeverity.INFO)
    value: Optional[float] = Field(None, description="Value that triggered alert")
    threshold: Optional[float] = Field(None, description="Threshold that was breached")
    acknowledged: bool = Field(default=False)

    def to_redis_dict(self) -> dict:
        """Convert to dict for Redis storage."""
        return {
            "id": self.id,
            "alert_type": self.alert_type.value,
            "symbol": self.symbol,
            "message": self.message,
            "timestamp": str(self.timestamp),
            "severity": self.severity.value,
            "value": str(self.value) if self.value else "",
            "threshold": str(self.threshold) if self.threshold else "",
            "acknowledged": "1" if self.acknowledged else "0"
        }


class AlertRule(BaseModel):
    """
    User-defined alert rule.
    """
    id: str = Field(..., description="Rule identifier")
    symbol: str = Field(..., description="Symbol to monitor")
    metric: str = Field(..., description="Metric to watch (e.g., z_score, correlation)")
    operator: Literal["gt", "lt", "gte", "lte", "eq"] = Field(..., description="Comparison operator")
    threshold: float = Field(..., description="Threshold value")
    severity: AlertSeverity = Field(default=AlertSeverity.WARNING)
    enabled: bool = Field(default=True)


class LogEntry(BaseModel):
    """Structured log entry for centralized logging."""
    timestamp: int = Field(..., description="Log timestamp in milliseconds")
    service: str = Field(..., description="Service name")
    level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = Field(default="INFO")
    operation: str = Field(..., description="Operation type (e.g., redis_read, redis_write)")
    key: Optional[str] = Field(None, description="Redis key if applicable")
    message: str = Field(..., description="Log message")
    duration_ms: Optional[int] = Field(None, description="Operation duration")
