from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class SymbolInfo(BaseModel):
    """Symbol information."""
    symbol: str
    is_active: bool
    last_update: Optional[int] = None


class AnalyticsResponse(BaseModel):
    """Analytics snapshot response."""
    symbol: str
    pair_symbol: Optional[str] = None
    timestamp: int
    last_price: Optional[float] = None
    price_change_pct: Optional[float] = None
    vwap: Optional[float] = None
    spread: Optional[float] = None
    hedge_ratio: Optional[float] = None
    z_score: Optional[float] = None
    correlation: Optional[float] = None
    adf_statistic: Optional[float] = None
    adf_pvalue: Optional[float] = None
    is_stationary: Optional[bool] = None
    data_freshness_ms: int
    validity_status: str
    tick_count: int

class OHLCBar(BaseModel):
    """OHLC bar for charting."""
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class AlertRuleCreate(BaseModel):
    """Alert rule creation request."""
    symbol: str
    metric: str
    operator: str
    threshold: float
    severity: str = "warning"


class AlertRuleResponse(BaseModel):
    """Alert rule response."""
    id: str
    symbol: str
    metric: str
    operator: str
    threshold: float
    severity: str
    enabled: bool

class PairAnalyticsHistoryPoint(BaseModel):
    """Single data point for pair analytics time series."""
    time: int  # Unix timestamp in seconds
    spread: Optional[float] = None
    hedge_ratio: Optional[float] = None
    z_score: Optional[float] = None
    correlation: Optional[float] = None
    adf_pvalue: Optional[float] = None
    is_stationary: Optional[bool] = None
