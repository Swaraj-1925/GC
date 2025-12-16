import asyncio
import time
import logging
import uuid
from typing import Dict, Optional, List, Deque
from collections import deque
from dataclasses import dataclass, field

import numpy as np
from scipy import stats

from ..shared.config import get_settings, RedisKeys
from ..shared.models import (
    TickData, AnalyticsSnapshot, Alert, AlertType, AlertSeverity, DataValidityStatus
)
from ..shared.db.redis_client import RedisClient


logger = logging.getLogger(__name__)


@dataclass
class SymbolData:
    """Rolling window data for a single symbol."""
    prices: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    quantities: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    timestamps: Deque[int] = field(default_factory=lambda: deque(maxlen=200))
    last_tick_time: int = 0
    vwap_sum_pq: float = 0.0  # Sum of price * qty
    vwap_sum_q: float = 0.0   # Sum of qty


class QuantEngine:
    """
    Real-time analytics computation engine.

    Consumes ticks from Redis Streams, maintains rolling windows,
    and computes quantitative analytics for trading pairs.

    Features:
    - Dynamic stream subscription
    - Sliding-window analytics using deques
    - Optimized OLS recomputation (once per second)
    - Alert generation on threshold breaches
    """

    SERVICE_NAME = "quant_engine"

    # Minimum data points for valid analytics
    MIN_POINTS_FOR_ANALYTICS = 20
    MIN_POINTS_FOR_ADF = 50

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        window_size: int = 100,
        alert_z_threshold: float = 2.0
    ):
        """
        Initialize QuantEngine.

        Args:
            symbols: List of symbols to analyze
            window_size: Rolling window size in ticks
            alert_z_threshold: Z-score threshold for alerts
        """
        self.settings = get_settings()
        self.symbols = [s.upper() for s in (symbols or self.settings.SYMBOLS)]
        self.window_size = window_size
        self.alert_z_threshold = alert_z_threshold

        # Per-symbol data storage
        self.symbol_data: Dict[str, SymbolData] = {
            symbol: SymbolData() for symbol in self.symbols
        }

        # Stream reading positions - use "$" to only read NEW messages
        # This avoids reprocessing old data from previous runs
        self.stream_positions: Dict[str, str] = {
            RedisKeys.tick_stream(s): "$" for s in self.symbols
        }

        # Last analytics computation time
        self.last_compute_time: Dict[str, int] = {}

        # Cached analytics
        self.cached_analytics: Dict[str, AnalyticsSnapshot] = {}

        self.redis: Optional[RedisClient] = None
        self.running = True

    async def start(self) -> None:
        """Start the quant engine service."""
        logger.info(f"Starting QuantEngine for symbols: {self.symbols}")

        # Initialize Redis connection
        self.redis = RedisClient(self.SERVICE_NAME, self._log_callback)
        await self.redis.connect()

        # Run main processing loop
        try:
            await self._main_loop()
        except asyncio.CancelledError:
            logger.info("QuantEngine cancelled")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the quant engine."""
        logger.info("Stopping QuantEngine...")
        self.running = False

        if self.redis:
            await self.redis.disconnect()

        logger.info("QuantEngine stopped")

    async def _main_loop(self) -> None:
        """Main processing loop - consume ticks and compute analytics."""
        while self.running:
            try:
                # Read from all symbol streams
                streams = {
                    RedisKeys.tick_stream(s): self.stream_positions[RedisKeys.tick_stream(s)]
                    for s in self.symbols
                }

                # Block for up to 500ms waiting for new data
                results = await self.redis.stream_read(streams, count=100, block=500)

                if results:
                    for stream_name, entries in results:
                        for entry_id, data in entries:
                            self._process_tick(data)
                            self.stream_positions[stream_name] = entry_id

                # Compute and publish analytics for each symbol
                await self._compute_all_analytics()

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1)

    def _process_tick(self, data: Dict) -> None:
        """
        Process a single tick and update rolling windows.

        Args:
            data: Tick data dictionary from Redis
        """
        try:
            tick = TickData.from_redis_dict(data)
            symbol = tick.symbol.upper()

            if symbol not in self.symbol_data:
                self.symbol_data[symbol] = SymbolData()

            sd = self.symbol_data[symbol]

            # Update rolling windows
            sd.prices.append(tick.price)
            sd.quantities.append(tick.qty)
            sd.timestamps.append(tick.timestamp)
            sd.last_tick_time = tick.timestamp

            # Update VWAP accumulators
            sd.vwap_sum_pq += tick.price * tick.qty
            sd.vwap_sum_q += tick.qty

            # Trim VWAP if window is full (simple approximation)
            if len(sd.prices) > self.window_size:
                # Remove oldest contribution (approximation)
                oldest_price = sd.prices[0]
                oldest_qty = sd.quantities[0]
                sd.vwap_sum_pq -= oldest_price * oldest_qty
                sd.vwap_sum_q -= oldest_qty

        except Exception as e:
            logger.error(f"Error processing tick: {e}")

    async def _compute_all_analytics(self) -> None:
        """Compute analytics for all symbols and pairs."""
        now = int(time.time() * 1000)

        # Single symbol analytics
        for symbol in self.symbols:
            # Throttle computation to once per 500ms per symbol
            if now - self.last_compute_time.get(symbol, 0) < 500:
                continue

            snapshot = self._compute_single_symbol_analytics(symbol)
            if snapshot:
                await self._publish_analytics(symbol, snapshot)
                await self._check_alerts(snapshot)
                self.cached_analytics[symbol] = snapshot

            self.last_compute_time[symbol] = now

        # Pair analytics (for all combinations)
        if len(self.symbols) >= 2:
            for i, sym_a in enumerate(self.symbols):
                for sym_b in self.symbols[i+1:]:
                    pair_key = f"{sym_a}:{sym_b}"

                    if now - self.last_compute_time.get(pair_key, 0) < 1000:
                        continue

                    snapshot = self._compute_pair_analytics(sym_a, sym_b)
                    if snapshot:
                        await self._publish_analytics(pair_key, snapshot)
                        await self._check_alerts(snapshot)

                    self.last_compute_time[pair_key] = now

    def _compute_single_symbol_analytics(self, symbol: str) -> Optional[AnalyticsSnapshot]:
        """
        Compute analytics for a single symbol.

        Args:
            symbol: Symbol to analyze

        Returns:
            AnalyticsSnapshot or None if insufficient data
        """
        sd = self.symbol_data.get(symbol)
        if not sd or len(sd.prices) == 0:
            return None

        now = int(time.time() * 1000)
        prices = np.array(sd.prices)

        # Determine data validity
        tick_count = len(prices)
        if tick_count < self.MIN_POINTS_FOR_ANALYTICS:
            validity = DataValidityStatus.INSUFFICIENT
        elif tick_count < self.window_size:
            validity = DataValidityStatus.WARMING_UP
        else:
            validity = DataValidityStatus.VALID

        # Basic stats
        last_price = float(prices[-1])
        price_change_pct = None
        if len(prices) >= 2:
            if prices[0] != 0:
                price_change_pct = ((prices[-1] - prices[0]) / prices[0]) * 100
            else:
                price_change_pct = 0.0

        # VWAP
        vwap = None
        if sd.vwap_sum_q > 0:
            vwap = sd.vwap_sum_pq / sd.vwap_sum_q

        # Data freshness
        data_freshness = now - sd.last_tick_time if sd.last_tick_time else 0

        return AnalyticsSnapshot(
            symbol=symbol,
            pair_symbol=None,
            timestamp=now,
            last_price=last_price,
            price_change_pct=price_change_pct,
            vwap=vwap,
            spread=None,
            hedge_ratio=None,
            z_score=None,
            correlation=None,
            adf_statistic=None,
            adf_pvalue=None,
            is_stationary=None,
            data_freshness_ms=data_freshness,
            validity_status=validity,
            tick_count=tick_count
        )

    def _compute_pair_analytics(self, symbol_a: str, symbol_b: str) -> Optional[AnalyticsSnapshot]:
        """
        Compute pair analytics (spread, hedge ratio, z-score, correlation, ADF).

        Args:
            symbol_a: First symbol
            symbol_b: Second symbol

        Returns:
            AnalyticsSnapshot or None if insufficient data
        """
        sd_a = self.symbol_data.get(symbol_a)
        sd_b = self.symbol_data.get(symbol_b)

        if not sd_a or not sd_b:
            return None

        # Need same number of data points for pair analysis
        min_len = min(len(sd_a.prices), len(sd_b.prices))
        if min_len < self.MIN_POINTS_FOR_ANALYTICS:
            return None

        now = int(time.time() * 1000)

        # Use most recent aligned data
        prices_a = np.array(list(sd_a.prices)[-min_len:])
        prices_b = np.array(list(sd_b.prices)[-min_len:])

        # Determine validity
        if min_len < self.MIN_POINTS_FOR_ANALYTICS:
            validity = DataValidityStatus.INSUFFICIENT
        elif min_len < self.window_size:
            validity = DataValidityStatus.WARMING_UP
        else:
            validity = DataValidityStatus.VALID

        # OLS Hedge Ratio: prices_a = hedge_ratio * prices_b + intercept
        hedge_ratio = self._calculate_ols_hedge_ratio(prices_a, prices_b)

        # Spread: prices_a - hedge_ratio * prices_b
        spread_series = self._calculate_spread(prices_a, prices_b, hedge_ratio)
        current_spread = float(spread_series[-1])

        # Z-score of spread
        z_score = self._calculate_zscore(spread_series)

        # Rolling correlation
        correlation = self._calculate_correlation(prices_a, prices_b)

        # ADF test (only if enough data)
        adf_statistic = None
        adf_pvalue = None
        is_stationary = None
        if min_len >= self.MIN_POINTS_FOR_ADF:
            adf_result = self._adf_test(spread_series)
            if adf_result:
                adf_statistic, adf_pvalue, is_stationary = adf_result

        # Data freshness (use older of the two)
        freshness = now - min(sd_a.last_tick_time, sd_b.last_tick_time)

        return AnalyticsSnapshot(
            symbol=symbol_a,
            pair_symbol=symbol_b,
            timestamp=now,
            last_price=float(prices_a[-1]),
            price_change_pct=None,
            vwap=None,
            spread=current_spread,
            hedge_ratio=hedge_ratio,
            z_score=z_score,
            correlation=correlation,
            adf_statistic=adf_statistic,
            adf_pvalue=adf_pvalue,
            is_stationary=is_stationary,
            data_freshness_ms=freshness,
            validity_status=validity,
            tick_count=min_len
        )

    # ==================== Analytics Functions ====================

    def _calculate_ols_hedge_ratio(self, y: np.ndarray, x: np.ndarray) -> float:
        """
        Calculate OLS hedge ratio (beta coefficient).

        y = beta * x + alpha

        Args:
            y: Dependent variable (prices of symbol A)
            x: Independent variable (prices of symbol B)

        Returns:
            Hedge ratio (beta)
        """
        if len(x) < 2:
            return 0.0

        try:
            # Add constant for intercept
            x_mean = np.mean(x)
            y_mean = np.mean(y)

            # Beta = Cov(x, y) / Var(x)
            numerator = np.sum((x - x_mean) * (y - y_mean))
            denominator = np.sum((x - x_mean) ** 2)

            if denominator == 0:
                return 0.0

            return float(numerator / denominator)
        except Exception:
            return 0.0

    def _calculate_spread(
        self,
        prices_a: np.ndarray,
        prices_b: np.ndarray,
        hedge_ratio: float
    ) -> np.ndarray:
        """
        Calculate spread between two price series.

        Spread = prices_a - hedge_ratio * prices_b

        Args:
            prices_a: Prices of symbol A
            prices_b: Prices of symbol B
            hedge_ratio: OLS hedge ratio

        Returns:
            Spread series
        """
        return prices_a - hedge_ratio * prices_b

    def _calculate_zscore(self, series: np.ndarray, window: int = 20) -> float:
        """
        Calculate z-score of the most recent value.

        Z = (x - mean) / std

        Args:
            series: Data series
            window: Lookback window for mean/std

        Returns:
            Z-score of the last value
        """
        if len(series) < window:
            window = len(series)

        if window < 2:
            return 0.0

        recent = series[-window:]
        mean = np.mean(recent)
        std = np.std(recent)

        if std == 0:
            return 0.0

        return float((series[-1] - mean) / std)

    def _calculate_correlation(self, x: np.ndarray, y: np.ndarray, window: int = 60) -> float:
        """
        Calculate rolling correlation between two series.

        Args:
            x: First series
            y: Second series
            window: Lookback window

        Returns:
            Correlation coefficient
        """
        min_len = min(len(x), len(y))
        if min_len < 2:
            return 0.0

        window = min(window, min_len)
        x_recent = x[-window:]
        y_recent = y[-window:]

        try:
            corr = np.corrcoef(x_recent, y_recent)[0, 1]
            return float(corr) if not np.isnan(corr) else 0.0
        except Exception:
            return 0.0

    def _adf_test(self, series: np.ndarray) -> Optional[tuple]:
        """
        Perform Augmented Dickey-Fuller test for stationarity.

        Args:
            series: Data series to test

        Returns:
            Tuple of (adf_statistic, p_value, is_stationary) or None
        """
        try:
            from statsmodels.tsa.stattools import adfuller

            result = adfuller(series, autolag='AIC')
            adf_stat = float(result[0])
            p_value = float(result[1])
            is_stationary = p_value < 0.05  # 5% significance level

            return (adf_stat, p_value, is_stationary)
        except ImportError:
            logger.warning("statsmodels not available for ADF test")
            return None
        except Exception as e:
            logger.debug(f"ADF test failed: {e}")
            return None

    # ==================== Publishing ====================

    async def _publish_analytics(self, key: str, snapshot: AnalyticsSnapshot) -> None:
        """Publish analytics snapshot to Redis hash."""
        try:
            redis_key = RedisKeys.analytics_state(key)
            await self.redis.hash_set(redis_key, snapshot.to_redis_dict())
        except Exception as e:
            logger.error(f"Failed to publish analytics for {key}: {e}")

    async def _check_alerts(self, snapshot: AnalyticsSnapshot) -> None:
        """Check for alert conditions and store/publish alerts."""
        alerts = []

        # Z-score alerts
        if snapshot.z_score is not None:
            if snapshot.z_score > self.alert_z_threshold:
                alerts.append(Alert(
                    id=str(uuid.uuid4()),
                    alert_type=AlertType.Z_SCORE_HIGH,
                    symbol=f"{snapshot.symbol}:{snapshot.pair_symbol}" if snapshot.pair_symbol else snapshot.symbol,
                    message=f"Z-score above threshold: {snapshot.z_score:.2f} > {self.alert_z_threshold}",
                    timestamp=snapshot.timestamp,
                    severity=AlertSeverity.WARNING,
                    value=snapshot.z_score,
                    threshold=self.alert_z_threshold
                ))
            elif snapshot.z_score < -self.alert_z_threshold:
                alerts.append(Alert(
                    id=str(uuid.uuid4()),
                    alert_type=AlertType.Z_SCORE_LOW,
                    symbol=f"{snapshot.symbol}:{snapshot.pair_symbol}" if snapshot.pair_symbol else snapshot.symbol,
                    message=f"Z-score below threshold: {snapshot.z_score:.2f} < -{self.alert_z_threshold}",
                    timestamp=snapshot.timestamp,
                    severity=AlertSeverity.WARNING,
                    value=snapshot.z_score,
                    threshold=-self.alert_z_threshold
                ))

        # Data staleness alert
        # if snapshot.data_freshness_ms > 10000:  # 5 seconds stale
        #     alerts.append(Alert(
        #         id=str(uuid.uuid4()),
        #         alert_type=AlertType.DATA_STALE,
        #         symbol=snapshot.symbol,
        #         message=f"Data stale: {snapshot.data_freshness_ms}ms since last tick",
        #         timestamp=snapshot.timestamp,
        #         severity=AlertSeverity.WARNING,
        #         value=float(snapshot.data_freshness_ms),
        #         threshold=5000.0
        #     ))

        # Store alerts in Redis (hot storage) and publish to channel
        for alert in alerts:
            try:
                alert_dict = alert.to_redis_dict()

                # Store in Redis hot storage (with 24h TTL)
                await self.redis.add_alert(alert_dict, ttl_hours=24)

                # Also publish to channel for real-time subscribers
                await self.redis.publish(RedisKeys.CHANNEL_ALERTS, alert_dict)

                logger.info(f"Alert: {alert.message}")
            except Exception as e:
                logger.error(f"Failed to store/publish alert: {e}")

    def _log_callback(self, log_entry: dict) -> None:
        """Callback for Redis client logging."""
        if log_entry.get("operation") in ["connect", "disconnect"]:
            logger.info(f"Redis {log_entry['operation']}: {log_entry['message']}")


async def run_quant_engine(
    symbols: Optional[List[str]] = None,
    window_size: int = 100
) -> None:
    """
    Entry point to run QuantEngine as a standalone service.

    Args:
        symbols: Optional list of symbols
        window_size: Rolling window size
    """
    engine = QuantEngine(symbols, window_size)

    try:
        await engine.start()
    except KeyboardInterrupt:
        pass
    finally:
        await engine.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    asyncio.run(run_quant_engine())
