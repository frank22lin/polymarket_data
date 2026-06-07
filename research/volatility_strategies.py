from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist
from typing import Protocol

import pandas as pd

from backtester.datamodel import Cancel, Order
from research.strategy import MarketContext, Strategy

_N = NormalDist()
_SECONDS_PER_YEAR = 365.25 * 24 * 3600.0
_MINUTES_PER_YEAR = _SECONDS_PER_YEAR / 60.0


def binary_call_probability(
    spot: float,
    strike: float,
    sigma_annual: float,
    seconds_to_expiry: float,
) -> float:
    """Lognormal probability of finishing above strike."""
    if spot <= 0 or strike <= 0:
        return 0.5
    if seconds_to_expiry <= 0:
        return 1.0 if spot >= strike else 0.0

    years = seconds_to_expiry / _SECONDS_PER_YEAR
    sigma_root_t = max(0.0, sigma_annual) * math.sqrt(years)
    if sigma_root_t <= 1e-12:
        return 1.0 if spot >= strike else 0.0

    d2 = (math.log(spot / strike) - 0.5 * sigma_root_t * sigma_root_t) / sigma_root_t
    return _N.cdf(d2)


@dataclass
class VolSnapshot:
    sigma_annual: float
    jump_score: float = 0.0


class VolatilityEstimator(Protocol):
    def estimate(self, ts_ms: int) -> VolSnapshot: ...


class BTCMinuteFeatureFeed:
    """BTC minute OHLC helper with interpolation and realized-vol estimates."""

    def __init__(self, btc_minutes: pd.DataFrame) -> None:
        if btc_minutes.empty:
            raise ValueError("btc_minutes is empty")
        self.df = btc_minutes.copy()
        self.df["start"] = pd.to_datetime(self.df["start"], utc=True)
        self.df = self.df.sort_values("start").reset_index(drop=True)
        self.ts_ms = self.df["start"].astype("int64").to_numpy() // 1_000_000

    def price_at(self, query_ms: int) -> float:
        opens = self.df["open"].astype(float).to_numpy()
        closes = self.df["close"].astype(float).to_numpy()

        if query_ms <= self.ts_ms[0]:
            return float(opens[0])
        if query_ms >= self.ts_ms[-1] + 60_000:
            return float(closes[-1])

        for i in range(len(self.ts_ms) - 1):
            if self.ts_ms[i] <= query_ms < self.ts_ms[i + 1]:
                frac = (query_ms - self.ts_ms[i]) / (self.ts_ms[i + 1] - self.ts_ms[i])
                return float(opens[i] + frac * (opens[i + 1] - opens[i]))

        i = len(self.ts_ms) - 1
        frac = max(0.0, min(1.0, (query_ms - self.ts_ms[i]) / 60_000))
        return float(opens[i] + frac * (closes[i] - opens[i]))

    def completed_rows(self, ts_ms: int) -> pd.DataFrame:
        return self.df[self.ts_ms < ts_ms].copy()

    def log_returns(self, ts_ms: int, lookback: int) -> pd.Series:
        rows = self.completed_rows(ts_ms).tail(lookback + 1)
        if len(rows) < 2:
            return pd.Series(dtype=float)
        close = rows["close"].astype(float)
        return (close / close.shift(1)).apply(math.log).dropna()

    def parkinson_sigma(self, ts_ms: int, lookback: int) -> float | None:
        rows = self.completed_rows(ts_ms).tail(lookback)
        if rows.empty:
            return None
        high = rows["high"].astype(float)
        low = rows["low"].astype(float)
        ranges = (high / low).apply(math.log) ** 2
        minute_var = float(ranges.mean()) / (4.0 * math.log(2.0))
        return math.sqrt(max(0.0, minute_var) * _MINUTES_PER_YEAR)


class EWMAVolatility:
    def __init__(
        self,
        feed: BTCMinuteFeatureFeed,
        *,
        lookback: int = 20,
        half_life: float = 5.0,
        fallback_sigma: float = 0.60,
        min_sigma: float = 0.20,
        max_sigma: float = 3.00,
    ) -> None:
        self.feed = feed
        self.lookback = lookback
        self.half_life = half_life
        self.fallback_sigma = fallback_sigma
        self.min_sigma = min_sigma
        self.max_sigma = max_sigma

    def estimate(self, ts_ms: int) -> VolSnapshot:
        returns = self.feed.log_returns(ts_ms, self.lookback)
        if returns.empty:
            return VolSnapshot(self.fallback_sigma, 0.0)

        alpha = 1.0 - math.exp(math.log(0.5) / max(self.half_life, 1e-9))
        weights = [(1.0 - alpha) ** i for i in range(len(returns) - 1, -1, -1)]
        weight_sum = sum(weights)
        ewma_var = sum(w * float(r) ** 2 for w, r in zip(weights, returns)) / weight_sum
        sigma = math.sqrt(max(0.0, ewma_var) * _MINUTES_PER_YEAR)

        realized_std = float(returns.std()) if len(returns) > 1 else 0.0
        jump_score = abs(float(returns.iloc[-1])) / realized_std if realized_std > 0 else 0.0
        return VolSnapshot(
            sigma_annual=max(self.min_sigma, min(self.max_sigma, sigma)),
            jump_score=jump_score,
        )


class HybridRangeEWMAVolatility:
    def __init__(
        self,
        feed: BTCMinuteFeatureFeed,
        *,
        lookback: int = 20,
        half_life: float = 5.0,
        fallback_sigma: float = 0.60,
        min_sigma: float = 0.20,
        max_sigma: float = 3.00,
    ) -> None:
        self.feed = feed
        self.ewma = EWMAVolatility(
            feed,
            lookback=lookback,
            half_life=half_life,
            fallback_sigma=fallback_sigma,
            min_sigma=min_sigma,
            max_sigma=max_sigma,
        )
        self.lookback = lookback
        self.fallback_sigma = fallback_sigma
        self.min_sigma = min_sigma
        self.max_sigma = max_sigma

    def estimate(self, ts_ms: int) -> VolSnapshot:
        ewma = self.ewma.estimate(ts_ms)
        range_sigma = self.feed.parkinson_sigma(ts_ms, self.lookback)
        sigma = max(ewma.sigma_annual, range_sigma or self.fallback_sigma)
        return VolSnapshot(
            sigma_annual=max(self.min_sigma, min(self.max_sigma, sigma)),
            jump_score=ewma.jump_score,
        )


class FixedVolatility:
    def __init__(self, sigma_annual: float) -> None:
        self.sigma_annual = sigma_annual

    def estimate(self, ts_ms: int) -> VolSnapshot:
        return VolSnapshot(self.sigma_annual, 0.0)


class VolatilityBinaryMarketMaker(Strategy):
    """Market maker for BTC Up/Down binaries using live vol estimates."""

    tick_interval = "1s"

    def __init__(
        self,
        *,
        btc_feed: BTCMinuteFeatureFeed,
        vol_estimator: VolatilityEstimator,
        strike_ms: int,
        half_spread: float = 0.025,
        quote_size: float = 25.0,
        max_inventory: float = 250.0,
        requote_threshold: float = 0.005,
        skew_per_share: float = 0.00005,
        jump_widen_threshold: float = 2.5,
        jump_widen_multiplier: float = 2.0,
    ) -> None:
        super().__init__()
        self.btc_feed = btc_feed
        self.vol_estimator = vol_estimator
        self.strike_ms = strike_ms
        self.half_spread = half_spread
        self.quote_size = quote_size
        self.max_inventory = max_inventory
        self.requote_threshold = requote_threshold
        self.skew_per_share = skew_per_share
        self.jump_widen_threshold = jump_widen_threshold
        self.jump_widen_multiplier = jump_widen_multiplier
        # per-run ephemeral state
        self.strike: float | None = None
        self.last_center: float | None = None

    def reset(self) -> None:
        super().reset()
        self.strike = None
        self.last_center = None

    def observe(self, ts) -> dict:
        return {"btc_price": self.btc_feed.price_at(int(ts.timestamp() * 1000))}

    def _fair_value(self, ctx: MarketContext) -> tuple[float, VolSnapshot] | None:
        if ctx.timestamp < self.strike_ms:
            return None
        if self.strike is None:
            self.strike = self.btc_feed.price_at(self.strike_ms)
        spot = ctx.observations["btc_price"]
        vol = self.vol_estimator.estimate(ctx.timestamp)
        fair = binary_call_probability(
            spot=spot,
            strike=self.strike,
            sigma_annual=vol.sigma_annual,
            seconds_to_expiry=ctx.seconds_to_expiry,
        )
        return fair, vol

    def on_tick(self, ctx: MarketContext) -> list[Order | Cancel]:
        value = self._fair_value(ctx)
        if value is None:
            return []
        fair, vol = value

        center = max(0.01, min(0.99, fair - self.skew_per_share * ctx.position))
        if (
            self.last_center is not None
            and abs(center - self.last_center) < self.requote_threshold
            and ctx.open_orders
        ):
            return []
        self.last_center = center

        spread = self.half_spread
        if vol.jump_score >= self.jump_widen_threshold:
            spread *= self.jump_widen_multiplier

        bid = max(0.01, center - spread)
        ask = min(0.99, center + spread)

        instructions: list = ctx.cancel_all()
        if ctx.position < self.max_inventory:
            instructions.append(Order(symbol=ctx.symbol, price=bid, quantity=self.quote_size))
        if ctx.position > 0:
            instructions.append(Order(symbol=ctx.symbol, price=ask, quantity=-min(self.quote_size, ctx.position)))

        return instructions

    def params(self) -> dict:
        return {
            **super().params(),
            "strike_ms": self.strike_ms,
            "half_spread": self.half_spread,
            "quote_size": self.quote_size,
            "max_inventory": self.max_inventory,
            "requote_threshold": self.requote_threshold,
            "skew_per_share": self.skew_per_share,
            "jump_widen_threshold": self.jump_widen_threshold,
            "jump_widen_multiplier": self.jump_widen_multiplier,
        }
