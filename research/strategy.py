from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Union

import pandas as pd

from backtester.datamodel import (
    Bar,
    Cancel,
    Listing,
    OpenOrder,
    Order,
    Symbol,
    Time,
    Trade,
    TradingState,
)
from backtester.engine import BacktestEngine
from backtester.metrics import BacktestResult


@dataclass
class MarketContext:
    """Flattened per-symbol view of TradingState. Passed to Strategy.on_tick()."""

    timestamp: Time
    symbol: Symbol
    listing: Listing
    position: float
    cash: float
    last_price: float
    bar: Bar                   # synthetic OHLCV from market trades since last tick; always set
    own_trades: List[Trade]
    market_trades: List[Trade]
    open_orders: List[OpenOrder]
    observations: Dict[str, float]

    @property
    def seconds_to_expiry(self) -> float:
        return max(0.0, (self.listing.resolution_time - self.timestamp) / 1000.0)

    def cancel_all(self) -> List[Cancel]:
        """Return Cancel instructions for every open order on this symbol."""
        return [Cancel(o.order_id) for o in self.open_orders]

    @classmethod
    def from_state(cls, state: TradingState, symbol: Symbol) -> "MarketContext":
        return cls(
            timestamp=state.timestamp,
            symbol=symbol,
            listing=state.listings[symbol],
            position=state.position.get(symbol, 0.0),
            cash=state.cash,
            last_price=state.last_price.get(symbol, math.nan),
            bar=state.bars[symbol],            # always populated for active symbols
            own_trades=state.own_trades.get(symbol, []),
            market_trades=state.market_trades.get(symbol, []),
            open_orders=state.open_orders.get(symbol, []),
            observations=state.observations,
        )


class Strategy(ABC):
    """Abstract base for research strategies.

    Subclasses implement on_tick() and set class-level config defaults.
    Use .backtest() for a full engine run; .params() for artifact logging.

    Lifecycle per run:
        reset() → [on_market_open() × N] → [on_tick() × T]

    Subclass pattern::

        class MyStrategy(Strategy):
            tick_interval = "1s"

            def __init__(self, half_spread: float = 0.02):
                super().__init__()
                self.half_spread = half_spread

            def on_tick(self, ctx: MarketContext) -> list[Order | Cancel]:
                ...

            def reset(self) -> None:
                super().reset()
                self._last_fair: float | None = None

            def params(self) -> dict:
                return {**super().params(), "half_spread": self.half_spread}
    """

    # ── Backtest config — override at class or instance level ──────────────
    cadence:          str   = "clock"
    tick_interval:    str   = "1s"
    initial_cash:     float = 1000.0
    taker_fee_bps:    float = 0.0
    maker_rebate_bps: float = 0.0
    position_limit:   float = float("inf")

    def __init__(self) -> None:
        self._seen_symbols: set[Symbol] = set()

    # ── Implement this ─────────────────────────────────────────────────────

    @abstractmethod
    def on_tick(self, ctx: MarketContext) -> list[Order | Cancel]:
        """Return orders/cancels for this symbol at this timestamp."""
        ...

    # ── Optional hooks ─────────────────────────────────────────────────────

    def on_market_open(self, listing: Listing) -> None:
        """Called once the first time each symbol appears in the engine."""

    def observe(self, ts: pd.Timestamp) -> Dict[str, float]:
        """Return external observations injected into ctx.observations each tick.

        Override to expose data (e.g. BTC spot) without passing a feed as a
        constructor arg. Reads cleanly from ctx.observations["btc_price"] etc.

        Example::

            def observe(self, ts):
                return {"btc_price": self.btc_feed.price_at(int(ts.timestamp() * 1000))}
        """
        return {}

    def reset(self) -> None:
        """Clear per-run state. Override in subclasses; always call super().reset()."""
        self._seen_symbols = set()

    # ── Engine integration (override only for portfolio-level logic) ───────

    def run(self, state: TradingState) -> tuple[dict, str]:
        """BacktestEngine interface. Dispatches on_tick() per active symbol."""
        for symbol, listing in state.listings.items():
            if symbol not in self._seen_symbols:
                self._seen_symbols.add(symbol)
                self.on_market_open(listing)

        results: dict[Symbol, list] = {}
        for symbol in state.listings:
            results[symbol] = self.on_tick(MarketContext.from_state(state, symbol))
        return results, ""

    # ── Research convenience ───────────────────────────────────────────────

    def backtest(
        self,
        listings: Union[Dict[Symbol, Listing], Listing],
        trades,
        resolutions,
        *,
        bars=None,
        observations_fn: Optional[Callable] = None,
        **engine_kwargs,
    ) -> BacktestResult:
        """Run a full backtest. Calls reset() first for a clean slate.

        observations_fn overrides self.observe() when provided explicitly.
        """
        self.reset()
        return BacktestEngine(
            listings=listings,
            trades=trades,
            resolutions=resolutions,
            trader=self,
            bars=bars,
            initial_cash=self.initial_cash,
            cadence=self.cadence,
            tick_interval=self.tick_interval,
            taker_fee_bps=self.taker_fee_bps,
            maker_rebate_bps=self.maker_rebate_bps,
            position_limit=self.position_limit,
            observations_fn=observations_fn if observations_fn is not None else self.observe,
            **engine_kwargs,
        ).run()

    def params(self) -> dict:
        """Serializable parameter dict for config.json artifacts."""
        return {
            "strategy": type(self).__name__,
            "cadence": self.cadence,
            "tick_interval": self.tick_interval,
            "initial_cash": self.initial_cash,
            "taker_fee_bps": self.taker_fee_bps,
            "maker_rebate_bps": self.maker_rebate_bps,
            "position_limit": self.position_limit,
        }
