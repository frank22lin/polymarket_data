from __future__ import annotations

import math
from typing import Callable, Dict, List, Optional

import pandas as pd

from .datamodel import Bar, Listing, Order, Symbol, Trade, TradingState
from .fill_model import FillModel, NextBarOpen
from .metrics import BacktestResult


def _ts_ms(ts) -> int:
    """Convert a pandas Timestamp to unix milliseconds."""
    return int(ts.timestamp() * 1000)


def _row_to_bar(symbol: Symbol, ts_ms: int, row) -> Bar:
    def _f(v) -> float:
        return float(v) if not (isinstance(v, float) and math.isnan(v)) else float("nan")

    return Bar(
        symbol=symbol,
        timestamp=ts_ms,
        open=_f(row["open"]),
        high=_f(row["high"]),
        low=_f(row["low"]),
        close=_f(row["close"]),
        vwap=_f(row["vwap"]),
        volume=float(row["volume"]),
        trade_count=int(row["trade_count"]),
    )


class BacktestEngine:
    """
    Vectorized bar-by-bar backtester for a single Polymarket market.

    Parameters
    ----------
    symbol : str
        Identifier for the market (e.g. the slug).
    listing : Listing
        Market metadata.
    bars : pd.DataFrame
        Output of ``PolymarketData.price_series()`` (pandas backend).
        Index must be a DatetimeTZIndex (UTC); columns: open, high, low,
        close, vwap, volume, trade_count.
    resolution : float
        Settlement value: 1.0 if the market resolved YES, 0.0 if NO.
    trader
        Any object implementing ``run(state: TradingState) -> (dict, str)``.
    fill_model : FillModel, optional
        Defaults to NextBarOpen.
    initial_cash : float
        Starting capital in USD. Default 1000.
    position_limit : float
        Maximum absolute share position. Default unlimited.
    observations_fn : callable, optional
        ``fn(timestamp: pd.Timestamp) -> dict[str, float]`` — inject external
        data (e.g. BTC spot price) into each TradingState.
    """

    def __init__(
        self,
        symbol: str,
        listing: Listing,
        bars: pd.DataFrame,
        resolution: float,
        trader,
        fill_model: Optional[FillModel] = None,
        initial_cash: float = 1000.0,
        position_limit: float = float("inf"),
        observations_fn: Optional[Callable] = None,
    ):
        self.symbol = symbol
        self.listing = listing
        self.bars = bars
        self.resolution = resolution
        self.trader = trader
        self.fill_model = fill_model or NextBarOpen()
        self.initial_cash = initial_cash
        self.position_limit = position_limit
        self.observations_fn = observations_fn or (lambda ts: {})

    def run(self) -> BacktestResult:
        symbol = self.symbol
        bar_rows = list(self.bars.iterrows())

        cash: float = self.initial_cash
        position: float = 0.0
        trader_data: str = ""
        pending_orders: List[Order] = []
        all_fills: List[Trade] = []
        equity_curve: Dict = {}
        last_close: float = float("nan")

        for i, (ts, row) in enumerate(bar_rows):
            ts_ms = _ts_ms(ts)
            current_bar = _row_to_bar(symbol, ts_ms, row)

            # Fill any orders pending from the previous bar
            new_fills: List[Trade] = []
            for order in pending_orders:
                fill = self.fill_model.try_fill(order, current_bar)
                if fill is None:
                    continue
                new_pos = position + fill.quantity
                if abs(new_pos) > self.position_limit:
                    continue
                cash -= fill.quantity * fill.price
                position = new_pos
                new_fills.append(fill)
                all_fills.append(fill)
            pending_orders = []

            # Build and deliver TradingState
            state = TradingState(
                traderData=trader_data,
                timestamp=ts_ms,
                listings={symbol: self.listing},
                bars={symbol: current_bar},
                own_trades={symbol: new_fills},
                market_trades={symbol: []},
                position={symbol: position},
                observations=self.observations_fn(ts),
            )

            result, trader_data = self.trader.run(state)

            for order in result.get(symbol, []):
                pending_orders.append(order)

            # Mark to market: use close if available, else last known close
            if not math.isnan(current_bar.close):
                last_close = current_bar.close
            equity_curve[ts] = cash + position * last_close

        # Settle remaining position at resolution
        cash += position * self.resolution
        if bar_rows:
            equity_curve[bar_rows[-1][0]] = cash  # overwrite last bar with settled value

        equity_series = pd.Series(equity_curve, name="equity")
        equity_series.index.name = "timestamp"

        return BacktestResult(
            symbol=symbol,
            bars=self.bars,
            fills=all_fills,
            resolution=self.resolution,
            equity_curve=equity_series,
            initial_cash=self.initial_cash,
        )
