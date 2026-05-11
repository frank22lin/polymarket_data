from __future__ import annotations

import math
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Tuple, Union

import pandas as pd

from .datamodel import (
    Bar,
    Cancel,
    Listing,
    OpenOrder,
    Order,
    OrderId,
    Position,
    Symbol,
    Time,
    Trade,
    TradingState,
)
from .fill_model import is_marketable, match_trade_to_order
from .metrics import BacktestResult


# --- Event kinds --------------------------------------------------------------
# Encoded as (ts_ms, kind_rank, symbol_or_empty, payload).
# kind_rank determines tie-breaking when timestamps are equal:
#   0 = trade event (must process before tick at same ts so fills land first)
#   1 = clock tick (trader decisions affect future trades only)
#   2 = settlement (position is closed; no further activity)
_KIND_TRADE = 0
_KIND_TICK = 1
_KIND_SETTLE = 2


@dataclass
class _Lot:
    """One FIFO lot for realized-PnL accounting."""
    qty: float            # remaining size
    cost_per_unit: float  # entry price + pro-rated entry fee
    ts: int               # entry timestamp (ms)


def _ts_ms(ts: pd.Timestamp) -> int:
    return int(ts.timestamp() * 1000)


def _to_dict(value, default_key: Optional[str] = None) -> dict:
    """Accept either a dict or a single value; wrap singletons under default_key."""
    if isinstance(value, dict):
        return value
    if default_key is None:
        raise TypeError("expected a dict; got a bare value with no default key")
    return {default_key: value}


def _normalize_trades(df: pd.DataFrame, symbol: Symbol) -> List[Tuple[int, str, float, float, str]]:
    """Return list of (ts_ms, symbol, price, size, side) sorted by timestamp."""
    if df is None or len(df) == 0:
        return []
    if "timestamp" in df.columns:
        ts_col = pd.to_datetime(df["timestamp"], utc=True)
    else:
        ts_col = df.index
        if not isinstance(ts_col, pd.DatetimeIndex):
            raise ValueError("trades DataFrame must have 'timestamp' column or DatetimeIndex")
        ts_col = ts_col.tz_convert("UTC") if ts_col.tz is not None else ts_col.tz_localize("UTC")

    out: List[Tuple[int, str, float, float, str]] = []
    for ts, p, s, side in zip(ts_col, df["price"], df["size"], df["side"]):
        out.append((int(ts.timestamp() * 1000), symbol, float(p), float(s), str(side).upper()))
    out.sort(key=lambda r: r[0])
    return out


def _bar_from_trades(symbol: Symbol, ts_ms: int, trades: List[Trade], last_price: float) -> Bar:
    """Build a synthetic OHLCV bar from a list of own/market trades since last tick."""
    if not trades:
        nan = float("nan")
        return Bar(symbol=symbol, timestamp=ts_ms,
                   open=nan, high=nan, low=nan, close=nan,
                   vwap=nan if math.isnan(last_price) else last_price,
                   volume=0.0, trade_count=0)
    prices = [t.price for t in trades]
    sizes = [abs(t.quantity) for t in trades]
    dollar_volume = sum(p * s for p, s in zip(prices, sizes))
    total_size = sum(sizes)
    return Bar(
        symbol=symbol,
        timestamp=ts_ms,
        open=prices[0],
        high=max(prices),
        low=min(prices),
        close=prices[-1],
        vwap=(dollar_volume / total_size) if total_size > 0 else prices[-1],
        volume=total_size,
        trade_count=len(trades),
    )


class BacktestEngine:
    """
    Trade-driven, multi-market backtester for Polymarket.

    Parameters
    ----------
    listings : Dict[Symbol, Listing] | Listing
        Market metadata, keyed by symbol. A bare ``Listing`` is wrapped under
        ``listing.symbol`` for convenience.
    trades : Dict[Symbol, pd.DataFrame] | pd.DataFrame
        Output of ``PolymarketData.fetch_trades(...)`` for each market. Required
        columns: ``timestamp`` (UTC), ``price``, ``size``, ``side`` ('BUY' or
        'SELL', representing the taker side).
    resolutions : Dict[Symbol, float] | float
        Settlement values per market (1.0 = YES, 0.0 = NO).
    trader
        Object implementing ``run(state) -> (dict, str)`` where the dict maps
        symbol to a list of ``Order`` and/or ``Cancel`` instructions.
    bars : Dict[Symbol, pd.DataFrame], optional
        Pre-computed OHLCV bars for plotting/result context. Engine doesn't
        use them for fills.
    initial_cash : float
        Starting capital. Default 1000.
    cadence : {"clock", "trade"}
        ``clock`` invokes the trader on a regular tick (see ``tick_interval``);
        ``trade`` invokes the trader after every market trade.
    tick_interval : str
        Pandas offset alias (e.g. ``"5min"``) — required when ``cadence="clock"``.
    taker_fee_bps : float
        Fee on notional for taker (marketable) fills, in basis points.
    maker_rebate_bps : float
        Rebate on notional for resting limit fills (negative cost), in bps.
    position_limit : float
        Max absolute share position per symbol. Default unlimited.
    observations_fn : callable, optional
        ``fn(timestamp: pd.Timestamp) -> dict[str, float]`` — inject external
        data (e.g. BTC spot) into each TradingState.
    """

    def __init__(
        self,
        listings: Union[Dict[Symbol, Listing], Listing],
        trades: Union[Dict[Symbol, pd.DataFrame], pd.DataFrame],
        resolutions: Union[Dict[Symbol, float], float],
        trader,
        bars: Optional[Dict[Symbol, pd.DataFrame]] = None,
        initial_cash: float = 1000.0,
        cadence: str = "clock",
        tick_interval: Optional[str] = None,
        taker_fee_bps: float = 0.0,
        maker_rebate_bps: float = 0.0,
        position_limit: float = float("inf"),
        observations_fn: Optional[Callable] = None,
        # Backwards-compat for single-market callers; ignored when listings is a dict.
        symbol: Optional[Symbol] = None,
    ):
        # Normalize inputs to dicts
        if isinstance(listings, Listing):
            inferred = listings.symbol or symbol
            if not inferred:
                raise ValueError("Listing has no symbol and no `symbol` kwarg supplied")
            listings = {inferred: listings}
        if not isinstance(trades, dict):
            if len(listings) != 1:
                raise ValueError("trades must be a dict when listings has multiple symbols")
            (only_symbol,) = listings.keys()
            trades = {only_symbol: trades}
        if not isinstance(resolutions, dict):
            if len(listings) != 1:
                raise ValueError("resolutions must be a dict when listings has multiple symbols")
            (only_symbol,) = listings.keys()
            resolutions = {only_symbol: float(resolutions)}

        if cadence not in ("clock", "trade"):
            raise ValueError(f"cadence must be 'clock' or 'trade', got {cadence!r}")
        if cadence == "clock" and tick_interval is None:
            raise ValueError("tick_interval is required when cadence='clock'")

        self.listings = listings
        self.trades_dfs = trades
        self.resolutions = {s: float(v) for s, v in resolutions.items()}
        self.trader = trader
        self.bars_dfs = bars or {}
        self.initial_cash = float(initial_cash)
        self.cadence = cadence
        self.tick_interval = tick_interval
        self.taker_fee_bps = float(taker_fee_bps)
        self.maker_rebate_bps = float(maker_rebate_bps)
        self.position_limit = float(position_limit)
        self.observations_fn = observations_fn or (lambda ts: {})

    # ------------------------------------------------------------------
    # Event stream construction
    # ------------------------------------------------------------------

    def _build_events(self) -> List[tuple]:
        """Merge trade events, clock ticks, and settlement events into one sorted stream."""
        events: List[tuple] = []
        for symbol, df in self.trades_dfs.items():
            for ts_ms, sym, price, size, side in _normalize_trades(df, symbol):
                events.append((ts_ms, _KIND_TRADE, sym, (price, size, side)))

        for symbol, listing in self.listings.items():
            res_ts = int(listing.resolution_time)
            events.append((res_ts, _KIND_SETTLE, symbol, None))

        if self.cadence == "clock":
            ts_min, ts_max = self._timeline_bounds()
            if ts_min is not None and ts_max is not None:
                start = pd.Timestamp(ts_min, unit="ms", tz="UTC").floor(self.tick_interval)
                end = pd.Timestamp(ts_max, unit="ms", tz="UTC")
                ticks = pd.date_range(start=start, end=end, freq=self.tick_interval, inclusive="both", tz="UTC")
                for ts in ticks:
                    events.append((_ts_ms(ts), _KIND_TICK, "", None))

        events.sort(key=lambda e: (e[0], e[1]))
        return events

    def _timeline_bounds(self) -> Tuple[Optional[int], Optional[int]]:
        ts_min: Optional[int] = None
        ts_max: Optional[int] = None
        for symbol, df in self.trades_dfs.items():
            if df is None or len(df) == 0:
                continue
            if "timestamp" in df.columns:
                col = pd.to_datetime(df["timestamp"], utc=True)
                a, b = int(col.min().timestamp() * 1000), int(col.max().timestamp() * 1000)
            else:
                idx = df.index
                a, b = int(idx.min().timestamp() * 1000), int(idx.max().timestamp() * 1000)
            ts_min = a if ts_min is None else min(ts_min, a)
            ts_max = b if ts_max is None else max(ts_max, b)
        for listing in self.listings.values():
            r = int(listing.resolution_time)
            ts_min = r if ts_min is None else min(ts_min, r)
            ts_max = r if ts_max is None else max(ts_max, r)
        return ts_min, ts_max

    # ------------------------------------------------------------------
    # Core run loop
    # ------------------------------------------------------------------

    def run(self) -> BacktestResult:
        active: Dict[Symbol, bool] = {s: True for s in self.listings}
        cash: float = self.initial_cash
        cash_reserved: float = 0.0  # collateral reserved by open buys
        position: Dict[Symbol, Position] = {s: 0.0 for s in self.listings}
        position_reserved: Dict[Symbol, float] = {s: 0.0 for s in self.listings}
        open_orders: Dict[Symbol, List[OpenOrder]] = {s: [] for s in self.listings}
        last_price: Dict[Symbol, float] = {s: float("nan") for s in self.listings}
        all_fills: List[Trade] = []
        equity_curve: "OrderedDict[pd.Timestamp, float]" = OrderedDict()
        total_taker_fees: float = 0.0
        total_maker_rebates: float = 0.0
        next_order_id: OrderId = 1
        trader_data: str = ""

        # Per-symbol PnL accounting
        cash_flow_per_symbol: Dict[Symbol, float] = {s: 0.0 for s in self.listings}
        lots: Dict[Symbol, Deque[_Lot]] = {s: deque() for s in self.listings}
        realized_pnl_per_symbol: Dict[Symbol, float] = {s: 0.0 for s in self.listings}
        equity_per_symbol_curve: Dict[Symbol, "OrderedDict[pd.Timestamp, float]"] = {
            s: OrderedDict() for s in self.listings
        }
        round_trips: List[dict] = []

        # Buffers for trades since the trader's last invocation
        market_trades_buf: Dict[Symbol, List[Trade]] = {s: [] for s in self.listings}
        own_trades_buf: Dict[Symbol, List[Trade]] = {s: [] for s in self.listings}

        events = self._build_events()

        def mark_to_market() -> float:
            equity = cash
            for s in self.listings:
                if active[s] and not math.isnan(last_price[s]):
                    equity += position[s] * last_price[s]
            return equity

        def equity_contribution(s: Symbol) -> float:
            """PnL contribution of symbol s = realized + unrealized.

            cash_flow_per_symbol[s] is cumulative cash flow attributed to s
            (negative on buys, positive on sells, includes fees).
            position[s] * last_price[s] is the MTM value of the open position.
            Their sum equals total PnL contribution from s; settlement folds
            unrealized into realized when position goes to 0.
            """
            value = cash_flow_per_symbol[s]
            if active[s] and not math.isnan(last_price[s]):
                value += position[s] * last_price[s]
            return value

        def apply_buy_to_lots(symbol: Symbol, qty: float, fill_price: float, fee: float, ts_ms: int) -> None:
            """Add a new long lot. fee is signed (positive = paid, negative = rebate received)."""
            cost_per_unit = fill_price + fee / qty if qty > 0 else fill_price
            lots[symbol].append(_Lot(qty=qty, cost_per_unit=cost_per_unit, ts=ts_ms))
            cash_flow_per_symbol[symbol] -= qty * fill_price + fee

        def apply_sell_to_lots(symbol: Symbol, qty: float, fill_price: float, fee: float, ts_ms: int) -> None:
            """Consume FIFO lots, accumulate realized PnL and round-trip records."""
            net_proceeds_per_unit = fill_price - (fee / qty if qty > 0 else 0.0)
            remaining = qty
            book = lots[symbol]
            while remaining > 1e-12 and book:
                lot = book[0]
                slice_qty = min(lot.qty, remaining)
                cost_for_slice = slice_qty * lot.cost_per_unit
                proceeds_for_slice = slice_qty * net_proceeds_per_unit
                pnl = proceeds_for_slice - cost_for_slice
                realized_pnl_per_symbol[symbol] += pnl
                round_trips.append({
                    "symbol": symbol,
                    "entry_ts": lot.ts,
                    "exit_ts": ts_ms,
                    "qty": slice_qty,
                    "entry_price": lot.cost_per_unit,
                    "exit_price": net_proceeds_per_unit,
                    "pnl": pnl,
                })
                lot.qty -= slice_qty
                remaining -= slice_qty
                if lot.qty <= 1e-12:
                    book.popleft()
            cash_flow_per_symbol[symbol] += qty * fill_price - fee

        def cancel_order(symbol: Symbol, order_id: OrderId) -> bool:
            nonlocal cash_reserved
            book = open_orders.get(symbol, [])
            for i, o in enumerate(book):
                if o.order_id == order_id:
                    if o.quantity > 0 and not is_marketable(o.price, is_buy=True):
                        cash_reserved -= o.remaining * o.price
                    elif o.quantity < 0:
                        position_reserved[symbol] -= o.remaining
                    book.pop(i)
                    return True
            return False

        def submit_order(order: Order, ts_ms: int) -> Optional[OpenOrder]:
            """Validate and add to the book. Returns the OpenOrder, or None if rejected.

            Cash policy:
              * Resting limit buys (price < 1.0) reserve qty * price upfront —
                Polymarket actually locks this collateral.
              * Marketable buys (price >= 1.0) reserve nothing; cash is checked
                at fill time so two simultaneous market orders at modest fill
                prices don't falsely trip a worst-case-1.0 ceiling.
              * Sells require a backing long position (no naked shorts).
            """
            nonlocal cash_reserved, next_order_id
            if order.quantity == 0:
                return None
            if not active.get(order.symbol, False):
                return None
            qty = order.quantity
            price = order.price
            reserved_for_this: float = 0.0
            if qty > 0:
                if not is_marketable(price, is_buy=True):
                    needed = qty * price
                    if cash - cash_reserved < needed:
                        return None  # insufficient collateral for resting limit
                    cash_reserved += needed
                    reserved_for_this = needed
            else:
                size = -qty
                avail = position[order.symbol] - position_reserved[order.symbol]
                if avail < size:
                    return None  # would short / oversell
                position_reserved[order.symbol] += size
            new_pos_after = position[order.symbol] + qty
            if abs(new_pos_after) > self.position_limit:
                # Roll back reservation
                if qty > 0:
                    cash_reserved -= reserved_for_this
                else:
                    position_reserved[order.symbol] -= -qty
                return None
            oid = next_order_id
            next_order_id += 1
            oo = OpenOrder(
                order_id=oid,
                symbol=order.symbol,
                price=price,
                quantity=qty,
                submitted_ts=ts_ms,
                remaining=abs(qty),
            )
            open_orders.setdefault(order.symbol, []).append(oo)
            return oo

        def attempt_fills_for_trade(symbol: Symbol, ts_ms: int, trade_price: float, trade_side: str) -> List[Trade]:
            """Match the incoming trade against open orders for the symbol. Returns own fills."""
            nonlocal cash, cash_reserved, total_taker_fees, total_maker_rebates
            fills: List[Trade] = []
            book = open_orders.get(symbol, [])
            i = 0
            while i < len(book):
                o = book[i]
                is_buy = o.quantity > 0
                fill_price: Optional[float] = None
                is_taker: bool = False

                if is_marketable(o.price, is_buy):
                    # Marketable: fills at the next opposing-side trade's price
                    if (is_buy and trade_side == "BUY") or (not is_buy and trade_side == "SELL"):
                        fill_price = trade_price
                        is_taker = True
                else:
                    matched = match_trade_to_order(o, trade_price, trade_side)
                    if matched is not None:
                        fill_price = matched
                        # If filled on the same ms it was submitted, treat as taker (no rebate)
                        is_taker = (ts_ms <= o.submitted_ts)

                if fill_price is None:
                    i += 1
                    continue

                qty_signed = o.quantity  # full fill
                size = abs(qty_signed)
                notional = size * fill_price
                fee_bps = self.taker_fee_bps if is_taker else -self.maker_rebate_bps
                fee = notional * fee_bps / 10000.0

                # Marketable buys didn't reserve cash; check at fill time.
                if is_buy and is_marketable(o.price, is_buy=True):
                    needed = size * fill_price + max(fee, 0.0)
                    if cash - cash_reserved < needed:
                        # Skip this fill — order stays on book to retry on next trade
                        i += 1
                        continue

                # Update cash & position
                cash -= qty_signed * fill_price + fee
                position[symbol] += qty_signed
                if is_taker:
                    total_taker_fees += fee
                else:
                    total_maker_rebates += -fee  # rebate = negative fee

                # FIFO bookkeeping for realized PnL / round trips
                if is_buy:
                    apply_buy_to_lots(symbol, size, fill_price, fee, ts_ms)
                else:
                    apply_sell_to_lots(symbol, size, fill_price, fee, ts_ms)

                # Release reservations (only resting limits reserved cash)
                if is_buy and not is_marketable(o.price, is_buy=True):
                    cash_reserved -= size * o.price
                elif not is_buy:
                    position_reserved[symbol] -= size

                trade = Trade(
                    symbol=symbol,
                    price=fill_price,
                    quantity=qty_signed,
                    buyer="SUBMISSION" if is_buy else "",
                    seller="SUBMISSION" if not is_buy else "",
                    timestamp=ts_ms,
                )
                fills.append(trade)
                all_fills.append(trade)
                book.pop(i)
                # Don't increment i — list shifted
            return fills

        def settle(symbol: Symbol, ts_ms: int) -> None:
            nonlocal cash, cash_reserved
            if not active.get(symbol, False):
                return
            # Cancel any open orders for this symbol
            book = open_orders.get(symbol, [])
            for o in list(book):
                if o.quantity > 0 and not is_marketable(o.price, is_buy=True):
                    cash_reserved -= o.remaining * o.price
                elif o.quantity < 0:
                    position_reserved[symbol] -= o.remaining
            open_orders[symbol] = []
            # Convert position to cash at resolution
            res = self.resolutions.get(symbol, 0.0)
            pos = position[symbol]
            if pos > 1e-12:
                # Force-close the long via the FIFO path so realized PnL captures it
                apply_sell_to_lots(symbol, pos, res, 0.0, ts_ms)
                # cash_flow already updated by apply_sell_to_lots; reflect in actual cash
                cash += pos * res
                position[symbol] = 0.0
            elif pos < -1e-12:
                # Negative position shouldn't happen (we reject naked shorts), but settle cleanly
                cash += pos * res
                position[symbol] = 0.0
            position_reserved[symbol] = 0.0
            last_price[symbol] = res
            active[symbol] = False

        def invoke_trader(ts_ms: int) -> None:
            nonlocal trader_data
            ts_pd = pd.Timestamp(ts_ms, unit="ms", tz="UTC")
            # Build per-symbol synthetic bar from buffered market trades
            bars_by_symbol: Dict[Symbol, Bar] = {}
            for s in self.listings:
                if not active[s]:
                    continue
                bars_by_symbol[s] = _bar_from_trades(s, ts_ms, market_trades_buf[s], last_price[s])
            state = TradingState(
                traderData=trader_data,
                timestamp=ts_ms,
                listings={s: l for s, l in self.listings.items() if active[s]},
                bars=bars_by_symbol,
                own_trades={s: list(own_trades_buf[s]) for s in self.listings if active[s]},
                market_trades={s: list(market_trades_buf[s]) for s in self.listings if active[s]},
                position={s: position[s] for s in self.listings if active[s]},
                open_orders={s: list(open_orders[s]) for s in self.listings if active[s]},
                cash=cash,
                observations=self.observations_fn(ts_pd),
                last_price={s: last_price[s] for s in self.listings if active[s]},
            )
            result, trader_data = self.trader.run(state)

            # Process trader's instructions
            for symbol, items in (result or {}).items():
                for item in items:
                    if isinstance(item, Cancel):
                        cancel_order(symbol, item.order_id)
                    elif isinstance(item, Order):
                        submit_order(item, ts_ms)
                    else:
                        raise TypeError(f"Trader returned unsupported item: {item!r}")

            # Reset since-last-tick buffers
            for s in self.listings:
                market_trades_buf[s].clear()
                own_trades_buf[s].clear()

            equity_curve[ts_pd] = mark_to_market()
            for s in self.listings:
                equity_per_symbol_curve[s][ts_pd] = equity_contribution(s)

        # --- Main event loop -----------------------------------------------
        for ts_ms, kind, symbol, payload in events:
            if kind == _KIND_TRADE:
                if not active.get(symbol, False):
                    continue
                trade_price, trade_size, trade_side = payload
                # Record market trade for trader visibility
                signed_qty = trade_size if trade_side == "BUY" else -trade_size
                market_trades_buf[symbol].append(Trade(
                    symbol=symbol,
                    price=trade_price,
                    quantity=signed_qty,
                    timestamp=ts_ms,
                ))
                last_price[symbol] = trade_price
                # Try to fill our resting orders against this trade
                new_fills = attempt_fills_for_trade(symbol, ts_ms, trade_price, trade_side)
                own_trades_buf[symbol].extend(new_fills)

                if self.cadence == "trade":
                    invoke_trader(ts_ms)

            elif kind == _KIND_TICK:
                # Only invoke if at least one market is still active
                if any(active.values()):
                    invoke_trader(ts_ms)

            elif kind == _KIND_SETTLE:
                settle(symbol, ts_ms)
                ts_pd = pd.Timestamp(ts_ms, unit="ms", tz="UTC")
                equity_curve[ts_pd] = mark_to_market()
                for s in self.listings:
                    equity_per_symbol_curve[s][ts_pd] = equity_contribution(s)

        # Final safety net: if any market never received its settle event
        # (shouldn't happen — settle events are always emitted), force settle.
        for s in list(active.keys()):
            if active[s]:
                settle(s, int(self.listings[s].resolution_time))

        # Build result
        if equity_curve:
            equity_series = pd.Series(equity_curve, name="equity")
        else:
            equity_series = pd.Series(dtype=float, name="equity")
        equity_series.index.name = "timestamp"

        equity_by_symbol: Dict[Symbol, pd.Series] = {}
        for s, curve in equity_per_symbol_curve.items():
            ser = pd.Series(curve, name=f"equity_{s}") if curve else pd.Series(dtype=float, name=f"equity_{s}")
            ser.index.name = "timestamp"
            equity_by_symbol[s] = ser

        # Bars for the result keyed by symbol (or single dataframe if only one)
        bars_for_result: Optional[pd.DataFrame] = None
        if len(self.listings) == 1:
            (only,) = self.listings.keys()
            bars_for_result = self.bars_dfs.get(only)

        run_config = {
            "symbols": list(self.listings.keys()),
            "initial_cash": self.initial_cash,
            "cadence": self.cadence,
            "tick_interval": self.tick_interval,
            "taker_fee_bps": self.taker_fee_bps,
            "maker_rebate_bps": self.maker_rebate_bps,
            "position_limit": self.position_limit,
            "resolutions": dict(self.resolutions),
        }

        return BacktestResult(
            symbol=next(iter(self.listings)) if len(self.listings) == 1 else ",".join(self.listings),
            bars=bars_for_result,
            bars_by_symbol=dict(self.bars_dfs),
            fills=all_fills,
            resolution=next(iter(self.resolutions.values())) if len(self.resolutions) == 1 else float("nan"),
            resolutions=dict(self.resolutions),
            equity_curve=equity_series,
            equity_by_symbol=equity_by_symbol,
            realized_pnl_by_symbol=dict(realized_pnl_per_symbol),
            round_trips=round_trips,
            initial_cash=self.initial_cash,
            total_taker_fees=total_taker_fees,
            total_maker_rebates=total_maker_rebates,
            run_config=run_config,
        )
