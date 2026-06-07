"""Black-Scholes binary-call market-making backtest on a 5-min BTC up/down market.

Market: "Bitcoin Up or Down — April 20, 8:40PM-8:45PM ET"
  Strike fixes at 2026-04-21 00:40 UTC, resolves at 00:45 UTC, Down won → Up=0.0.

Strategy:
  Each clock tick, compute BS binary-call fair price for the "Up" outcome with
  current BTC spot, fixed strike (BTC at 00:40), constant annualized sigma, and
  time-to-expiration. Post a two-sided limit quote (bid below fair, ask above)
  with optional inventory-aware skew. Cancel and replace when fair moves more
  than `requote_threshold`.

BTC spot: Coinbase Advanced Trade public market data, BTC-USD 1-min OHLC,
linearly interpolated between minute closes (sub-minute resolution is not
available without authenticated APIs). Coinbase data is cached to a local
CSV after the first fetch.
"""
from __future__ import annotations

import json
import math
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from statistics import NormalDist

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from polymarket_data import PolymarketData
from backtester import BacktestEngine, Cancel, Listing, OpenOrder, Order, TradingState

# ─────────────────────────────────────────────────────────────────────────────
# Market constants
# ─────────────────────────────────────────────────────────────────────────────
EVENT_SLUG = "btc-updown-5m-1776732000"
STRIKE_TIME    = datetime(2026, 4, 21, 0, 40, tzinfo=timezone.utc)   # K fixes here
RESOLUTION_TIME = datetime(2026, 4, 21, 0, 45, tzinfo=timezone.utc)  # market settles here
RESOLUTION_VALUE = 0.0                                               # Down won → Up = 0.0

STRIKE_MS    = int(STRIKE_TIME.timestamp() * 1000)
RES_MS       = int(RESOLUTION_TIME.timestamp() * 1000)
_DATA_DIR    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
_BTC_CACHE   = os.path.join(_DATA_DIR, "btc_usd_coinbase_1m_20260421.csv")

# ─────────────────────────────────────────────────────────────────────────────
# BTC spot loader (Coinbase Advanced Trade public market-data API)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_btc_minutes(start: datetime, end: datetime) -> pd.DataFrame:
    """Return BTC-USD 1-min OHLC over [start, end] from Coinbase, cached to disk."""
    os.makedirs(_DATA_DIR, exist_ok=True)
    if os.path.exists(_BTC_CACHE):
        df = pd.read_csv(_BTC_CACHE)
        df["start"] = pd.to_datetime(df["start"], utc=True)
        return df
    url = "https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD/candles?" + urllib.parse.urlencode({
        "start": int(start.timestamp()),
        "end":   int(end.timestamp()),
        "granularity": "ONE_MINUTE",
    })
    with urllib.request.urlopen(url, timeout=20) as r:
        payload = json.loads(r.read())
    rows = []
    for c in payload.get("candles", []):
        rows.append({
            "start":  datetime.fromtimestamp(int(c["start"]), tz=timezone.utc),
            "open":   float(c["open"]),
            "high":   float(c["high"]),
            "low":    float(c["low"]),
            "close":  float(c["close"]),
            "volume": float(c["volume"]),
        })
    df = pd.DataFrame(rows).sort_values("start").reset_index(drop=True)
    df.to_csv(_BTC_CACHE, index=False)
    return df


def btc_price_function(btc_df: pd.DataFrame):
    """Return a function f(ts_ms) -> BTC mid price, linearly interpolated.

    We interpolate between consecutive minute closes. For a query timestamp
    inside the i-th minute window, we treat (start_i, open_i) and
    (start_{i+1}, open_{i+1}) as anchor points. The minute 'open' is the BTC
    price at the start of that minute, so it's the cleanest discrete sample.
    """
    if btc_df.empty:
        raise RuntimeError("BTC data is empty; cannot build price function.")
    ts_ms = btc_df["start"].astype("int64").to_numpy() // 1_000_000  # ns → ms
    opens = btc_df["open"].to_numpy()
    closes = btc_df["close"].to_numpy()

    def f(query_ms: int) -> float:
        if query_ms <= ts_ms[0]:
            return float(opens[0])
        if query_ms >= ts_ms[-1] + 60_000:
            # past last minute → use that minute's close
            return float(closes[-1])
        # Find the bucket
        for i in range(len(ts_ms) - 1):
            if ts_ms[i] <= query_ms < ts_ms[i + 1]:
                # interpolate between open[i] (at ts_ms[i]) and open[i+1] (at ts_ms[i+1])
                frac = (query_ms - ts_ms[i]) / (ts_ms[i + 1] - ts_ms[i])
                return float(opens[i] + frac * (opens[i + 1] - opens[i]))
        # If we fell off the end, last minute: interpolate open[-1] → close[-1]
        i = len(ts_ms) - 1
        frac = max(0.0, min(1.0, (query_ms - ts_ms[i]) / 60_000))
        return float(opens[i] + frac * (closes[i] - opens[i]))

    return f


# ─────────────────────────────────────────────────────────────────────────────
# Black-Scholes binary call
# ─────────────────────────────────────────────────────────────────────────────
_N = NormalDist()


def bs_binary_call(S: float, K: float, sigma_annual: float, T_seconds: float) -> float:
    """Price of a binary call paying $1 if S_T > K. r = 0."""
    if T_seconds <= 0:
        return 1.0 if S > K else 0.0
    T_years = T_seconds / (365.25 * 24 * 3600.0)
    sigma_root_T = sigma_annual * math.sqrt(T_years)
    if sigma_root_T <= 0:
        return 1.0 if S > K else 0.0
    d2 = (math.log(S / K) - 0.5 * sigma_root_T * sigma_root_T) / sigma_root_T
    return _N.cdf(d2)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy
# ─────────────────────────────────────────────────────────────────────────────
class BSBinaryCallMM:
    """Two-sided market maker around BS binary-call fair value.

    Posts a resting limit bid at `fair - half_spread` and a resting limit ask
    at `fair + half_spread`. The ask is only posted when we have inventory
    (engine rejects naked shorts). The bid is suppressed when position is
    near max_inventory.

    On each tick:
      1. Compute current fair from BS using BTC spot + strike + sigma + (T - t).
      2. If fair has moved more than `requote_threshold` since last quote,
         cancel resting orders and re-post.
      3. Inventory-aware: skew quotes by `skew_per_share * position`.
    """

    def __init__(
        self,
        btc_price_fn,
        strike_ms: int,
        resolution_ms: int,
        sigma_annual: float = 0.6,
        half_spread: float = 0.02,
        quote_size: float = 25.0,
        max_inventory: float = 500.0,
        requote_threshold: float = 0.005,
        skew_per_share: float = 0.0,
    ):
        self.btc_price_fn = btc_price_fn
        self.strike_ms = strike_ms
        self.resolution_ms = resolution_ms
        self.sigma_annual = sigma_annual
        self.half_spread = half_spread
        self.quote_size = quote_size
        self.max_inventory = max_inventory
        self.requote_threshold = requote_threshold
        self.skew_per_share = skew_per_share

        self.strike: float | None = None
        self.last_fair: float | None = None

    def _fair(self, ts_ms: int) -> float | None:
        if self.strike is None:
            if ts_ms < self.strike_ms:
                return None
            self.strike = self.btc_price_fn(self.strike_ms)
        spot = self.btc_price_fn(ts_ms)
        if spot is None or self.strike is None or self.strike <= 0:
            return None
        T_seconds = (self.resolution_ms - ts_ms) / 1000.0
        return bs_binary_call(spot, self.strike, self.sigma_annual, T_seconds)

    def run(self, state: TradingState):
        symbol = next(iter(state.listings))
        position = state.position.get(symbol, 0.0)
        open_book = state.open_orders.get(symbol, [])

        fair = self._fair(state.timestamp)
        if fair is None:
            return {symbol: []}, ""

        # Inventory-aware skew: if long, tilt fair downward (sell more aggressively)
        center = max(0.01, min(0.99, fair - self.skew_per_share * position))

        bid_px = max(0.01, center - self.half_spread)
        ask_px = min(0.99, center + self.half_spread)

        # Skip requote if fair barely moved AND we still have resting quotes
        if (self.last_fair is not None
                and abs(fair - self.last_fair) < self.requote_threshold
                and open_book):
            return {symbol: []}, ""
        self.last_fair = fair

        instructions: list = [Cancel(o.order_id) for o in open_book]

        # Bid: open if we can grow our long
        if position < self.max_inventory:
            instructions.append(Order(symbol=symbol, price=bid_px, quantity=self.quote_size))
        # Ask: open only if we actually have inventory (engine rejects naked shorts)
        if position > 0:
            sell_qty = min(self.quote_size, position)
            instructions.append(Order(symbol=symbol, price=ask_px, quantity=-sell_qty))

        return {symbol: instructions}, ""


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────
def main(
    sigma_annual: float = 0.6,
    half_spread: float = 0.02,
    quote_size: float = 25.0,
    max_inventory: float = 500.0,
    requote_threshold: float = 0.005,
    tick_interval: str = "1s",
    initial_cash: float = 1000.0,
    taker_fee_bps: float = 0.0,
    maker_rebate_bps: float = 0.0,
    show_plot: bool = True,
):
    # ── 1. Polymarket market metadata ────────────────────────────────────────
    pm = PolymarketData()
    markets = pm.get_event(EVENT_SLUG)
    market = markets[0]
    slug = market.slug
    print(f"Market: {market.question}")
    print(f"Outcomes: {market.outcomes}  | trading 'Up' (outcome 0)")

    # ── 2. Polymarket trades for the 5-min decision window ───────────────────
    pad = pd.Timedelta(seconds=0)
    win_start = STRIKE_TIME - pad
    win_end   = RESOLUTION_TIME + pd.Timedelta(seconds=30)

    print(f"\nFetching Polymarket trades {win_start} .. {win_end} ...")
    trades_df = pm.fetch_trades(
        slug=slug, start_time=win_start, end_time=win_end,
        outcome_index=0, backend="pandas",
    )
    print(f"  {len(trades_df)} trades")
    if len(trades_df) == 0:
        print("WARN: no trades in window — the backtest will be empty.")

    # Optional companion bars (for plotting)
    bars_df = pm.price_series(
        slug=slug, start_time=win_start, end_time=win_end,
        freq="10s", outcome_index=0, fill_gaps=True, backend="pandas",
    )

    # ── 3. BTC spot data ─────────────────────────────────────────────────────
    # Pad ±2 minutes so the minute-OHLC interpolation has boundary candles
    print("\nFetching BTC-USD 1-min OHLC from Coinbase ...")
    btc_df = fetch_btc_minutes(
        STRIKE_TIME - pd.Timedelta(minutes=2),
        RESOLUTION_TIME + pd.Timedelta(minutes=2),
    )
    print(f"  {len(btc_df)} minute candles")
    if not btc_df.empty:
        print(f"  range: ${btc_df['low'].min():.2f} .. ${btc_df['high'].max():.2f}")
    btc_fn = btc_price_function(btc_df)
    print(f"  K (BTC @ strike): ${btc_fn(STRIKE_MS):.2f}")
    print(f"  S (BTC @ resolution): ${btc_fn(RES_MS):.2f}")

    # ── 4. Strategy & backtest ───────────────────────────────────────────────
    strategy = BSBinaryCallMM(
        btc_price_fn=btc_fn,
        strike_ms=STRIKE_MS,
        resolution_ms=RES_MS,
        sigma_annual=sigma_annual,
        half_spread=half_spread,
        quote_size=quote_size,
        max_inventory=max_inventory,
        requote_threshold=requote_threshold,
    )

    listing = Listing(
        symbol=slug, question=market.question, outcomes=market.outcomes,
        resolution_time=RES_MS,
    )

    print(f"\nRunning backtest: σ={sigma_annual}, half_spread={half_spread}, "
          f"quote_size={quote_size}, tick={tick_interval}")
    result = BacktestEngine(
        listings={slug: listing},
        trades={slug: trades_df},
        bars={slug: bars_df},
        resolutions={slug: RESOLUTION_VALUE},
        trader=strategy,
        initial_cash=initial_cash,
        cadence="clock",
        tick_interval=tick_interval,
        taker_fee_bps=taker_fee_bps,
        maker_rebate_bps=maker_rebate_bps,
    ).run()

    # ── 5. Report ────────────────────────────────────────────────────────────
    print("\n=== Summary ===")
    for k, v in result.summary().items():
        print(f"  {k:<24}: {v}")

    print(f"\n=== Fills ({len(result.fills)}) — first 20 ===")
    for f in result.fills[:20]:
        ts = pd.Timestamp(f.timestamp, unit="ms", tz="UTC")
        side = "BUY " if f.quantity > 0 else "SELL"
        print(f"  {ts.strftime('%H:%M:%S')}  {side}  qty={f.quantity:+.0f}  px={f.price:.4f}")
    if len(result.fills) > 20:
        print(f"  ... ({len(result.fills) - 20} more)")

    if show_plot:
        result.plot()

    return result


if __name__ == "__main__":
    main()
