"""Quick script version of btc_updown_backtest — run with: python examples/run_backtest.py"""
import sys, os, math
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
import pandas as pd

from polymarket_data import PolymarketData
from backtester import BacktestEngine, Cancel, Listing, Order, TradingState

# ── 1. Market metadata ────────────────────────────────────────────────────────
pm = PolymarketData()
markets = pm.get_event("btc-updown-5m-1776732000")
market  = markets[0]
SLUG    = market.slug
print(f"Question : {market.question}")
print(f"Outcomes : {market.outcomes}")

# ── 2. Fetch trades + bars ────────────────────────────────────────────────────
START = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
END   = datetime(2026, 4, 21, 1, 0, tzinfo=timezone.utc)

trades = pm.fetch_trades(slug=SLUG, start_time=START, end_time=END,
                         outcome_index=0, backend="pandas")
bars   = pm.price_series(slug=SLUG, start_time=START, end_time=END,
                         freq="5min", outcome_index=0, fill_gaps=True, backend="pandas")
print(f"\n{len(trades)} trades | {len(bars)} bars | {(bars['trade_count'] > 0).sum()} bars with trades")

# ── 3. Strategy ───────────────────────────────────────────────────────────────
BUY_THRESHOLD, SELL_THRESHOLD, TRADE_SIZE = 0.35, 0.65, 100

class FadeExtremesTrader:
    def run(self, state: TradingState):
        symbol   = next(iter(state.listings))
        bar      = state.bars[symbol]
        position = state.position.get(symbol, 0.0)
        open_buys  = sum(1 for o in state.open_orders.get(symbol, []) if o.quantity > 0)
        open_sells = sum(1 for o in state.open_orders.get(symbol, []) if o.quantity < 0)

        orders = []
        if bar.trade_count == 0 or math.isnan(bar.close):
            return {symbol: orders}, ""
        if bar.close < BUY_THRESHOLD and position <= 0 and open_buys == 0:
            orders.append(Order(symbol=symbol, price=1.0, quantity=TRADE_SIZE))
        elif bar.close > SELL_THRESHOLD and position > 0 and open_sells == 0:
            orders.append(Order(symbol=symbol, price=0.0, quantity=-position))
        return {symbol: orders}, ""

# ── 4. Backtest ───────────────────────────────────────────────────────────────
listing = Listing(
    symbol=SLUG, question=market.question, outcomes=market.outcomes,
    resolution_time=int(datetime(2026, 4, 21, 0, 45, tzinfo=timezone.utc).timestamp() * 1000),
)
result = BacktestEngine(
    listings={SLUG: listing},
    trades={SLUG: trades},
    bars={SLUG: bars},
    resolutions={SLUG: 0.0},
    trader=FadeExtremesTrader(),
    initial_cash=1000.0,
    cadence="clock",
    tick_interval="5min",
    taker_fee_bps=0.0,
    maker_rebate_bps=0.0,
).run()

# ── 5. Results ────────────────────────────────────────────────────────────────
print("\n=== Summary ===")
for k, v in result.summary().items():
    print(f"  {k:<15}: {v}")

print(f"\n=== Fills ({len(result.fills)}) ===")
for f in result.fills:
    ts   = pd.Timestamp(f.timestamp, unit="ms", tz="UTC")
    side = "BUY" if f.quantity > 0 else "SELL"
    print(f"  {ts}  {side}  qty={f.quantity}  price={f.price:.4f}")

result.plot()
