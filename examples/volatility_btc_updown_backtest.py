"""Backtest BTC Up/Down market makers using short-horizon volatility estimates."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backtester import Listing
from research import (
    BTCMinuteFeatureFeed,
    EWMAVolatility,
    FixedVolatility,
    HybridRangeEWMAVolatility,
    VolatilityBinaryMarketMaker,
)
from polymarket_data import PolymarketData
from bs_mm_backtest import fetch_btc_minutes


EVENT_SLUG = "btc-updown-5m-1776732000"
STRIKE_TIME = datetime(2026, 4, 21, 0, 40, tzinfo=timezone.utc)
RESOLUTION_TIME = datetime(2026, 4, 21, 0, 45, tzinfo=timezone.utc)
RESOLUTION_VALUE = 0.0

STRIKE_MS = int(STRIKE_TIME.timestamp() * 1000)
RESOLUTION_MS = int(RESOLUTION_TIME.timestamp() * 1000)


def _load_market_data():
    pm = PolymarketData()
    market = pm.get_event(EVENT_SLUG)[0]
    slug = market.slug

    win_start = STRIKE_TIME
    win_end = RESOLUTION_TIME + pd.Timedelta(seconds=30)
    trades = pm.fetch_trades(
        slug=slug,
        start_time=win_start,
        end_time=win_end,
        outcome_index=0,
        backend="pandas",
    )
    bars = pm.price_series(
        slug=slug,
        start_time=win_start,
        end_time=win_end,
        freq="10s",
        outcome_index=0,
        fill_gaps=True,
        backend="pandas",
    )
    listing = Listing(
        symbol=slug,
        question=market.question,
        outcomes=market.outcomes,
        resolution_time=RESOLUTION_MS,
    )
    return slug, listing, trades, bars


def _build_strategy(name: str, btc_feed: BTCMinuteFeatureFeed):
    if name == "fixed_60":
        estimator = FixedVolatility(0.60)
        return VolatilityBinaryMarketMaker(
            btc_feed=btc_feed,
            vol_estimator=estimator,
            strike_ms=STRIKE_MS,
            resolution_ms=RESOLUTION_MS,
            half_spread=0.025,
            quote_size=25,
            max_inventory=250,
        )

    if name == "ewma":
        estimator = EWMAVolatility(
            btc_feed,
            lookback=8,
            half_life=3,
            fallback_sigma=0.60,
            min_sigma=0.25,
            max_sigma=2.50,
        )
        return VolatilityBinaryMarketMaker(
            btc_feed=btc_feed,
            vol_estimator=estimator,
            strike_ms=STRIKE_MS,
            resolution_ms=RESOLUTION_MS,
            half_spread=0.025,
            quote_size=25,
            max_inventory=250,
        )

    if name == "hybrid_jump":
        estimator = HybridRangeEWMAVolatility(
            btc_feed,
            lookback=8,
            half_life=3,
            fallback_sigma=0.60,
            min_sigma=0.25,
            max_sigma=2.50,
        )
        return VolatilityBinaryMarketMaker(
            btc_feed=btc_feed,
            vol_estimator=estimator,
            strike_ms=STRIKE_MS,
            resolution_ms=RESOLUTION_MS,
            half_spread=0.03,
            quote_size=20,
            max_inventory=200,
            jump_widen_threshold=2.0,
            jump_widen_multiplier=2.0,
        )

    raise ValueError(f"unknown strategy: {name}")


def run_variant(name: str, slug: str, listing: Listing, trades, bars, btc_feed: BTCMinuteFeatureFeed):
    strategy = _build_strategy(name, btc_feed)
    result = strategy.backtest(
        listings={slug: listing},
        trades={slug: trades},
        resolutions={slug: RESOLUTION_VALUE},
        bars={slug: bars},
    )
    summary = result.summary()
    return {
        "strategy": name,
        "pnl": result.total_pnl,
        "fills": len(result.fills),
        "turnover": result.turnover,
        "max_drawdown": result.max_drawdown,
        "sharpe": result.sharpe,
        "final_equity": summary.get("final_equity"),
    }, result


def main(show_plot: bool = False):
    slug, listing, trades, bars = _load_market_data()

    btc_minutes = fetch_btc_minutes(
        STRIKE_TIME - pd.Timedelta(minutes=10),
        RESOLUTION_TIME + pd.Timedelta(minutes=2),
    )
    btc_feed = BTCMinuteFeatureFeed(btc_minutes)

    print(f"Market: {listing.question}")
    print(f"Trades: {len(trades)}")
    print(f"Strike BTC: {btc_feed.price_at(STRIKE_MS):.2f}")
    print(f"Resolution BTC: {btc_feed.price_at(RESOLUTION_MS):.2f}")

    rows = []
    results = {}
    for name in ["fixed_60", "ewma", "hybrid_jump"]:
        row, result = run_variant(name, slug, listing, trades, bars, btc_feed)
        rows.append(row)
        results[name] = result

    comparison = pd.DataFrame(rows).set_index("strategy")
    print("\n=== Volatility Strategy Comparison ===")
    print(comparison.round(4))

    if show_plot and results:
        results["hybrid_jump"].plot()

    return comparison, results


if __name__ == "__main__":
    main()
