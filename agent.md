# BTC 5-Minute Polymarket Strategy Research Agent

## Objective

Find robust trading strategies for Polymarket BTC 5-minute Up/Down markets.
The settlement target is the Chainlink BTC/USD Data Stream used by Polymarket,
not raw CEX spot. Strategies may use CEX spot/order-book data as predictive
inputs, but all labels and evaluation must be tied to the Polymarket market
resolution rule.

The research process uses an island model:

1. Spawn three independent idea islands.
2. Let each island run two independent search loops with no cross-island
   information sharing.
3. After the second loop, a coordinator extracts worthy information and sends
   compact migration packets back to all islands.
4. Islands run one post-migration loop.
5. The coordinator evaluates final candidates on the frozen test set.

## Non-Negotiable Rules

- Do not optimize on the test set.
- Do not inspect another island's private notes before the first migration.
- Do not use future prices, final resolution, or final market metadata inside
  a strategy decision before the corresponding timestamp.
- Distinguish predictive inputs from settlement truth.
- Report negative results. A failed idea with a clear reason is useful.
- Keep code changes scoped to strategy, data, and backtest files.
- Preserve reproducibility: every run must write config, metrics, and a short
  rationale.

## Data Splits

Use BTC Up/Down 5-minute markets only. All splits are constrained to the
Goldsky subgraph window (Apr 1–28, 2026). The subgraph stopped indexing at
Apr 28 ~11:00 UTC due to a CTF Exchange v2 contract upgrade. No reliable
per-trade data exists beyond this date via any public API.

### Train Set

Date range: 2026-04-01 through 2026-04-21 UTC.
Markets: ~6,044 (6,031 with trades).

Purpose:

- Fit strategy parameters.
- Compare volatility estimators.
- Tune quote widths, inventory limits, and signal thresholds.
- Explore feature families.

### Validation Set

Date range: 2026-04-22 through 2026-04-28 UTC.
Markets: ~2,016 (1,863 with trades after ~11:00 UTC Apr 28 cutoff).

Purpose:

- Select among variants produced during independent island loops.
- Reject unstable strategies.
- Tune only coarse risk controls after train performance is known.

### Frozen Test Set

No test set is available with real trade data. The validation set serves as
the final hold-out. Do not tune on validation results after selecting a
strategy.

### Embargo

Apply a 1-day embargo between train and validation if features use rolling
windows longer than 30 minutes. Do not carry state, inventory, rolling
statistics, or learned parameters across split boundaries unless explicitly
declared as part of a live-deploy warmup protocol.

## Shared Evaluation Metrics

Every island must report:

- Total PnL.
- PnL per market.
- Sharpe or per-market mean/std PnL.
- Max drawdown.
- Turnover.
- Number of fills.
- Average inventory and max inventory.
- Tail loss: worst 1%, 5%, and 10% market-level PnL.
- Calibration: predicted fair probability bucket vs realized frequency.
- Adverse selection: average next-price move after our fills.
- Capacity proxy: PnL after halving fill probability and doubling slippage.

Primary selection metric:

```text
validation_score =
    median_market_pnl
  - 0.50 * abs(worst_5pct_market_pnl)
  - 0.10 * max_drawdown
  - 0.02 * turnover
```

Do not select purely by total PnL.

## Shared Backtest Assumptions

- Base instrument: Up outcome share.
- Fair value target: `P[Chainlink_BTCUSD_end >= Chainlink_BTCUSD_start]`.
- If Chainlink historical stream data is unavailable, use CEX BTC data as a
  proxy and label the experiment as `cex_proxy_settlement`.
- Backtest cadence: start with 1-second clock ticks.
- Initial cash: 1000 USDC per continuous run.
- No naked shorts.
- Position limits must be explicit.
- Include Polymarket fees if supported by the data; otherwise report
  fee-free and fee-stressed results.

## Artifact Layout

Each island writes artifacts under:

```text
research/island_<name>/loop_<n>/
```

Required files:

```text
config.json
notes.md
metrics_train.json
metrics_validation.json
strategy_summary.md
```

The coordinator writes:

```text
research/coordinator/migration_round_1.md
research/coordinator/final_selection.md
research/coordinator/test_results.json
```

## Island A: Volatility Surface And Digital Pricing

Hypothesis:

Polymarket 5-minute markets misprice short-horizon volatility and digital
convexity, especially near the strike and near expiry.

Initial idea family:

- EWMA realized volatility.
- Parkinson high-low range volatility.
- Hybrid max(EWMA, range volatility).
- Jump-aware quote widening.
- Time-to-expiry dependent spreads.
- Probability calibration by moneyness bucket:

```text
moneyness = (current_btc - price_to_beat) / expected_remaining_move
```

Search loop 1:

- Sweep volatility lookback: 3, 5, 8, 13, 21 minutes.
- Sweep half-life: 1, 2, 3, 5, 8 minutes.
- Sweep min/max annualized sigma clamps.
- Record calibration error and PnL by time-to-expiry bucket.

Search loop 2:

- Add quote policy changes:
  - wider spreads near strike in final 60 seconds,
  - no-quote band when fair is between 0.45 and 0.55 with high jump score,
  - inventory skew proportional to binary delta.
- Select the top two validation candidates before migration.

Post-migration loop:

- Incorporate only coordinator-approved external features.
- Do not turn into a lead-lag strategy unless the migration packet clearly
  shows a robust feature from Island B.

## Island B: Chainlink/CEX Lead-Lag And Directional Overlay

Hypothesis:

CEX spot moves lead the Chainlink settlement stream or Polymarket prices
enough to add a directional edge on top of the binary fair value.

Initial idea family:

- Coinbase/Binance/Bybit mid-price momentum.
- Cross-exchange price dispersion.
- Last 5/15/30/60 second returns.
- Micro trend vs mean reversion after bursts.
- Proxy model for Chainlink stream lag:

```text
expected_chainlink_next = current_chainlink_or_proxy + beta * cex_return_lead
```

Search loop 1:

- Build directional overlays on top of a fixed-vol binary model.
- Sweep momentum windows: 5, 10, 15, 30, 60 seconds.
- Sweep edge thresholds: 0.5%, 1%, 2%, 3%, 5% probability points.
- Compare quote-only vs taker-entry behavior.

Search loop 2:

- Add regime classification:
  - quiet,
  - trend,
  - reversal,
  - liquidation burst.
- Gate trading by regime.
- Measure hit rate conditional on seconds-to-expiry and distance-to-strike.

Post-migration loop:

- Share only features that improve validation score after slippage stress.
- If a signal works only in one market or one day, mark it as non-migratable.

## Island C: Microstructure, Inventory, And Execution

Hypothesis:

Even with a decent fair value, the strategy lives or dies on fill selection,
inventory, and when not to quote.

Initial idea family:

- Maker-only quote placement.
- Quote cancellation thresholds.
- Inventory-aware skew.
- Fill toxicity filters.
- Spread widening when market trades accelerate.
- Endgame de-risking in final 30/60/90 seconds.

Search loop 1:

- Sweep half spread: 1, 2, 3, 4, 5 cents.
- Sweep quote size: 5, 10, 20, 50 shares.
- Sweep inventory limits: 50, 100, 200, 500 shares.
- Compare fixed fair value vs EWMA fair value as the pricing anchor.

Search loop 2:

- Add no-quote filters:
  - skip if last N market trades are one-sided,
  - skip if fair changes faster than requote threshold,
  - skip near strike after jump score exceeds threshold,
  - flatten inventory when remaining time is below threshold.
- Stress fill assumptions:
  - optimistic current engine,
  - 50% fill haircut,
  - adverse one-tick fill price.

Post-migration loop:

- Convert useful features from Islands A and B into execution filters, not
  full strategy rewrites.

## Coordinator Protocol

The coordinator remains inactive until all islands complete loop 2.

Inputs:

```text
research/island_vol/loop_1/*
research/island_vol/loop_2/*
research/island_lead_lag/loop_1/*
research/island_lead_lag/loop_2/*
research/island_microstructure/loop_1/*
research/island_microstructure/loop_2/*
```

Coordinator tasks:

1. Read each island's `strategy_summary.md`, train metrics, and validation
   metrics.
2. Identify information worth migrating:
   - robust feature families,
   - risk controls,
   - calibration failures,
   - market regimes where a strategy should be disabled,
   - implementation bugs or data pitfalls.
3. Reject migration candidates that:
   - improve train but not validation,
   - depend on final resolution data,
   - require test-set inspection,
   - are too complex to reproduce.
4. Write one concise migration packet:

```text
research/coordinator/migration_round_1.md
```

Migration packet format:

```text
# Migration Round 1

## Global Findings

## Send To Island A

## Send To Island B

## Send To Island C

## Do Not Use
```

Each island may use only the section addressed to it plus Global Findings.

## Final Selection Protocol

After post-migration loop 3:

1. Freeze all parameters.
2. Run validation one final time.
3. Select at most three candidates:
   - one highest validation score,
   - one lowest drawdown,
   - one most orthogonal PnL profile.
4. Run only those candidates on the frozen test set.
5. Write:

```text
research/coordinator/final_selection.md
research/coordinator/test_results.json
```

Final report must include:

- Train, validation, and test metrics side by side.
- Whether the strategy uses CEX proxy settlement or Chainlink settlement data.
- Failure modes.
- Recommended next experiment.
- Whether the result is deployable, research-only, or rejected.

## Suggested Commands

Load data and run a strategy backtest:

```python
from research.dataset import DatasetCache, SPLITS
from research import VolatilityBinaryMarketMaker, BTCMinuteFeatureFeed, HybridRangeEWMAVolatility

cache = DatasetCache("data/btc_5m")
markets = cache.load_split("train")
btc_feed = cache.load_btc_feed()
# ... build strategy, call strategy.backtest(m.listing, m.trades, m.resolution)
```

When new experiment runners are created, they should accept:

```bash
--split train
--split validation
--split test
--strategy <name>
--outdir research/island_<name>/loop_<n>
```

## First Three Independent Ideas

Begin with these three seed strategies:

1. Volatility-calibrated market maker:
   Hybrid EWMA/range volatility, binary fair value, jump widening, and
   near-expiry no-quote bands.

2. Lead-lag directional maker:
   Fixed-vol binary fair value plus CEX momentum adjustment, trading only when
   the directional edge exceeds fees, spread, and adverse-selection buffer.

3. Execution-first maker:
   Simple fair value but strict inventory, toxicity, fill-stress, and endgame
   flattening controls.

The goal is not to make all three profitable immediately. The goal is to make
their errors different enough that migration can combine genuine information
instead of copying the same overfit strategy three times.
