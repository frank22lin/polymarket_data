# polymarket-data

A Python library for fetching historical trade data from
[Polymarket](https://polymarket.com), with built-in OHLCV + VWAP price series
computation.

## Installation

```bash
# With pandas
pip install "polymarket-data[pandas]"

# With polars
pip install "polymarket-data[polars]"

# Both
pip install "polymarket-data[all]"
```

Requires Python 3.11+. The core package has no mandatory dependencies —
pandas and polars are optional extras loaded only when you pass
`backend="pandas"` or `backend="polars"`.

## Quick start

```python
from datetime import datetime, timezone
from polymarket_data import PolymarketData

pm = PolymarketData()

# Hourly OHLCV + VWAP bars for the Yes outcome
bars = pm.price_series(
    slug="russia-x-ukraine-ceasefire-by-february-28-2026",
    start_time=datetime(2026, 2, 17, tzinfo=timezone.utc),
    end_time=datetime(2026, 2, 20, tzinfo=timezone.utc),
    freq="1h",
    outcome="Yes",
    backend="pandas",
)
# timestamp (index) | open | high | low | close | vwap | volume | trade_count
```

See [`examples/tutorial.ipynb`](examples/tutorial.ipynb) for a full walkthrough.

## API

### `PolymarketData`

All methods accept `outcome` (label string, e.g. `"Yes"`) or
`outcome_index` (integer, e.g. `0`) to select which side of a market to
query. The default is `outcome_index=0`.

---

#### `get_market(slug) → MarketInfo`

Resolve a market slug to its metadata.

```python
market = pm.get_market("will-donald-trump-win-the-2024-us-presidential-election")
# market.question   → "Will Donald Trump win the 2024 US Presidential Election?"
# market.outcomes   → ["Yes", "No"]
# market.token_ids  → ["<yes-token-id>", "<no-token-id>"]
```

---

#### `fetch_trades(...) → DataFrame`

Return every filled trade for a single outcome as a DataFrame.

```python
trades = pm.fetch_trades(
    slug="...",
    start_time=datetime(2024, 10, 28, tzinfo=timezone.utc),
    end_time=datetime(2024, 10, 29, tzinfo=timezone.utc),
    outcome="Yes",       # or outcome_index=0
    backend="pandas",    # or "polars"
)
```

| Column | Type | Description |
|---|---|---|
| `timestamp` | datetime (UTC) | Fill time |
| `price` | float | USDC per share \[0, 1\] |
| `size` | float | Shares traded |
| `side` | str | `"BUY"` or `"SELL"` |
| `outcome` | str | Outcome label |

---

#### `price_series(...) → DataFrame`

Aggregate trades into OHLCV + VWAP time bars.

```python
bars = pm.price_series(
    slug="...",
    start_time=...,
    end_time=...,
    freq="1h",           # pandas-style offset string
    outcome="Yes",
    fill_gaps=True,      # include empty bars with NaN prices, zero volume
    backend="pandas",
)
```

| Column | Description |
|---|---|
| `open` | First trade price in the interval |
| `high` | Highest trade price |
| `low` | Lowest trade price |
| `close` | Last trade price |
| `vwap` | Σ(price × size) / Σ(size) |
| `volume` | Total shares traded |
| `trade_count` | Number of fills |

The timestamp is the **index** for pandas and the **first column** for polars,
aligned to natural UTC boundaries (e.g. full hours, midnight).

Supported frequency strings: `"1h"`, `"4h"`, `"1d"`, `"5min"`, `"1w"`, etc.

### Data sources

| Source | Used for |
|---|---|
| [Gamma API](https://gamma-api.polymarket.com) | Market metadata (slug → token IDs) |
| [Goldsky subgraph](https://goldsky.com) | Historical filled trades |
