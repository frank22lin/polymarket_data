"""Disk-backed dataset cache for BTC Up/Down 5-min Polymarket markets.

Typical workflow::

    from research.dataset import DatasetCache

    cache = DatasetCache("data/btc_5m")

    # One-time downloads (safe to re-run, skips already-cached items)
    cache.fetch("2026-04-01", "2026-06-04")
    cache.fetch_btc_spot("2026-03-25", "2026-06-07")  # wider window for vol warmup

    # In experiments
    markets = cache.load_split("train")   # → list[MarketData]
    btc_feed = cache.load_btc_feed()      # → BTCMinuteFeatureFeed (full cached range)

    for m in markets:
        strategy.strike_ms = m.strike_ms
        result = strategy.backtest(m.listing, m.trades, m.resolution)
"""

from __future__ import annotations

import json
import math
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from backtester.datamodel import Listing
from polymarket_data.gamma_client import GammaClient
from polymarket_data.subgraph_client import SubgraphClient
from polymarket_data.core import _parse_trade_record

_UTC = timezone.utc

# ── Split definitions ──────────────────────────────────────────────────────────
# Constrained to the Goldsky subgraph data window (Apr 1 – Apr 28, 2026).
# The subgraph stopped indexing at Apr 28 ~11:00 UTC due to a CTF Exchange
# contract upgrade. May/Jun markets have no reliable per-trade data.
SPLITS: dict[str, tuple[datetime, datetime]] = {
    "train":      (datetime(2026, 4, 1,  tzinfo=_UTC), datetime(2026, 4, 21, 23, 59, 59, tzinfo=_UTC)),
    "validation": (datetime(2026, 4, 22, tzinfo=_UTC), datetime(2026, 4, 28, 23, 59, 59, tzinfo=_UTC)),
}

# For BTC Up/Down 5-min markets the market window is exactly 5 minutes.
_MARKET_DURATION_MS = 5 * 60 * 1_000


@dataclass
class MarketData:
    """A single BTC Up/Down 5-min market, ready for backtesting.

    Plugs directly into strategy.backtest()::

        result = strategy.backtest(m.listing, m.trades, m.resolution)

    The listing's resolution_time is the end of the 5-minute window (in ms).
    strike_ms is the start of the window (when the reference BTC price is snapped).
    """

    slug: str
    listing: Listing       # symbol=slug, resolution_time in ms
    trades: pd.DataFrame   # columns: timestamp, price, size, side, outcome
    resolution: float      # 1.0 = Up won, 0.0 = Down won

    @property
    def strike_ms(self) -> int:
        """Start of the 5-minute window (BTC reference price snapshot time)."""
        return self.listing.resolution_time - _MARKET_DURATION_MS

    @property
    def resolution_dt(self) -> datetime:
        return datetime.fromtimestamp(self.listing.resolution_time / 1000, tz=_UTC)


class DatasetCache:
    """Disk-backed cache of BTC Up/Down 5-min Polymarket markets.

    Directory layout::

        {cache_dir}/
          index.parquet               market metadata (slug, question, times, resolution)
          trades/{slug}.parquet       per-market trades
          btc_spot.parquet            BTC-USD 1-min OHLC (Coinbase)
    """

    # Default tag_slug for BTC 5-min Up/Down markets on Polymarket.
    # If list_markets() returns 0 results, inspect a known market's tags field
    # at gamma-api.polymarket.com/markets?slug=<known-slug> to find the right value.
    TAG_SLUG = "btc-updown-5m"

    def __init__(self, cache_dir: str = "data/btc_5m") -> None:
        self.root = Path(cache_dir)
        (self.root / "trades").mkdir(parents=True, exist_ok=True)

    # ── Index ──────────────────────────────────────────────────────────────

    @property
    def _index_path(self) -> Path:
        return self.root / "index.parquet"

    def _load_index(self) -> pd.DataFrame:
        if not self._index_path.exists():
            return pd.DataFrame({
                "slug":              pd.Series(dtype="str"),
                "question":          pd.Series(dtype="str"),
                "resolution_time_s": pd.Series(dtype="float64"),
                "resolution_value":  pd.Series(dtype="float64"),
                "token_id_0":        pd.Series(dtype="str"),
            })
        return pd.read_parquet(self._index_path)

    def _save_index(self, df: pd.DataFrame) -> None:
        df.to_parquet(self._index_path, index=False)

    # ── Market download ────────────────────────────────────────────────────

    def fetch(
        self,
        start_date: str,
        end_date: str,
        force: bool = False,
        sleep: float = 0.2,
        batch_size: int = 90,
    ) -> None:
        """Download all BTC Up/Down 5-min markets in [start_date, end_date] to disk.

        Enumerates markets by generating expected event slugs every 5 minutes
        and batch-fetching from the Gamma API (~130 requests for 45 days).
        Safe to re-run — already-cached trade files are skipped unless force=True.

        Parameters
        ----------
        start_date, end_date : str
            ISO date strings, e.g. ``"2026-04-01"`` / ``"2026-06-04"``.
        force : bool
            Re-download trade files even if they already exist.
        sleep : float
            Seconds to pause between Gamma API batch requests.
        batch_size : int
            Slugs per Gamma API request (max 90; API hard-caps results at 100).
        """
        start_dt = datetime.fromisoformat(start_date).replace(tzinfo=_UTC)
        end_dt   = datetime.fromisoformat(end_date).replace(tzinfo=_UTC)
        start_ts = int(start_dt.timestamp())
        end_ts   = int(end_dt.timestamp())

        # Build expected event slugs: btc-updown-5m-{strike_ts}
        # resolution_time = strike_ts + 300s; we want resolution_time in [start_ts, end_ts]
        step = 300
        first_strike = (start_ts - step) - ((start_ts - step) % step)
        slugs = [
            f"btc-updown-5m-{ts}"
            for ts in range(first_strike, end_ts + step, step)
        ]
        n_batches = (len(slugs) + batch_size - 1) // batch_size
        print(f"Enumerating {len(slugs)} candidate slugs in {n_batches} batches ...")

        gamma   = GammaClient()
        fetched = gamma.get_markets_by_event_slugs(slugs, batch_size=batch_size, sleep=sleep)
        in_range = [
            m for m in fetched
            if m.resolution_time is not None
            and start_ts <= m.resolution_time <= end_ts
        ]
        print(f"  Found {len(in_range)} markets (from {len(fetched)} returned by API)")
        if not in_range:
            print("  No markets found in range.")
            return

        # Update index with any newly discovered markets
        idx = self._load_index()
        known_slugs = set(idx["slug"].tolist())
        new_rows = [
            {
                "slug":              m.slug,
                "question":          m.question,
                "resolution_time_s": m.resolution_time,
                "resolution_value":  m.resolution_value if m.resolution_value is not None else math.nan,
                "token_id_0":        m.token_ids[0] if m.token_ids else "",
            }
            for m in in_range if m.slug not in known_slugs
        ]
        if new_rows:
            new_df = pd.DataFrame(new_rows).dropna(how="all")
            idx = pd.concat([idx, new_df], ignore_index=True)
            self._save_index(idx)
            print(f"  Added {len(new_rows)} new entries to index")

        # Fetch trades for markets not yet cached
        subgraph = SubgraphClient()
        to_fetch = [
            row for _, row in idx.iterrows()
            if start_dt <= datetime.fromtimestamp(row["resolution_time_s"], tz=_UTC) <= end_dt
            and (force or not (self.root / "trades" / f"{row['slug']}.parquet").exists())
        ]
        print(f"Fetching trades for {len(to_fetch)} markets ...")

        for i, row in enumerate(to_fetch, 1):
            slug     = row["slug"]
            token_id = row["token_id_0"]
            res_s    = int(row["resolution_time_s"])
            # Fetch from 15 min before resolution (covers 5-min window + 10-min pre-trading)
            start_s  = res_s - 15 * 60
            end_s    = res_s + 60

            try:
                raw = subgraph.fetch_order_filled_events(
                    token_ids=[token_id],
                    start_ts=start_s,
                    end_ts=end_s,
                )
                records = [_parse_trade_record(e, "Up") for e in raw]
                trades_df = pd.DataFrame(records) if records else pd.DataFrame(
                    columns=["timestamp", "price", "size", "side", "outcome"]
                )
                if not trades_df.empty:
                    trades_df["timestamp"] = pd.to_datetime(trades_df["timestamp"], utc=True)

                # If subgraph returned nothing, fall back to CLOB price snapshots.
                # The CLOB /prices-history endpoint returns 1-min last-trade prices;
                # we emit one synthetic trade per snapshot (size=1) so the backtester
                # has a price signal to work with even when subgraph data is unavailable.
                if trades_df.empty:
                    trades_df = self._fetch_clob_trades(token_id, start_s, end_s)

                trades_df.to_parquet(self.root / "trades" / f"{slug}.parquet", index=False)

                if i % 50 == 0 or i == len(to_fetch):
                    print(f"  [{i}/{len(to_fetch)}] {slug}: {len(trades_df)} trades")
            except Exception as exc:
                print(f"  WARN [{i}/{len(to_fetch)}] {slug}: {exc}")

            time.sleep(sleep)

        print("fetch() complete.")

    # ── CLOB fallback ─────────────────────────────────────────────────────

    _CLOB_PRICES_URL = "https://clob.polymarket.com/prices-history"

    def _fetch_clob_trades(
        self,
        token_id: str,
        start_s: int,
        end_s: int,
    ) -> pd.DataFrame:
        """Fetch 1-min price snapshots from CLOB and return as synthetic trade records.

        Each price snapshot becomes one trade row with size=1 and side='BUY'.
        This is an approximation used when the Goldsky subgraph has no data.
        """
        empty = pd.DataFrame(columns=["timestamp", "price", "size", "side", "outcome"])
        try:
            params = urllib.parse.urlencode({
                "market":   token_id,
                "startTs":  start_s,
                "endTs":    end_s,
                "fidelity": 1,
            })
            req = urllib.request.Request(
                f"{self._CLOB_PRICES_URL}?{params}",
                headers={"Accept": "application/json", "User-Agent": "polymarket-data/0.1"},
            )
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
            history = data.get("history", [])
            if not history:
                return empty
            rows = [
                {
                    "timestamp": pd.Timestamp(h["t"], unit="s", tz="UTC"),
                    "price":     float(h["p"]),
                    "size":      1.0,
                    "side":      "BUY",
                    "outcome":   "Up",
                }
                for h in history
            ]
            return pd.DataFrame(rows)
        except Exception:
            return empty

    # ── BTC spot ───────────────────────────────────────────────────────────

    @property
    def _btc_spot_path(self) -> Path:
        return self.root / "btc_spot.parquet"

    def fetch_btc_spot(
        self,
        start: str | datetime,
        end: str | datetime,
        sleep: float = 0.2,
    ) -> None:
        """Download BTC-USD 1-min OHLC from Coinbase and cache to disk.

        Paginates in 250-minute windows (Coinbase hard limit: 300 candles).
        Safe to re-run — merges with existing cache, deduplicates by timestamp.

        Parameters
        ----------
        start, end : str or datetime
            Inclusive date range. Use a wider window than your market data
            (e.g. 10 min before first market open) to ensure vol estimators
            have enough history at warm-up.
        """
        if isinstance(start, str):
            start = datetime.fromisoformat(start).replace(tzinfo=_UTC)
        if isinstance(end, str):
            end = datetime.fromisoformat(end).replace(tzinfo=_UTC)

        existing = pd.DataFrame()
        if self._btc_spot_path.exists():
            existing = pd.read_parquet(self._btc_spot_path)
            existing["start"] = pd.to_datetime(existing["start"], utc=True)

        all_rows: list[dict] = []
        cursor   = start
        window   = timedelta(minutes=250)

        while cursor < end:
            window_end = min(cursor + window, end)
            url = (
                "https://api.coinbase.com/api/v3/brokerage/market/products"
                "/BTC-USD/candles?" + urllib.parse.urlencode({
                    "start":       int(cursor.timestamp()),
                    "end":         int(window_end.timestamp()),
                    "granularity": "ONE_MINUTE",
                })
            )
            try:
                with urllib.request.urlopen(url, timeout=20) as resp:
                    payload = json.loads(resp.read())
                for c in payload.get("candles", []):
                    all_rows.append({
                        "start":  datetime.fromtimestamp(int(c["start"]), tz=_UTC),
                        "open":   float(c["open"]),
                        "high":   float(c["high"]),
                        "low":    float(c["low"]),
                        "close":  float(c["close"]),
                        "volume": float(c["volume"]),
                    })
            except Exception as exc:
                print(f"  WARN: candles {cursor:%Y-%m-%d %H:%M}–{window_end:%H:%M}: {exc}")

            cursor = window_end
            time.sleep(sleep)

        if not all_rows:
            print("No BTC candles fetched.")
            return

        new_df = (
            pd.DataFrame(all_rows)
            .sort_values("start")
            .drop_duplicates("start")
        )

        combined = (
            pd.concat([existing, new_df])
            .drop_duplicates("start")
            .sort_values("start")
            .reset_index(drop=True)
            if not existing.empty
            else new_df.reset_index(drop=True)
        )
        combined.to_parquet(self._btc_spot_path, index=False)
        print(f"BTC spot: {len(combined)} candles cached ({len(all_rows)} fetched).")

    def load_btc_feed(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ):
        """Return a BTCMinuteFeatureFeed from the cached BTC spot data.

        If start/end are None, returns the full cached range.
        """
        from research.volatility_strategies import BTCMinuteFeatureFeed

        if not self._btc_spot_path.exists():
            raise FileNotFoundError(
                "BTC spot data not found. Run cache.fetch_btc_spot(...) first."
            )
        df = pd.read_parquet(self._btc_spot_path)
        df["start"] = pd.to_datetime(df["start"], utc=True)

        if start is not None or end is not None:
            mask = pd.Series(True, index=df.index)
            if start is not None:
                mask &= df["start"] >= pd.Timestamp(start, tz=_UTC)
            if end is not None:
                mask &= df["start"] <= pd.Timestamp(end, tz=_UTC)
            df = df[mask].reset_index(drop=True)

        if df.empty:
            raise ValueError("No BTC spot data in the requested range.")
        return BTCMinuteFeatureFeed(df)

    # ── Load split ─────────────────────────────────────────────────────────

    def load_split(
        self,
        split: str,
        min_trades: int = 5,
        splits: Optional[dict] = None,
    ) -> list[MarketData]:
        """Load all cached markets for the named split.

        Parameters
        ----------
        split : str
            ``"train"``, ``"validation"``, or ``"test"``.
        min_trades : int
            Skip markets with fewer trades than this (near-empty / illiquid).
        splits : dict, optional
            Override default split date ranges from ``research.dataset.SPLITS``.

        Returns
        -------
        list[MarketData]
            Sorted by resolution_time ascending.
        """
        _splits = splits or SPLITS
        if split not in _splits:
            raise ValueError(f"Unknown split {split!r}. Options: {list(_splits)}")

        start_dt, end_dt = _splits[split]
        idx = self._load_index()
        if idx.empty:
            return []

        mask = idx["resolution_time_s"].between(
            start_dt.timestamp(),
            end_dt.timestamp(),
        )
        rows = idx[mask].sort_values("resolution_time_s").reset_index(drop=True)

        markets: list[MarketData] = []
        skipped_missing = 0
        skipped_sparse  = 0

        for _, row in rows.iterrows():
            slug       = row["slug"]
            trade_path = self.root / "trades" / f"{slug}.parquet"

            if not trade_path.exists():
                skipped_missing += 1
                continue

            trades = pd.read_parquet(trade_path)
            if len(trades) < min_trades:
                skipped_sparse += 1
                continue
            trades["timestamp"] = pd.to_datetime(trades["timestamp"], utc=True)

            resolution_value = float(row["resolution_value"])
            if math.isnan(resolution_value):
                skipped_sparse += 1
                continue

            resolution_time_ms = int(row["resolution_time_s"]) * 1_000
            listing = Listing(
                symbol=slug,
                question=str(row["question"]),
                outcomes=["Up", "Down"],
                resolution_time=resolution_time_ms,
            )

            markets.append(MarketData(
                slug=slug,
                listing=listing,
                trades=trades,
                resolution=resolution_value,
            ))

        total = len(rows)
        print(
            f"load_split({split!r}): {len(markets)}/{total} markets loaded"
            f" (skipped {skipped_missing} not-fetched, {skipped_sparse} sparse/unresolved)"
        )
        return markets

    # ── Stats ──────────────────────────────────────────────────────────────

    def summary(self) -> pd.DataFrame:
        """Return a summary DataFrame of the index for inspection."""
        idx = self._load_index()
        if idx.empty:
            return idx
        df = idx.copy()
        df["resolution_dt"] = pd.to_datetime(df["resolution_time_s"], unit="s", utc=True)
        df["has_trades"] = df["slug"].apply(
            lambda s: (self.root / "trades" / f"{s}.parquet").exists()
        )
        return df[["slug", "resolution_dt", "resolution_value", "has_trades", "question"]]
