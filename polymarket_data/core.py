"""PolymarketData — fetch and compute price series from Polymarket."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional, Union

from .gamma_client import GammaClient
from .models import MarketInfo
from .subgraph_client import SubgraphClient

_SCALE = 1_000_000


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_unix(dt: Union[datetime, int, float]) -> int:
    if isinstance(dt, (int, float)):
        return int(dt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _to_utc_datetime(dt: Union[datetime, int, float]) -> datetime:
    if isinstance(dt, (int, float)):
        return datetime.fromtimestamp(int(dt), tz=timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_trade_record(raw: dict, outcome_name: str) -> dict:
    """Convert a raw subgraph event into a flat trade dict."""
    maker_amt = int(raw["makerAmountFilled"])
    taker_amt = int(raw["takerAmountFilled"])

    # Polymarket fills always pair one outcome token with USDC (asset id "0").
    #   taker_id == "0"  →  maker sold outcome token  (SELL)
    #   maker_id == "0"  →  taker received outcome token (BUY)
    if raw["takerAssetId"] == "0":
        size = maker_amt / _SCALE
        price = (taker_amt / _SCALE) / size if size else 0.0
        side = "SELL"
    else:
        size = taker_amt / _SCALE
        price = (maker_amt / _SCALE) / size if size else 0.0
        side = "BUY"

    return {
        "timestamp": datetime.fromtimestamp(int(raw["timestamp"]), tz=timezone.utc),
        "price": price,
        "size": size,
        "side": side,
        "outcome": outcome_name,
    }


def _freq_for_pandas(freq: str) -> str:
    """Normalise a user-supplied frequency string for pandas resample."""
    m = re.fullmatch(r"(\d+)?([A-Za-z]+)", freq.strip())
    if not m:
        raise ValueError(f"Cannot parse frequency: {freq!r}")
    n, unit = m.group(1) or "1", m.group(2)
    unit = {"T": "min", "t": "min", "H": "h", "D": "d"}.get(unit, unit)
    return f"{n}{unit}"


def _freq_for_polars(freq: str) -> str:
    """Normalise a user-supplied frequency string for polars group_by_dynamic."""
    m = re.fullmatch(r"(\d+)?([A-Za-z]+)", freq.strip())
    if not m:
        raise ValueError(f"Cannot parse frequency: {freq!r}")
    n, unit = m.group(1) or "1", m.group(2).lower()
    mapping = {"h": "h", "min": "m", "t": "m", "d": "d", "w": "w", "s": "s"}
    pl_unit = mapping.get(unit)
    if pl_unit is None:
        raise ValueError(f"Unsupported frequency unit {unit!r} in {freq!r}")
    return f"{n}{pl_unit}"


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class PolymarketData:
    """
    Fetch and compute price series data from Polymarket.

    Each method that returns trade or bar data accepts an ``outcome`` (label
    string) or ``outcome_index`` (integer) parameter to select which side of
    the market to query.  Scoping to a single outcome token eliminates the
    mirror-image duplicate that would otherwise appear — a BUY of 10 shares
    at price *p* on the YES token also appears as a SELL of 10 shares at
    price *1−p* on the NO token.  By querying only one token ID the
    duplication never enters the data.

    Parameters
    ----------
    gamma_client : GammaClient, optional
    subgraph_client : SubgraphClient, optional

    Examples
    --------
    ::

        from datetime import datetime, timezone
        from polymarket_data import PolymarketData

        pm = PolymarketData()

        # OHLCV + VWAP hourly bars for the "Yes" outcome
        bars = pm.price_series(
            slug="russia-x-ukraine-ceasefire-by-february-28-2026",
            start_time=datetime(2026, 2, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 2, 20, tzinfo=timezone.utc),
            freq="1h",
            outcome="Yes",
            backend="pandas",
        )

        # Raw trade-by-trade data
        trades = pm.fetch_trades(
            slug="russia-x-ukraine-ceasefire-by-february-28-2026",
            start_time=datetime(2026, 2, 17, tzinfo=timezone.utc),
            end_time=datetime(2026, 2, 20, tzinfo=timezone.utc),
            outcome="Yes",
            backend="polars",
        )
    """

    def __init__(
        self,
        gamma_client: Optional[GammaClient] = None,
        subgraph_client: Optional[SubgraphClient] = None,
    ) -> None:
        self._gamma = gamma_client or GammaClient()
        self._subgraph = subgraph_client or SubgraphClient()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_market(self, slug: str) -> MarketInfo:
        """Resolve a market slug to metadata (token IDs, outcomes, question)."""
        info = self._gamma.get_market_by_slug(slug)
        if info is None:
            raise ValueError(f"No market found for slug: {slug!r}")
        return info

    def get_event(self, event_slug: str) -> list[MarketInfo]:
        """Return all markets belonging to an event slug.

        Use this when you have a Polymarket event URL slug (e.g.
        ``btc-updown-5m-1776732000``) rather than a direct market slug.
        Most events contain a single market; some (e.g. multi-outcome events)
        contain several.
        """
        markets = self._gamma.get_markets_by_event_slug(event_slug)
        if not markets:
            raise ValueError(f"No event found for slug: {event_slug!r}")
        return markets

    def fetch_trades(
        self,
        slug: str,
        start_time: Union[datetime, int, float],
        end_time: Union[datetime, int, float],
        outcome: Optional[str] = None,
        outcome_index: Optional[int] = None,
        backend: str = "pandas",
    ):
        """
        Return all filled trades for a single market outcome as a DataFrame.

        Trades are scoped to one outcome token, so each fill appears exactly
        once.  See class docstring for an explanation of the deduplication.

        Parameters
        ----------
        slug : str
            Polymarket market slug.
        start_time, end_time : datetime | int | float
            Time window (UTC datetime or Unix timestamp in seconds).
        outcome : str, optional
            Outcome label, e.g. ``"Yes"`` or ``"Trump"``.  Case-insensitive.
            Mutually exclusive with ``outcome_index``.
        outcome_index : int, optional
            Zero-based outcome index.  Defaults to ``0`` when neither
            ``outcome`` nor ``outcome_index`` is given.
        backend : {"pandas", "polars"}
            DataFrame library to use for the return value.

        Returns
        -------
        pandas.DataFrame or polars.DataFrame
            Columns: ``timestamp``, ``price``, ``size``, ``side``,
            ``outcome``.  Sorted by timestamp ascending.
        """
        info = self.get_market(slug)
        token_id, outcome_name = self._resolve_outcome(info, outcome, outcome_index)

        raw = self._subgraph.fetch_order_filled_events(
            token_ids=[token_id],
            start_ts=_to_unix(start_time),
            end_ts=_to_unix(end_time),
        )

        records = sorted(
            [_parse_trade_record(e, outcome_name) for e in raw],
            key=lambda r: r["timestamp"],
        )
        return self._to_df(records, backend)

    def price_series(
        self,
        slug: str,
        start_time: Union[datetime, int, float],
        end_time: Union[datetime, int, float],
        freq: str = "1h",
        outcome: Optional[str] = None,
        outcome_index: Optional[int] = None,
        fill_gaps: bool = True,
        backend: str = "pandas",
    ):
        """
        Compute OHLCV + VWAP bars for a market outcome.

        Each bar covers one ``freq`` interval. VWAP is computed as
        ``sum(price × size) / sum(size)`` within the interval.  Open and
        close are the prices of the first and last trades in the interval;
        high and low are the extremes.

        Parameters
        ----------
        slug : str
            Polymarket market slug.
        start_time, end_time : datetime | int | float
            Time window (UTC datetime or Unix timestamp in seconds).
        freq : str
            Bar width as a pandas-style offset string, e.g. ``"1h"``,
            ``"4h"``, ``"1d"``.
        outcome : str, optional
            Outcome label (e.g. ``"Yes"``).  Mutually exclusive with
            ``outcome_index``.
        outcome_index : int, optional
            Zero-based outcome index.  Defaults to ``0``.
        fill_gaps : bool
            If ``True`` (default), empty bars are included with ``NaN``
            prices and zero volume.  If ``False``, only bars with at least
            one trade are returned.
        backend : {"pandas", "polars"}

        Returns
        -------
        pandas.DataFrame or polars.DataFrame
            Columns: ``open``, ``high``, ``low``, ``close``, ``vwap``,
            ``volume``, ``trade_count``.  For pandas the timestamp is the
            index; for polars it is the first column.
        """
        trades = self.fetch_trades(
            slug=slug,
            start_time=start_time,
            end_time=end_time,
            outcome=outcome,
            outcome_index=outcome_index,
            backend=backend,
        )
        start_dt = _to_utc_datetime(start_time)
        end_dt = _to_utc_datetime(end_time)

        if backend == "pandas":
            return self._bars_pandas(trades, freq, start_dt, end_dt, fill_gaps)
        elif backend == "polars":
            return self._bars_polars(trades, freq, start_dt, end_dt, fill_gaps)
        else:
            raise ValueError(f"backend must be 'pandas' or 'polars', got {backend!r}")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_outcome(
        self,
        info: MarketInfo,
        outcome: Optional[str],
        outcome_index: Optional[int],
    ) -> tuple[str, str]:
        """Return ``(token_id, outcome_name)`` for the requested outcome."""
        if outcome is not None and outcome_index is not None:
            raise ValueError("Specify outcome or outcome_index, not both.")

        if outcome is not None:
            lower = outcome.lower()
            for name, tid in zip(info.outcomes, info.token_ids):
                if name.lower() == lower:
                    return tid, name
            raise ValueError(
                f"Outcome {outcome!r} not found. Available: {info.outcomes}"
            )

        idx = outcome_index if outcome_index is not None else 0
        if not 0 <= idx < len(info.token_ids):
            raise ValueError(
                f"outcome_index {idx} out of range "
                f"(market has {len(info.token_ids)} outcomes)"
            )
        return info.token_ids[idx], info.outcomes[idx]

    def _to_df(self, records: list[dict], backend: str):
        _COLS = ["timestamp", "price", "size", "side", "outcome"]

        if backend == "pandas":
            import pandas as pd
            if not records:
                return pd.DataFrame(columns=_COLS)
            df = pd.DataFrame(records)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df[_COLS]

        elif backend == "polars":
            import polars as pl
            if not records:
                return pl.DataFrame(schema={
                    "timestamp": pl.Datetime("us", "UTC"),
                    "price": pl.Float64,
                    "size": pl.Float64,
                    "side": pl.String,
                    "outcome": pl.String,
                })
            # Build column-wise to avoid polars timezone-parsing edge cases
            ts_us = [int(r["timestamp"].timestamp() * 1_000_000) for r in records]
            return pl.DataFrame({
                "timestamp": pl.Series(ts_us).cast(pl.Datetime("us", "UTC")),
                "price":   [r["price"]   for r in records],
                "size":    [r["size"]    for r in records],
                "side":    [r["side"]    for r in records],
                "outcome": [r["outcome"] for r in records],
            }).select(_COLS)

        else:
            raise ValueError(f"backend must be 'pandas' or 'polars', got {backend!r}")

    def _bars_pandas(
        self,
        df,
        freq: str,
        start_dt: datetime,
        end_dt: datetime,
        fill_gaps: bool,
    ):
        import pandas as pd

        freq_pd = _freq_for_pandas(freq)
        full_idx = pd.date_range(
            start=start_dt, end=end_dt, freq=freq_pd, tz="UTC", inclusive="left"
        )

        if df.empty:
            empty = pd.DataFrame(
                index=full_idx,
                columns=["open", "high", "low", "close", "vwap", "volume", "trade_count"],
                dtype=float,
            )
            empty["volume"] = 0.0
            empty["trade_count"] = 0
            empty.index.name = "timestamp"
            return empty if fill_gaps else empty.iloc[0:0]

        df = df.set_index("timestamp").sort_index()
        df["dollar_volume"] = df["price"] * df["size"]
        r = df.resample(freq_pd)

        bars = pd.DataFrame({
            "open":        r["price"].first(),
            "high":        r["price"].max(),
            "low":         r["price"].min(),
            "close":       r["price"].last(),
            "vwap":        r["dollar_volume"].sum() / r["size"].sum(),
            "volume":      r["size"].sum(),
            "trade_count": r["price"].count(),
        })

        if fill_gaps:
            bars = bars.reindex(full_idx)
            bars["volume"]      = bars["volume"].fillna(0.0)
            bars["trade_count"] = bars["trade_count"].fillna(0).astype(int)
        else:
            bars = bars[bars["trade_count"] > 0]

        bars.index.name = "timestamp"
        return bars

    def _bars_polars(
        self,
        df,
        freq: str,
        start_dt: datetime,
        end_dt: datetime,
        fill_gaps: bool,
    ):
        import polars as pl

        freq_pl = _freq_for_polars(freq)

        _EMPTY_SCHEMA = {
            "timestamp":   pl.Datetime("us", "UTC"),
            "open":        pl.Float64,
            "high":        pl.Float64,
            "low":         pl.Float64,
            "close":       pl.Float64,
            "vwap":        pl.Float64,
            "volume":      pl.Float64,
            "trade_count": pl.UInt32,
        }

        def _full_range() -> pl.DataFrame:
            ts = pl.datetime_range(
                start=start_dt,
                end=end_dt,
                interval=freq_pl,
                eager=True,
                time_unit="us",
                time_zone="UTC",
                closed="left",
            )
            return pl.DataFrame({"timestamp": ts})

        if df.is_empty():
            if fill_gaps:
                base = _full_range()
                return base.with_columns([
                    pl.lit(None).cast(pl.Float64).alias(c)
                    for c in ("open", "high", "low", "close", "vwap")
                ] + [
                    pl.lit(0.0).alias("volume"),
                    pl.lit(0).cast(pl.UInt32).alias("trade_count"),
                ])
            return pl.DataFrame(schema=_EMPTY_SCHEMA)

        df = df.sort("timestamp").with_columns(
            (pl.col("price") * pl.col("size")).alias("dollar_volume")
        )

        bars = df.group_by_dynamic(
            index_column="timestamp",
            every=freq_pl,
            start_by="window",
        ).agg([
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            (pl.col("dollar_volume").sum() / pl.col("size").sum()).alias("vwap"),
            pl.col("size").sum().alias("volume"),
            pl.col("price").count().cast(pl.UInt32).alias("trade_count"),
        ])

        if fill_gaps:
            bars = (
                _full_range()
                .join(bars, on="timestamp", how="left")
                .with_columns([
                    pl.col("volume").fill_null(0.0),
                    pl.col("trade_count").fill_null(0).cast(pl.UInt32),
                ])
            )

        return bars.sort("timestamp")
