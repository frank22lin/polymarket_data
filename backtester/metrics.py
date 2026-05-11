from __future__ import annotations

import json
import math
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

import pandas as pd

from .datamodel import Trade

_SECONDS_PER_YEAR = 365.25 * 24 * 3600


@dataclass
class BacktestResult:
    symbol: str
    bars: Optional[pd.DataFrame]                   # OHLCV bars for the single-market plot path; may be None
    fills: List[Trade]
    resolution: float                              # NaN for multi-market
    equity_curve: pd.Series                        # aggregate equity over time
    initial_cash: float
    bars_by_symbol: Dict[str, pd.DataFrame] = field(default_factory=dict)
    resolutions: Dict[str, float] = field(default_factory=dict)
    equity_by_symbol: Dict[str, pd.Series] = field(default_factory=dict)
    realized_pnl_by_symbol: Dict[str, float] = field(default_factory=dict)
    round_trips: List[dict] = field(default_factory=list)  # FIFO-grouped closed slices
    total_taker_fees: float = 0.0
    total_maker_rebates: float = 0.0
    run_config: Dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # PnL & equity
    # ------------------------------------------------------------------

    @property
    def pnl_curve(self) -> pd.Series:
        return self.equity_curve - self.initial_cash

    @property
    def total_pnl(self) -> float:
        if self.equity_curve.empty:
            return 0.0
        return float(self.equity_curve.iloc[-1] - self.initial_cash)

    @property
    def total_realized_pnl(self) -> float:
        return float(sum(self.realized_pnl_by_symbol.values()))

    @property
    def total_unrealized_pnl(self) -> float:
        # When the run has fully settled (resolution events fire) total_unrealized = 0.
        # During the run it equals total_pnl - realized.
        return self.total_pnl - self.total_realized_pnl

    @property
    def total_fees(self) -> float:
        return float(self.total_taker_fees - self.total_maker_rebates)

    @property
    def gross_pnl(self) -> float:
        return self.total_pnl + self.total_fees

    # ------------------------------------------------------------------
    # Risk-adjusted return
    # ------------------------------------------------------------------

    def _returns(self) -> pd.Series:
        """Per-step returns: Δequity / equity_prev. Drops the first sample."""
        if self.equity_curve.empty or len(self.equity_curve) < 2:
            return pd.Series(dtype=float)
        eq = self.equity_curve.astype(float)
        prev = eq.shift(1)
        # Avoid divide-by-near-zero
        prev = prev.where(prev.abs() > 1e-9, other=float("nan"))
        return (eq - prev) / prev

    def _avg_seconds_per_step(self) -> float:
        if self.equity_curve.empty or len(self.equity_curve) < 2:
            return float("nan")
        idx = self.equity_curve.index
        deltas = (idx[-1] - idx[0]).total_seconds()
        return deltas / max(len(idx) - 1, 1)

    @property
    def volatility(self) -> float:
        """Std dev of per-step returns (not annualized)."""
        r = self._returns().dropna()
        return float(r.std()) if len(r) else float("nan")

    @property
    def annualized_volatility(self) -> float:
        r = self._returns().dropna()
        if len(r) == 0:
            return float("nan")
        avg_dt = self._avg_seconds_per_step()
        if math.isnan(avg_dt) or avg_dt <= 0:
            return float("nan")
        steps_per_year = _SECONDS_PER_YEAR / avg_dt
        return float(r.std() * math.sqrt(steps_per_year))

    @property
    def sharpe(self) -> float:
        """Annualized Sharpe using actual time deltas. Risk-free rate assumed 0."""
        r = self._returns().dropna()
        if len(r) == 0:
            return float("nan")
        std = r.std()
        if std == 0 or math.isnan(std):
            return float("nan")
        avg_dt = self._avg_seconds_per_step()
        if math.isnan(avg_dt) or avg_dt <= 0:
            return float("nan")
        steps_per_year = _SECONDS_PER_YEAR / avg_dt
        return float(r.mean() / std * math.sqrt(steps_per_year))

    # ------------------------------------------------------------------
    # Drawdown
    # ------------------------------------------------------------------

    @property
    def underwater_curve(self) -> pd.Series:
        """Equity drawdown from running peak, in dollars (≤ 0)."""
        if self.equity_curve.empty:
            return pd.Series(dtype=float)
        peak = self.equity_curve.cummax()
        return self.equity_curve - peak

    @property
    def max_drawdown(self) -> float:
        uw = self.underwater_curve
        return float(uw.min()) if len(uw) else float("nan")

    @property
    def max_drawdown_pct(self) -> float:
        if self.equity_curve.empty:
            return float("nan")
        peak = self.equity_curve.cummax()
        peak_safe = peak.where(peak.abs() > 1e-9, other=float("nan"))
        rel = (self.equity_curve - peak) / peak_safe
        return float(rel.min())

    @property
    def max_drawdown_duration(self) -> Optional[pd.Timedelta]:
        """Longest peak-to-recovery span. None if never recovered."""
        if self.equity_curve.empty:
            return None
        eq = self.equity_curve
        peak = eq.cummax()
        underwater = eq < peak
        if not underwater.any():
            return pd.Timedelta(0)
        # Find longest contiguous underwater stretch
        longest = pd.Timedelta(0)
        start_ts = None
        for ts, is_uw in underwater.items():
            if is_uw and start_ts is None:
                start_ts = ts
            elif not is_uw and start_ts is not None:
                longest = max(longest, ts - start_ts)
                start_ts = None
        if start_ts is not None:
            longest = max(longest, eq.index[-1] - start_ts)
        return longest

    # ------------------------------------------------------------------
    # Trade-level stats (from FIFO round-trips)
    # ------------------------------------------------------------------

    @property
    def num_round_trips(self) -> int:
        return len(self.round_trips)

    @property
    def win_rate(self) -> float:
        if not self.round_trips:
            return float("nan")
        wins = sum(1 for r in self.round_trips if r["pnl"] > 0)
        return wins / len(self.round_trips)

    @property
    def avg_win(self) -> float:
        wins = [r["pnl"] for r in self.round_trips if r["pnl"] > 0]
        return float(sum(wins) / len(wins)) if wins else float("nan")

    @property
    def avg_loss(self) -> float:
        losses = [r["pnl"] for r in self.round_trips if r["pnl"] < 0]
        return float(sum(losses) / len(losses)) if losses else float("nan")

    @property
    def profit_factor(self) -> float:
        gross_wins = sum(r["pnl"] for r in self.round_trips if r["pnl"] > 0)
        gross_losses = -sum(r["pnl"] for r in self.round_trips if r["pnl"] < 0)
        if gross_losses == 0:
            return float("inf") if gross_wins > 0 else float("nan")
        return float(gross_wins / gross_losses)

    @property
    def avg_hold_time(self) -> Optional[pd.Timedelta]:
        if not self.round_trips:
            return None
        deltas_ms = [r["exit_ts"] - r["entry_ts"] for r in self.round_trips]
        return pd.Timedelta(milliseconds=float(sum(deltas_ms)) / len(deltas_ms))

    @property
    def turnover(self) -> float:
        """Total notional traded / initial cash."""
        if self.initial_cash <= 0:
            return float("nan")
        notional = sum(abs(f.quantity) * f.price for f in self.fills)
        return float(notional / self.initial_cash)

    # ------------------------------------------------------------------
    # Buy-and-hold benchmark
    # ------------------------------------------------------------------

    def buy_and_hold_curve(self, shares_per_market: float = 1.0) -> pd.Series:
        """Equal-weight 'buy 1 share at first observed price, hold to resolution' benchmark.

        Aligned to the strategy's equity_curve index (forward-fill last known
        price per market).
        """
        if self.equity_curve.empty or not self.bars_by_symbol:
            return pd.Series(dtype=float)
        idx = self.equity_curve.index
        total = pd.Series(0.0, index=idx)
        cost = 0.0
        for symbol, bars in self.bars_by_symbol.items():
            if bars is None or bars.empty or "close" not in bars.columns:
                continue
            close = bars["close"].astype(float).reindex(idx, method="ffill")
            # Use first non-NaN close as the entry price for the benchmark
            first_valid = close.dropna()
            if first_valid.empty:
                continue
            entry_price = float(first_valid.iloc[0])
            cost += shares_per_market * entry_price
            # After settlement, MTM at resolution
            res = self.resolutions.get(symbol, float("nan"))
            mtm = close.fillna(method="ffill").fillna(res)
            total = total + shares_per_market * mtm
        # Re-base to initial_cash so the curve is comparable to equity_curve
        if cost == 0:
            return pd.Series(dtype=float)
        return total + (self.initial_cash - cost)

    # ------------------------------------------------------------------
    # Per-symbol breakdown
    # ------------------------------------------------------------------

    def summary_by_symbol(self) -> pd.DataFrame:
        rows = []
        for s, curve in self.equity_by_symbol.items():
            realized = self.realized_pnl_by_symbol.get(s, 0.0)
            total = float(curve.iloc[-1]) if len(curve) else 0.0
            rows.append({
                "symbol":         s,
                "total_pnl":      round(total, 4),
                "realized_pnl":   round(realized, 4),
                "unrealized_pnl": round(total - realized, 4),
                "resolution":     self.resolutions.get(s, float("nan")),
            })
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Aggregate summary
    # ------------------------------------------------------------------

    def summary(self) -> dict:
        max_dd_dur = self.max_drawdown_duration
        avg_hold = self.avg_hold_time
        return {
            "symbol":               self.symbol,
            "initial_cash":         self.initial_cash,
            "final_equity":         round(float(self.equity_curve.iloc[-1]), 4) if not self.equity_curve.empty else self.initial_cash,
            "total_pnl":            round(self.total_pnl, 4),
            "realized_pnl":         round(self.total_realized_pnl, 4),
            "unrealized_pnl":       round(self.total_unrealized_pnl, 4),
            "gross_pnl":            round(self.gross_pnl, 4),
            "total_fees":           round(self.total_fees, 6),
            "annualized_sharpe":    round(self.sharpe, 4) if not math.isnan(self.sharpe) else float("nan"),
            "annualized_vol":       round(self.annualized_volatility, 4) if not math.isnan(self.annualized_volatility) else float("nan"),
            "max_drawdown":         round(self.max_drawdown, 4) if not math.isnan(self.max_drawdown) else float("nan"),
            "max_drawdown_pct":     round(self.max_drawdown_pct, 4) if not math.isnan(self.max_drawdown_pct) else float("nan"),
            "max_drawdown_duration": str(max_dd_dur) if max_dd_dur is not None else None,
            "num_fills":            len(self.fills),
            "num_round_trips":      self.num_round_trips,
            "win_rate":             round(self.win_rate, 4) if not math.isnan(self.win_rate) else float("nan"),
            "profit_factor":        round(self.profit_factor, 4) if not (math.isnan(self.profit_factor) or math.isinf(self.profit_factor)) else self.profit_factor,
            "avg_win":              round(self.avg_win, 4) if not math.isnan(self.avg_win) else float("nan"),
            "avg_loss":             round(self.avg_loss, 4) if not math.isnan(self.avg_loss) else float("nan"),
            "avg_hold_time":        str(avg_hold) if avg_hold is not None else None,
            "turnover":             round(self.turnover, 4) if not math.isnan(self.turnover) else float("nan"),
        }

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-friendly dict capturing run config + key results."""
        return {
            "run_config": dict(self.run_config),
            "symbol": self.symbol,
            "initial_cash": self.initial_cash,
            "resolutions": dict(self.resolutions),
            "equity_curve": {str(ts): float(v) for ts, v in self.equity_curve.items()},
            "equity_by_symbol": {
                s: {str(ts): float(v) for ts, v in ser.items()}
                for s, ser in self.equity_by_symbol.items()
            },
            "realized_pnl_by_symbol": dict(self.realized_pnl_by_symbol),
            "round_trips": list(self.round_trips),
            "fills": [
                {"symbol": f.symbol, "price": f.price, "quantity": f.quantity,
                 "buyer": f.buyer, "seller": f.seller, "timestamp": f.timestamp}
                for f in self.fills
            ],
            "total_taker_fees": self.total_taker_fees,
            "total_maker_rebates": self.total_maker_rebates,
            "summary": self.summary(),
        }

    def to_json(self, **kwargs) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ------------------------------------------------------------------
    # Visualizer
    # ------------------------------------------------------------------

    def plot(self, show_benchmark: bool = True) -> None:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        # Decide layout: single-symbol gets price+pnl+drawdown; multi gets per-symbol equity rows.
        single = len(self.equity_by_symbol) <= 1

        if single:
            fig, axes = plt.subplots(
                3, 1, figsize=(13, 9), sharex=True,
                gridspec_kw={"height_ratios": [2, 1, 1]},
            )
            ax_price, ax_pnl, ax_dd = axes
        else:
            n = len(self.equity_by_symbol)
            fig, axes = plt.subplots(
                n + 2, 1, figsize=(13, 4 + 2 * (n + 1)), sharex=True,
                gridspec_kw={"height_ratios": [2] + [1] * n + [1]},
            )
            ax_pnl = axes[0]
            per_symbol_axes = list(axes[1:1 + n])
            ax_dd = axes[-1]
            ax_price = None

        fig.suptitle(f"Backtest: {self.symbol}", fontsize=13, fontweight="bold")

        # --- Single-symbol price panel -------------------------------------
        if ax_price is not None:
            if self.bars is not None and "close" in self.bars.columns:
                close = self.bars["close"].dropna()
                ax_price.plot(close.index, close.values, color="steelblue", linewidth=1.2, label="Close")
            self._plot_fill_markers(ax_price)
            if not math.isnan(self.resolution):
                ax_price.axhline(self.resolution, color="orange", linestyle="--",
                                 linewidth=0.9, label=f"Resolution ({self.resolution})")
            ax_price.set_ylabel("Price")
            ax_price.grid(True, alpha=0.3)
            ax_price.legend(loc="upper left", fontsize=9)

        # --- Aggregate cumulative PnL --------------------------------------
        pnl = self.pnl_curve
        ax_pnl.plot(pnl.index, pnl.values, color="darkorange", linewidth=1.5, label="Strategy")
        ax_pnl.axhline(0, color="black", linewidth=0.8, linestyle="--")
        ax_pnl.fill_between(pnl.index, pnl.values, 0,
                            where=(pnl.values >= 0), alpha=0.2, color="green")
        ax_pnl.fill_between(pnl.index, pnl.values, 0,
                            where=(pnl.values < 0), alpha=0.2, color="red")
        if show_benchmark:
            bh = self.buy_and_hold_curve()
            if not bh.empty:
                bh_pnl = bh - self.initial_cash
                ax_pnl.plot(bh_pnl.index, bh_pnl.values, color="grey",
                            linewidth=1.0, linestyle=":", label="Buy & Hold")

        ax_pnl.set_ylabel("Cumulative PnL ($)")
        ax_pnl.grid(True, alpha=0.3)
        s = self.summary()
        title = (
            f"PnL: ${s['total_pnl']:+.2f}  |  "
            f"Fees: ${s['total_fees']:+.2f}  |  "
            f"Sharpe(ann): {s['annualized_sharpe']}  |  "
            f"MaxDD: ${s['max_drawdown']:+.2f}  |  "
            f"Win: {s['win_rate']}  |  "
            f"Fills: {s['num_fills']}"
        )
        ax_pnl.set_title(title, fontsize=9, loc="left", pad=4)
        ax_pnl.legend(loc="upper left", fontsize=9)

        # --- Multi-symbol per-market rows ----------------------------------
        if not single:
            for ax_s, (symbol, curve) in zip(per_symbol_axes, self.equity_by_symbol.items()):
                ax_s.plot(curve.index, curve.values, linewidth=1.2)
                ax_s.axhline(0, color="black", linewidth=0.6, linestyle="--")
                ax_s.set_ylabel(f"{symbol}\nPnL contrib")
                ax_s.grid(True, alpha=0.3)

        # --- Drawdown panel ------------------------------------------------
        uw = self.underwater_curve
        if not uw.empty:
            ax_dd.fill_between(uw.index, uw.values, 0, color="red", alpha=0.3)
            ax_dd.plot(uw.index, uw.values, color="darkred", linewidth=0.8)
        ax_dd.axhline(0, color="black", linewidth=0.6, linestyle="--")
        ax_dd.set_ylabel("Drawdown ($)")
        ax_dd.set_xlabel("Time (UTC)")
        ax_dd.grid(True, alpha=0.3)

        ax_dd.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        fig.autofmt_xdate(rotation=30)
        plt.tight_layout()
        plt.show()

    def _plot_fill_markers(self, ax) -> None:
        if not self.fills:
            return
        # Color buys/sells by round-trip outcome where possible
        # For each fill, look up if it's been closed: buy→matches an entry_ts in round_trips; sell→matches an exit_ts
        round_trip_pnl_by_entry = {}
        round_trip_pnl_by_exit = {}
        for r in self.round_trips:
            round_trip_pnl_by_entry.setdefault(r["entry_ts"], []).append(r["pnl"])
            round_trip_pnl_by_exit.setdefault(r["exit_ts"], []).append(r["pnl"])

        def color_for(fill: Trade) -> str:
            if fill.quantity > 0:
                pnls = round_trip_pnl_by_entry.get(fill.timestamp, [])
            else:
                pnls = round_trip_pnl_by_exit.get(fill.timestamp, [])
            if not pnls:
                return "grey"  # no closing yet
            net = sum(pnls)
            return "green" if net > 0 else ("red" if net < 0 else "grey")

        ts_list = pd.to_datetime([f.timestamp for f in self.fills], unit="ms", utc=True)
        for f, ts in zip(self.fills, ts_list):
            marker = "^" if f.quantity > 0 else "v"
            ax.scatter([ts], [f.price], marker=marker, color=color_for(f), s=90, zorder=5,
                       edgecolors="black", linewidths=0.5)
