from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List

import pandas as pd

from .datamodel import Trade


@dataclass
class BacktestResult:
    symbol: str
    bars: pd.DataFrame        # raw OHLCV bars — used for the price panel
    fills: List[Trade]
    resolution: float
    equity_curve: pd.Series   # index=timestamp (UTC), values=total equity in $
    initial_cash: float

    # ------------------------------------------------------------------
    # Core metrics
    # ------------------------------------------------------------------

    @property
    def pnl_curve(self) -> pd.Series:
        """Cumulative P&L (equity minus starting cash)."""
        return self.equity_curve - self.initial_cash

    @property
    def total_pnl(self) -> float:
        return float(self.equity_curve.iloc[-1] - self.initial_cash)

    @property
    def volatility(self) -> float:
        """Std dev of bar-level equity changes."""
        returns = self.equity_curve.diff().dropna()
        return float(returns.std())

    @property
    def sharpe(self) -> float:
        """Bar-level Sharpe: mean(Δequity) / std(Δequity)."""
        returns = self.equity_curve.diff().dropna()
        std = returns.std()
        if std == 0 or math.isnan(std):
            return float("nan")
        return float(returns.mean() / std)

    def summary(self) -> dict:
        return {
            "symbol":        self.symbol,
            "resolution":    self.resolution,
            "initial_cash":  self.initial_cash,
            "final_equity":  round(float(self.equity_curve.iloc[-1]), 4),
            "total_pnl":     round(self.total_pnl, 4),
            "volatility":    round(self.volatility, 4),
            "sharpe":        round(self.sharpe, 4),
            "num_fills":     len(self.fills),
        }

    # ------------------------------------------------------------------
    # Visualizer
    # ------------------------------------------------------------------

    def plot(self) -> None:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(13, 7), sharex=True,
            gridspec_kw={"height_ratios": [2, 1]},
        )
        fig.suptitle(f"Backtest: {self.symbol}", fontsize=13, fontweight="bold")

        # --- Panel 1: market close price + fill markers ---
        close = self.bars["close"].dropna()
        ax1.plot(close.index, close.values, color="steelblue", linewidth=1.2, label="Close")

        if self.fills:
            fill_ts = pd.to_datetime(
                [f.timestamp for f in self.fills], unit="ms", utc=True
            )
            fill_prices = [f.price for f in self.fills]
            fill_qtys   = [f.quantity for f in self.fills]

            buys  = [(t, p) for t, p, q in zip(fill_ts, fill_prices, fill_qtys) if q > 0]
            sells = [(t, p) for t, p, q in zip(fill_ts, fill_prices, fill_qtys) if q < 0]

            if buys:
                bts, bps = zip(*buys)
                ax1.scatter(bts, bps, marker="^", color="green", s=90,
                            zorder=5, label="Buy")
            if sells:
                sts, sps = zip(*sells)
                ax1.scatter(sts, sps, marker="v", color="red", s=90,
                            zorder=5, label="Sell")

        ax1.set_ylabel("Price")
        ax1.legend(loc="upper left", fontsize=9)
        ax1.grid(True, alpha=0.3)

        # Resolution line
        ax1.axhline(self.resolution, color="orange", linestyle="--",
                    linewidth=0.9, label=f"Resolution ({self.resolution})")
        ax1.legend(loc="upper left", fontsize=9)

        # --- Panel 2: cumulative P&L ---
        pnl = self.pnl_curve
        ax2.plot(pnl.index, pnl.values, color="darkorange", linewidth=1.5)
        ax2.axhline(0, color="black", linewidth=0.8, linestyle="--")
        ax2.fill_between(pnl.index, pnl.values, 0,
                         where=(pnl.values >= 0), alpha=0.2, color="green", label="Profit")
        ax2.fill_between(pnl.index, pnl.values, 0,
                         where=(pnl.values < 0), alpha=0.2, color="red", label="Loss")

        metrics_text = (
            f"PnL: ${self.total_pnl:+.2f}  |  "
            f"Vol: {self.volatility:.4f}  |  "
            f"Sharpe: {self.sharpe:.3f}  |  "
            f"Fills: {len(self.fills)}"
        )
        ax2.set_title(metrics_text, fontsize=9, loc="left", pad=4)
        ax2.set_ylabel("Cumulative PnL ($)")
        ax2.set_xlabel("Time (UTC)")
        ax2.grid(True, alpha=0.3)

        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        fig.autofmt_xdate(rotation=30)
        plt.tight_layout()
        plt.show()
