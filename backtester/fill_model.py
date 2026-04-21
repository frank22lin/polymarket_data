from __future__ import annotations

import math
from abc import ABC, abstractmethod
from typing import Optional

from .datamodel import Bar, Order, Trade


class FillModel(ABC):
    @abstractmethod
    def fill_price(self, next_bar: Bar) -> Optional[float]:
        """Return execution price from the next bar, or None if unfillable."""
        ...

    def try_fill(self, order: Order, next_bar: Bar) -> Optional[Trade]:
        price = self.fill_price(next_bar)
        if price is None or math.isnan(price):
            return None
        # Limit check: buy fills if market price <= limit; sell fills if market price >= limit
        if order.quantity > 0 and price > order.price:
            return None
        if order.quantity < 0 and price < order.price:
            return None
        return Trade(
            symbol=order.symbol,
            price=price,
            quantity=order.quantity,
            buyer="SUBMISSION" if order.quantity > 0 else "",
            seller="SUBMISSION" if order.quantity < 0 else "",
            timestamp=next_bar.timestamp,
        )


class NextBarOpen(FillModel):
    """Fill at the open of the next bar. Most conservative — default."""
    def fill_price(self, next_bar: Bar) -> Optional[float]:
        return next_bar.open


class NextBarVWAP(FillModel):
    """Fill at the VWAP of the next bar; falls back to open on empty bars."""
    def fill_price(self, next_bar: Bar) -> Optional[float]:
        v = next_bar.vwap
        return v if not math.isnan(v) else next_bar.open


class NextBarClose(FillModel):
    """Fill at the close of the next bar."""
    def fill_price(self, next_bar: Bar) -> Optional[float]:
        return next_bar.close
