from __future__ import annotations

from typing import Optional

from .datamodel import OpenOrder


def match_trade_to_order(order: OpenOrder, trade_price: float, trade_side: str) -> Optional[float]:
    """Return the fill price if this trade matches the resting order, else None.

    Optimistic at-or-through semantics:

    * Buy limit at price p fills the first time a SELL-side trade prints
      at price <= p (someone hit the bid).
    * Sell limit at price p fills the first time a BUY-side trade prints
      at price >= p (someone lifted the offer).

    Marketable orders (buy at p>=1.0, sell at p<=0.0) collapse to "fill at
    next opposing-side trade's price." Caller is responsible for separating
    marketable orders from resting ones (they fill against the other side).

    For resting limits the fill price equals the limit (you got your level),
    not the trade price, since the trade price is what the *other* side
    paid. Marketable fills use the trade price.
    """
    is_buy = order.quantity > 0
    if is_buy:
        # Resting buy: fills when SELL-side trade prints at-or-through limit
        if trade_side == "SELL" and trade_price <= order.price:
            return order.price
        return None
    else:
        # Resting sell: fills when BUY-side trade prints at-or-through limit
        if trade_side == "BUY" and trade_price >= order.price:
            return order.price
        return None


def is_marketable(order_price: float, is_buy: bool) -> bool:
    """An order is marketable if its limit guarantees it crosses the spread.

    Buys at price >= 1.0 and sells at price <= 0.0 are treated as market
    orders (they consume the next opposing-side trade).
    """
    return order_price >= 1.0 if is_buy else order_price <= 0.0
