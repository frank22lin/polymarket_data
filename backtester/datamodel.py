from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

Time = int      # unix timestamp in milliseconds
Symbol = str
Position = float
OrderId = int


@dataclass
class Bar:
    symbol: Symbol
    timestamp: Time
    open: float
    high: float
    low: float
    close: float
    vwap: float
    volume: float
    trade_count: int


@dataclass
class Trade:
    symbol: Symbol
    price: float
    quantity: float   # signed: positive = taker bought (BUY-side), negative = taker sold (SELL-side)
    buyer: str = ""   # "SUBMISSION" if our buy
    seller: str = ""  # "SUBMISSION" if our sell
    timestamp: Time = 0


@dataclass
class Order:
    symbol: Symbol
    price: float      # limit price in [0, 1]; use 1.0 for marketable buy, 0.0 for marketable sell
    quantity: float   # signed: positive = buy shares, negative = sell shares


@dataclass
class OpenOrder:
    """An order resting on the book, with engine-assigned id."""
    order_id: OrderId
    symbol: Symbol
    price: float
    quantity: float
    submitted_ts: Time   # when the engine first received it
    remaining: float     # unsigned size still working


@dataclass
class Cancel:
    """Trader instruction to cancel a resting order by id."""
    order_id: OrderId


@dataclass
class Listing:
    symbol: Symbol
    question: str
    outcomes: List[str]
    resolution_time: Time


class TradingState:
    def __init__(
        self,
        traderData: str,
        timestamp: Time,
        listings: Dict[Symbol, Listing],
        bars: Dict[Symbol, Bar],
        own_trades: Dict[Symbol, List[Trade]],
        market_trades: Dict[Symbol, List[Trade]],
        position: Dict[Symbol, Position],
        open_orders: Dict[Symbol, List[OpenOrder]],
        cash: float,
        observations: Dict[str, float],
        last_price: Optional[Dict[Symbol, float]] = None,
    ):
        self.traderData = traderData
        self.timestamp = timestamp
        self.listings = listings
        self.bars = bars
        self.own_trades = own_trades
        self.market_trades = market_trades
        self.position = position
        self.open_orders = open_orders
        self.cash = cash
        self.observations = observations
        self.last_price = last_price if last_price is not None else {}

    def toJSON(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)
