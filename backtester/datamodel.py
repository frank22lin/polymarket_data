from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List

Time = int      # unix timestamp in milliseconds
Symbol = str
Position = float


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
    quantity: float   # positive = bought, negative = sold
    buyer: str = ""   # "SUBMISSION" if own buy
    seller: str = ""  # "SUBMISSION" if own sell
    timestamp: Time = 0


@dataclass
class Order:
    symbol: Symbol
    price: float      # limit price in [0, 1]; use 1.0 / 0.0 for market orders
    quantity: float   # positive = buy shares, negative = sell shares


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
        observations: Dict[str, float],
    ):
        self.traderData = traderData
        self.timestamp = timestamp
        self.listings = listings
        self.bars = bars
        self.own_trades = own_trades
        self.market_trades = market_trades
        self.position = position
        self.observations = observations

    def toJSON(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)
