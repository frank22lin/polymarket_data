from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MarketInfo:
    slug: str
    condition_id: str
    question: str
    outcomes: list[str]
    token_ids: list[str]           # one per outcome, same order as outcomes
    resolution_time: Optional[int] = None   # unix seconds; end of market window
    resolution_value: Optional[float] = None  # 1.0 / 0.0 for outcome-0 win/loss
