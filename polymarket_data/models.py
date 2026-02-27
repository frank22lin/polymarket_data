from dataclasses import dataclass


@dataclass
class MarketInfo:
    slug: str
    condition_id: str
    question: str
    outcomes: list[str]
    token_ids: list[str]  # one per outcome, same order as outcomes
