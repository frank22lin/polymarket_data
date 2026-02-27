"""Gamma API client — resolves a market slug to token IDs and metadata."""

import json
import urllib.request
import urllib.parse
from typing import Optional
from .models import MarketInfo

GAMMA_API_BASE = "https://gamma-api.polymarket.com"


class GammaClient:
    def get_market_by_slug(self, slug: str) -> Optional[MarketInfo]:
        """Return MarketInfo for the given slug, or None if not found."""
        params = urllib.parse.urlencode({"slug": slug, "limit": 1})
        url = f"{GAMMA_API_BASE}/markets?{params}"

        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "polymarket-data/0.1",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        if not data:
            return None

        m = data[0]
        token_ids: list[str] = json.loads(m["clobTokenIds"])
        outcomes: list[str] = json.loads(m["outcomes"])

        return MarketInfo(
            slug=m["slug"],
            condition_id=m["conditionId"],
            question=m["question"],
            outcomes=outcomes,
            token_ids=token_ids,
        )
