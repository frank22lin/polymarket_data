"""Gamma API client — resolves a market slug to token IDs and metadata."""

import json
import urllib.request
import urllib.parse
from typing import Optional
from .models import MarketInfo

GAMMA_API_BASE = "https://gamma-api.polymarket.com"


class GammaClient:
    def _get(self, url: str) -> list:
        req = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "polymarket-data/0.1"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def get_market_by_slug(self, slug: str) -> Optional[MarketInfo]:
        """Return MarketInfo for the given market slug, or None if not found."""
        params = urllib.parse.urlencode({"slug": slug, "limit": 1})
        data = self._get(f"{GAMMA_API_BASE}/markets?{params}")
        if not data:
            # Closed/resolved markets are excluded by default — retry with closed=true.
            params = urllib.parse.urlencode({"slug": slug, "limit": 1, "closed": "true"})
            data = self._get(f"{GAMMA_API_BASE}/markets?{params}")
        if not data:
            return None
        return self._parse_market(data[0])

    def get_markets_by_event_slug(self, event_slug: str) -> list[MarketInfo]:
        """Return all MarketInfo objects belonging to an event slug."""
        params = urllib.parse.urlencode({"slug": event_slug, "limit": 1})
        data = self._get(f"{GAMMA_API_BASE}/events?{params}")
        if not data:
            return []
        return [self._parse_market(m) for m in data[0].get("markets", [])]

    @staticmethod
    def _parse_market(m: dict) -> MarketInfo:
        token_ids: list[str] = json.loads(m["clobTokenIds"])
        outcomes: list[str] = json.loads(m["outcomes"])
        return MarketInfo(
            slug=m["slug"],
            condition_id=m["conditionId"],
            question=m["question"],
            outcomes=outcomes,
            token_ids=token_ids,
        )