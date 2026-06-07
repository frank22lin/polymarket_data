"""Gamma API client — resolves a market slug to token IDs and metadata."""

import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from typing import Optional
from .models import MarketInfo

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
_UTC = timezone.utc


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

    def get_markets_by_event_slugs(
        self,
        slugs: list[str],
        batch_size: int = 90,
        sleep: float = 0.2,
    ) -> list[MarketInfo]:
        """Batch-fetch markets for multiple event slugs.

        The Gamma API accepts repeated ``slug=`` query params and caps results
        at 100 per request.  ``batch_size`` must be ≤ 100; default 90 is safe.
        Non-existent slugs are silently skipped by the API.
        """
        results: list[MarketInfo] = []
        for i in range(0, len(slugs), batch_size):
            batch = slugs[i : i + batch_size]
            qs = "&".join(f"slug={s}" for s in batch) + f"&limit={len(batch)}"
            data = self._get(f"{GAMMA_API_BASE}/events?{qs}")
            for event in data:
                for m in event.get("markets", []):
                    results.append(self._parse_market(m))
            if i + batch_size < len(slugs):
                time.sleep(sleep)
        return results

    def list_markets(
        self,
        tag_slug: str,
        closed: bool = True,
        limit_per_page: int = 500,
    ) -> list[MarketInfo]:
        """Return all markets matching tag_slug, fully paginated.

        For BTC 5-min Up/Down markets the tag_slug is typically
        ``"btc-updown-5m"``.  If results are empty, check what tag_slug
        Polymarket uses by inspecting a known market's ``tags`` field.
        """
        results: list[MarketInfo] = []
        offset = 0
        while True:
            params = urllib.parse.urlencode({
                "tag_slug": tag_slug,
                "closed":   "true" if closed else "false",
                "limit":    limit_per_page,
                "offset":   offset,
            })
            page = self._get(f"{GAMMA_API_BASE}/markets?{params}")
            if not page:
                break
            for m in page:
                results.append(self._parse_market(m))
            if len(page) < limit_per_page:
                break
            offset += limit_per_page
        return results

    @staticmethod
    def _parse_market(m: dict) -> MarketInfo:
        token_ids: list[str] = json.loads(m["clobTokenIds"])
        outcomes: list[str] = json.loads(m["outcomes"])

        # ── resolution_time: parse from endDate ──────────────────────────
        resolution_time: Optional[int] = None
        raw_end = m.get("endDate") or m.get("end_date_iso") or m.get("endDateIso")
        if raw_end:
            try:
                dt = datetime.fromisoformat(str(raw_end).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_UTC)
                resolution_time = int(dt.timestamp())
            except Exception:
                pass

        # ── resolution_value: outcome-0 settlement price (0.0 or 1.0) ───
        resolution_value: Optional[float] = None
        raw_prices = m.get("outcomePrices")
        if raw_prices:
            try:
                prices = json.loads(raw_prices)
                val = float(prices[0])
                if val in (0.0, 1.0):
                    resolution_value = val
            except Exception:
                pass

        return MarketInfo(
            slug=m["slug"],
            condition_id=m["conditionId"],
            question=m["question"],
            outcomes=outcomes,
            token_ids=token_ids,
            resolution_time=resolution_time,
            resolution_value=resolution_value,
        )