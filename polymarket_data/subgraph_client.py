"""Goldsky subgraph client for Polymarket order-book data."""

import json
import urllib.request
from typing import Any

SUBGRAPH_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/"
    "subgraphs/orderbook-subgraph/0.0.1/gn"
)

# Subgraph hard-limit per request
PAGE_SIZE = 1000


class SubgraphClient:
    def __init__(self, url: str = SUBGRAPH_URL) -> None:
        self.url = url

    def _run(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        payload = json.dumps({"query": query, "variables": variables}).encode()
        req = urllib.request.Request(
            self.url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "polymarket-data/0.1",
            },
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read())

        if "errors" in result:
            raise RuntimeError(f"GraphQL error: {result['errors']}")
        return result["data"]

    # ------------------------------------------------------------------
    # orderFilledEvents — paginated, cursor-based on id
    # ------------------------------------------------------------------

    # We pass all filter values as flat variables so the where clause is
    # fully inlined — GraphQL does not allow a variable object to appear
    # as an element inside a literal list argument.
    # The Goldsky subgraph does NOT allow mixing column-level filters (id_gt,
    # timestamp_gte …) with an `or` at the same level.  We must push every
    # shared filter into each branch of the `or` instead.
    _ORDER_FILLED_QUERY = """
    query OrderFilled(
        $tokenIds: [String!]!
        $startTs: BigInt!
        $endTs: BigInt!
        $lastId: ID!
        $first: Int!
    ) {
        orderFilledEvents(
            first: $first
            orderBy: id
            orderDirection: asc
            where: {
                or: [
                    {
                        id_gt: $lastId
                        timestamp_gte: $startTs
                        timestamp_lte: $endTs
                        makerAssetId_in: $tokenIds
                    }
                    {
                        id_gt: $lastId
                        timestamp_gte: $startTs
                        timestamp_lte: $endTs
                        takerAssetId_in: $tokenIds
                    }
                ]
            }
        ) {
            id
            transactionHash
            timestamp
            maker
            taker
            makerAssetId
            takerAssetId
            makerAmountFilled
            takerAmountFilled
            fee
        }
    }
    """

    def fetch_order_filled_events(
        self,
        token_ids: list[str],
        start_ts: int,
        end_ts: int,
    ) -> list[dict[str, Any]]:
        """
        Fetch ALL OrderFilledEvents for the given token IDs within [start_ts, end_ts].

        Paginates automatically using id-based cursor pagination.
        token_ids — list of outcome token IDs (e.g. [YES_id, NO_id])
        start_ts / end_ts — Unix timestamps (seconds)
        """
        results: list[dict[str, Any]] = []
        last_id = ""

        while True:
            data = self._run(
                self._ORDER_FILLED_QUERY,
                {
                    "tokenIds": token_ids,
                    "startTs": str(start_ts),
                    "endTs": str(end_ts),
                    "lastId": last_id,
                    "first": PAGE_SIZE,
                },
            )
            page = data["orderFilledEvents"]
            results.extend(page)

            if len(page) < PAGE_SIZE:
                break
            last_id = page[-1]["id"]

        return results
