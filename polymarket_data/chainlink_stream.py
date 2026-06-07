"""Chainlink Data Streams WebSocket client."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, AsyncIterator


DEFAULT_WS_HOST = "ws.testnet-dataengine.chain.link"
DEFAULT_WS_PATH = "/api/v1/ws"
BTC_USD_PRODUCT_NAME = "BTC/USD-RefPrice-DS-Premium-Global-003"


@dataclass
class ChainlinkStreamReport:
    """Normalized Chainlink Data Streams report.

    Data Streams websocket payloads always include the signed report metadata.
    Some integrations also include decoded price fields. When only a signed
    fullReport blob is present, price remains None unless the blob is already
    the raw schema-v3 ABI payload.
    """

    feed_id: str
    valid_from_timestamp: int | None
    observations_timestamp: int | None
    full_report: str | None
    price: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    raw: dict[str, Any] | None = None


def _empty_body_hash() -> str:
    return hashlib.sha256(b"").hexdigest()


def generate_streams_signature(
    *,
    method: str,
    full_path: str,
    api_key: str,
    api_secret: str,
    timestamp_ms: int | None = None,
) -> tuple[str, int]:
    """Generate the HMAC signature required by Chainlink Data Streams."""

    timestamp = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    string_to_sign = (
        f"{method.upper()} {full_path} {_empty_body_hash()} {api_key} {timestamp}"
    )
    signature = hmac.new(
        api_secret.encode(),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    return signature, timestamp


def build_streams_auth_headers(
    *,
    method: str,
    full_path: str,
    api_key: str,
    api_secret: str,
) -> dict[str, str]:
    signature, timestamp = generate_streams_signature(
        method=method,
        full_path=full_path,
        api_key=api_key,
        api_secret=api_secret,
    )
    return {
        "Authorization": api_key,
        "X-Authorization-Timestamp": str(timestamp),
        "X-Authorization-Signature-SHA256": signature,
    }


def decode_schema_v3_payload(payload_hex: str, decimals: int = 18) -> dict[str, Any]:
    """Decode a raw Crypto Advanced v3 ABI payload.

    This expects exactly the v3 report payload fields, not a signed wrapper:
    feedId, validFromTimestamp, observationsTimestamp, nativeFee, linkFee,
    expiresAt, price, bid, ask.
    """

    data = payload_hex[2:] if payload_hex.startswith("0x") else payload_hex
    if len(data) != 9 * 64:
        raise ValueError(
            "schema-v3 payload must be exactly 9 ABI words; got "
            f"{len(data) // 2} bytes"
        )

    words = [data[i : i + 64] for i in range(0, len(data), 64)]

    def uint(idx: int) -> int:
        return int(words[idx], 16)

    def int192(idx: int) -> int:
        value = int(words[idx], 16)
        if value >= 1 << 191:
            value -= 1 << 192
        return value

    scale = Decimal(10) ** -decimals
    return {
        "feed_id": "0x" + words[0],
        "valid_from_timestamp": uint(1),
        "observations_timestamp": uint(2),
        "native_fee": uint(3),
        "link_fee": uint(4),
        "expires_at": uint(5),
        "price": Decimal(int192(6)) * scale,
        "bid": Decimal(int192(7)) * scale,
        "ask": Decimal(int192(8)) * scale,
    }


def _decimal_field(report: dict[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = report.get(key)
        if value is not None:
            return Decimal(str(value))
    return None


def normalize_stream_report(message: dict[str, Any], decimals: int = 18) -> ChainlinkStreamReport:
    report = message.get("report", message)
    if not isinstance(report, dict):
        raise ValueError(f"unexpected Chainlink stream message: {message!r}")

    full_report = report.get("fullReport") or report.get("full_report")
    decoded: dict[str, Any] = {}
    if full_report:
        try:
            decoded = decode_schema_v3_payload(full_report, decimals=decimals)
        except ValueError:
            decoded = {}

    feed_id = (
        report.get("feedID")
        or report.get("feedId")
        or report.get("feed_id")
        or decoded.get("feed_id")
    )
    if not feed_id:
        raise ValueError(f"missing feed id in Chainlink stream message: {message!r}")

    return ChainlinkStreamReport(
        feed_id=str(feed_id),
        valid_from_timestamp=report.get("validFromTimestamp")
        or report.get("valid_from_timestamp")
        or decoded.get("valid_from_timestamp"),
        observations_timestamp=report.get("observationsTimestamp")
        or report.get("observations_timestamp")
        or decoded.get("observations_timestamp"),
        full_report=full_report,
        price=_decimal_field(report, "price", "benchmarkPrice", "midPrice")
        or decoded.get("price"),
        bid=_decimal_field(report, "bid", "bidPrice") or decoded.get("bid"),
        ask=_decimal_field(report, "ask", "askPrice") or decoded.get("ask"),
        raw=message,
    )


class ChainlinkDataStreamsClient:
    """Small async client for Chainlink Data Streams WebSocket reports."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        ws_host: str | None = None,
        ws_path: str = DEFAULT_WS_PATH,
        decimals: int = 18,
    ) -> None:
        self.api_key = api_key or os.environ.get("STREAMS_API_KEY")
        self.api_secret = api_secret or os.environ.get("STREAMS_API_SECRET")
        self.ws_host = ws_host or os.environ.get("STREAMS_WS_HOST", DEFAULT_WS_HOST)
        self.ws_path = ws_path
        self.decimals = decimals

        if not self.api_key or not self.api_secret:
            raise ValueError(
                "Chainlink Data Streams credentials are required. Set "
                "STREAMS_API_KEY and STREAMS_API_SECRET."
            )

    async def subscribe(self, feed_ids: list[str]) -> AsyncIterator[ChainlinkStreamReport]:
        if not feed_ids:
            raise ValueError("at least one feed ID is required")

        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError(
                'Install websocket support with: pip install "polymarket-data[chainlink]"'
            ) from exc

        query = ",".join(feed_ids)
        full_path = f"{self.ws_path}?feedIDs={query}"
        headers = build_streams_auth_headers(
            method="GET",
            full_path=full_path,
            api_key=self.api_key,
            api_secret=self.api_secret,
        )
        url = f"wss://{self.ws_host}{full_path}"

        connect_kwargs = {
            "ping_interval": 5,
            "ping_timeout": 10,
            "open_timeout": 30,
        }
        try:
            websocket_context = websockets.connect(
                url,
                additional_headers=headers,
                **connect_kwargs,
            )
        except TypeError:
            websocket_context = websockets.connect(
                url,
                extra_headers=headers,
                **connect_kwargs,
            )

        async with websocket_context as websocket:
            async for payload in websocket:
                message = json.loads(payload)
                yield normalize_stream_report(message, decimals=self.decimals)


async def print_reports(
    *,
    feed_id: str,
    api_key: str | None = None,
    api_secret: str | None = None,
    ws_host: str | None = None,
    decimals: int = 18,
    limit: int | None = None,
) -> None:
    client = ChainlinkDataStreamsClient(
        api_key=api_key,
        api_secret=api_secret,
        ws_host=ws_host,
        decimals=decimals,
    )

    count = 0
    async for report in client.subscribe([feed_id]):
        count += 1
        print(
            json.dumps(
                {
                    "feed_id": report.feed_id,
                    "observations_timestamp": report.observations_timestamp,
                    "valid_from_timestamp": report.valid_from_timestamp,
                    "price": str(report.price) if report.price is not None else None,
                    "bid": str(report.bid) if report.bid is not None else None,
                    "ask": str(report.ask) if report.ask is not None else None,
                    "has_full_report": bool(report.full_report),
                }
            ),
            flush=True,
        )
        if limit is not None and count >= limit:
            break


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Stream Chainlink BTC/USD reports.")
    parser.add_argument(
        "--feed-id",
        default=os.environ.get("CHAINLINK_BTC_USD_FEED_ID"),
        help="Full BTC/USD Data Streams feed ID. Can also use CHAINLINK_BTC_USD_FEED_ID.",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("STREAMS_WS_HOST", DEFAULT_WS_HOST),
        help="Data Streams WebSocket host. Defaults to STREAMS_WS_HOST or Chainlink testnet docs host.",
    )
    parser.add_argument("--decimals", type=int, default=18)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if not args.feed_id:
        raise SystemExit(
            "Missing --feed-id. Polymarket points to "
            f"{BTC_USD_PRODUCT_NAME}, but Chainlink displays the public ID "
            "truncated on data.chain.link. Set the full feed ID from your "
            "Chainlink Data Streams access as CHAINLINK_BTC_USD_FEED_ID."
        )

    asyncio.run(
        print_reports(
            feed_id=args.feed_id,
            ws_host=args.host,
            decimals=args.decimals,
            limit=args.limit,
        )
    )


if __name__ == "__main__":
    main()
