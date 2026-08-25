#!/usr/bin/env python3
"""Fetch and print the current Polymarket BTC five-minute Up/Down market."""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


GAMMA_API = "https://gamma-api.polymarket.com"
WINDOW_SECONDS = 5 * 60
TIMEOUT_SECONDS = 5
PUBLICATION_RETRIES = 5
PUBLICATION_RETRY_DELAY_SECONDS = 0.5
USER_AGENT = "polymarket-current-btc-5m/1.0"


def window_slug(timestamp: float | None = None, windows_ago: int = 0) -> str:
    """Build the BTC 5m slug for a UTC Unix timestamp."""
    now = int(time.time() if timestamp is None else timestamp)
    start = now - (now % WINDOW_SECONDS) - (windows_ago * WINDOW_SECONDS)
    return f"btc-updown-5m-{start}"


def fetch_market(slug: str) -> dict[str, Any] | None:
    url = f"{GAMMA_API}/markets/slug/{quote(slug, safe='')}"
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            market = json.load(response)
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise

    if not isinstance(market, dict):
        raise ValueError("Gamma returned a non-object response")
    if market.get("slug") != slug:
        raise ValueError("Gamma returned a market with a different slug")
    return market


def _parse_json_list(value: Any, field_name: str) -> list[Any]:
    """Parse Gamma fields that may be returned as arrays or JSON strings."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"market {field_name} was not valid JSON") from exc
        if isinstance(parsed, list):
            return parsed
    raise ValueError(f"market {field_name} was not a list")


def _parse_timestamp(value: Any, field_name: str) -> float:
    """Parse a timezone-aware Gamma ISO-8601 timestamp as Unix seconds."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"market response omitted {field_name}")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"market {field_name} was not a valid ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError(f"market {field_name} did not include a timezone")
    return parsed.timestamp()


def validate_current_market(
    market: dict[str, Any],
    slug: str,
    now: float,
) -> float:
    """Validate a tradable BTC 5m market and return its expiry timestamp."""
    try:
        window_start = int(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"invalid BTC five-minute slug: {slug!r}") from exc

    if now < window_start:
        raise ValueError("market window has not started")

    end_timestamp = _parse_timestamp(
        market.get("endDate") or market.get("endDateIso"),
        "endDate",
    )
    if now >= end_timestamp:
        raise ValueError("market has expired")
    if market.get("active") is not True:
        raise ValueError("market is not active")
    if market.get("closed") is not False:
        raise ValueError("market is closed or its closed state is unknown")
    if market.get("enableOrderBook") is not True:
        raise ValueError("market does not have order-book trading enabled")

    outcomes = [
        str(outcome).strip().lower()
        for outcome in _parse_json_list(market.get("outcomes"), "outcomes")
    ]
    token_ids = [
        str(token_id).strip()
        for token_id in _parse_json_list(market.get("clobTokenIds"), "clobTokenIds")
    ]
    if len(outcomes) != 2 or set(outcomes) != {"up", "down"}:
        raise ValueError("market outcomes were not exactly Up and Down")
    if len(token_ids) != 2 or any(not token_id for token_id in token_ids):
        raise ValueError("market did not contain exactly two CLOB token IDs")
    if len(set(token_ids)) != 2:
        raise ValueError("market CLOB token IDs were not distinct")

    return end_timestamp


def find_current_market(
    retries: int = PUBLICATION_RETRIES,
    retry_delay: float = PUBLICATION_RETRY_DELAY_SECONDS,
) -> dict[str, Any]:
    """Return the open BTC market for the current UTC five-minute window.

    Gamma can publish a new window shortly after its epoch boundary. Recompute
    the slug for each attempt so a retry that crosses a boundary looks up the
    new window. Never fall back to an expired previous window.
    """
    if retries < 0:
        raise ValueError("retries cannot be negative")
    if retry_delay < 0:
        raise ValueError("retry_delay cannot be negative")

    attempted: list[str] = []
    last_problem = "market was not found"
    for attempt in range(retries + 1):
        now = time.time()
        slug = window_slug(timestamp=now)
        if not attempted or attempted[-1] != slug:
            attempted.append(slug)

        market = fetch_market(slug)
        if market is None:
            last_problem = "market was not found"
        else:
            try:
                validate_current_market(market, slug, now)
            except ValueError as exc:
                last_problem = str(exc)
            else:
                return market

        if attempt < retries:
            time.sleep(retry_delay)

    raise LookupError(
        "no tradable current BTC five-minute market found; "
        f"tried {', '.join(attempted)}; last problem: {last_problem}"
    )


def main() -> int:
    try:
        market = find_current_market()
        name = market.get("question") or market.get("title")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("market response did not contain a name")
        print(name.strip())
        return 0
    except (HTTPError, URLError, TimeoutError, OSError, ValueError, LookupError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
