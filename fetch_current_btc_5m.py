#!/usr/bin/env python3
"""Fetch and print the current Polymarket BTC five-minute Up/Down market."""

from __future__ import annotations

import json
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


GAMMA_API = "https://gamma-api.polymarket.com"
WINDOW_SECONDS = 5 * 60
TIMEOUT_SECONDS = 5
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


def find_current_market() -> dict[str, Any]:
    # The previous window is a narrow fallback if the new market has not been
    # published yet at a five-minute boundary.
    attempted: list[str] = []
    for windows_ago in (0, 1):
        slug = window_slug(windows_ago=windows_ago)
        attempted.append(slug)
        market = fetch_market(slug)
        if market is not None:
            return market
    raise LookupError("no current BTC 5-minute market found; tried " + ", ".join(attempted))


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
