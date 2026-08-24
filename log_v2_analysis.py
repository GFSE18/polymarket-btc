#!/usr/bin/env python3
"""Analyze Polymarket trade-fill CSVs using explicit CLOB winners."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
REQUIRED_COLUMNS = {
    "market", "market_slug", "duration", "side", "outcome", "shares", "cash", "fee"
}
USER_AGENT = "polymarket-log-v2-analysis/1.0"


@dataclass(frozen=True)
class Resolution:
    status: str
    winner: str | None = None
    detail: str = ""


class ApiClient:
    def __init__(self, timeout: float = 10.0, retries: int = 2) -> None:
        self.timeout = timeout
        self.retries = retries

    def get_json(self, url: str) -> dict[str, Any]:
        request = Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    payload = json.load(response)
                if not isinstance(payload, dict):
                    raise ValueError("API returned a non-object JSON response")
                return payload
            except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(0.5 * (2**attempt))
        assert last_error is not None
        raise last_error


def normalized(value: Any) -> str:
    return str(value or "").strip().lower()


def select_exact_market(event: dict[str, Any], slug: str) -> dict[str, Any] | None:
    markets = event.get("markets")
    if not isinstance(markets, list):
        return None
    exact = [market for market in markets if isinstance(market, dict) and market.get("slug") == slug]
    if len(exact) == 1:
        return exact[0]
    if len(markets) == 1 and isinstance(markets[0], dict):
        return markets[0]
    return None


def resolve_market(slug: str, api: ApiClient) -> Resolution:
    """Return a winner only when the CLOB explicitly marks exactly one winning token."""
    try:
        event_url = f"{GAMMA_API}/events/slug/{quote(slug, safe='')}"
        event = api.get_json(event_url)
        market = select_exact_market(event, slug)
        if market is None:
            return Resolution("DATA_ERROR", detail="event did not contain one exact market")

        condition_id = market.get("conditionId") or market.get("condition_id")
        if not condition_id:
            return Resolution("DATA_ERROR", detail="Gamma response omitted conditionId")

        # getMarket(conditionId): public CLOB response includes closed and tokens[].winner.
        clob_url = f"{CLOB_API}/markets/{quote(str(condition_id), safe='')}"
        clob_market = api.get_json(clob_url)
        if clob_market.get("closed") is not True:
            return Resolution("UNRESOLVED", detail="CLOB market is not closed")

        tokens = clob_market.get("tokens")
        if not isinstance(tokens, list):
            return Resolution("DATA_ERROR", detail="CLOB response omitted tokens")
        winners = [token for token in tokens if isinstance(token, dict) and token.get("winner") is True]
        if len(winners) != 1:
            return Resolution("UNRESOLVED", detail=f"expected one winning token; found {len(winners)}")

        winner = str(winners[0].get("outcome") or "").strip()
        if not winner:
            return Resolution("DATA_ERROR", detail="winning token omitted outcome")
        return Resolution("RESOLVED", winner=winner)
    except Exception as exc:
        return Resolution("API_ERROR", detail=f"{type(exc).__name__}: {exc}")


def read_trades(filename: Path) -> list[dict[str, Any]]:
    with filename.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError("CSV is missing required columns: " + ", ".join(sorted(missing)))
        trades: list[dict[str, Any]] = []
        for line, row in enumerate(reader, start=2):
            try:
                row["shares"] = float(row["shares"])
                row["cash"] = float(row["cash"])
                row["fee"] = float(row["fee"])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid numeric value on CSV line {line}") from exc
            if row["shares"] < 0 or row["cash"] < 0 or row["fee"] < 0:
                raise ValueError(f"negative shares, cash, or fee on CSV line {line}")
            row["side"] = str(row["side"]).strip().upper()
            if row["side"] not in {"BUY", "SELL"}:
                raise ValueError(f"unsupported side {row['side']!r} on CSV line {line}")
            if normalized(row["outcome"]) not in {"up", "down"}:
                raise ValueError(f"unsupported outcome {row['outcome']!r} on CSV line {line}")
            trades.append(row)
    if not trades:
        raise ValueError("CSV contains no trades")
    return trades


def short_market_name(market: str, duration: str) -> str:
    time_part = market.rsplit(", ", 1)[-1].replace(" ET", "")
    return f"{time_part} {duration}"


def money(value: float) -> str:
    return f"${value:,.2f}"


def pnl_money(value: float) -> str:
    return f"+${value:,.2f}" if value > 0 else f"-${abs(value):,.2f}" if value < 0 else "$0.00"


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    widths = [len(header) for header in headers]
    for row in rows:
        widths = [max(width, len(cell)) for width, cell in zip(widths, row)]
    rule = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
    print(rule)
    print("|" + "|".join(f" {header:<{width}} " for header, width in zip(headers, widths)) + "|")
    print(rule)
    for row in rows:
        print("|" + "|".join(f" {cell:>{width}} " for cell, width in zip(row, widths)) + "|")
    print(rule)


def analyze(filename: Path, api: ApiClient) -> int:
    trades = read_trades(filename)
    groups: OrderedDict[tuple[str, str, str], list[dict[str, Any]]] = OrderedDict()
    for trade in trades:
        key = (str(trade["market"]), str(trade["market_slug"]), str(trade["duration"]))
        groups.setdefault(key, []).append(trade)

    print(f"Analyzing {len(trades)} fills across {len(groups)} markets...")
    results: list[dict[str, Any]] = []
    for (market, slug, duration), fills in groups.items():
        resolution = resolve_market(slug, api)
        if resolution.status in {"API_ERROR", "DATA_ERROR"}:
            print(f"WARNING [{resolution.status}] {slug}: {resolution.detail}", file=sys.stderr)

        positions = {"up": 0.0, "down": 0.0}
        buy_cash = sell_cash = fees = 0.0
        for fill in fills:
            outcome = normalized(fill["outcome"])
            sign = 1.0 if fill["side"] == "BUY" else -1.0
            positions[outcome] += sign * fill["shares"]
            if fill["side"] == "BUY":
                buy_cash += fill["cash"]
            else:
                sell_cash += fill["cash"]
            fees += fill["fee"]

        net_spend = buy_cash - sell_cash + fees
        payout = pnl = None
        if resolution.status == "RESOLVED":
            winner_key = normalized(resolution.winner)
            if winner_key not in positions:
                resolution = Resolution("DATA_ERROR", detail=f"unexpected winner {resolution.winner!r}")
            else:
                payout = positions[winner_key]
                pnl = payout - net_spend

        results.append({
            "name": short_market_name(market, duration), "up": positions["up"],
            "down": positions["down"], "buy_cash": buy_cash, "sell_cash": sell_cash,
            "fees": fees, "net_spend": net_spend, "resolution": resolution,
            "payout": payout, "pnl": pnl,
        })

    table_rows: list[list[str]] = []
    for row in results:
        resolution = row["resolution"]
        result_text = resolution.winner if resolution.status == "RESOLVED" else resolution.status
        table_rows.append([
            row["name"], f"{row['up']:,.3f}", f"{row['down']:,.3f}", money(row["buy_cash"]),
            money(row["sell_cash"]), money(row["fees"]), money(row["net_spend"]), result_text,
            money(row["payout"]) if row["payout"] is not None else "N/A",
            pnl_money(row["pnl"]) if row["pnl"] is not None else "N/A",
        ])
    print()
    print_table(
        ["Market", "Net Up", "Net Down", "Buy cash", "Sell cash", "Fees", "Net spend", "Result", "Payout", "P/L"],
        table_rows,
    )

    resolved = [row for row in results if row["pnl"] is not None]
    unresolved = [row for row in results if row["pnl"] is None]
    total_buy = sum(row["buy_cash"] for row in results)
    total_sell = sum(row["sell_cash"] for row in results)
    total_fees = sum(row["fees"] for row in results)
    total_spend = sum(row["net_spend"] for row in results)
    known_payout = sum(row["payout"] for row in resolved)
    known_pnl = sum(row["pnl"] for row in resolved)
    resolved_spend = sum(row["net_spend"] for row in resolved)

    print("\nSUMMARY")
    print("-" * 44)
    print(f"Buy cash:            {money(total_buy)}")
    print(f"Sell proceeds:       {money(total_sell)}")
    print(f"Fees:                {money(total_fees)}")
    print(f"Net spend:           {money(total_spend)}")
    print(f"Resolved markets:    {len(resolved)}/{len(results)}")
    print(f"Known payout:        {money(known_payout)}")
    print(f"Known P/L:           {pnl_money(known_pnl)}")
    if resolved and resolved_spend:
        print(f"Resolved-only ROI:   {known_pnl / resolved_spend * 100:+.2f}%")
    if unresolved:
        print("Final total P/L:     N/A (some markets lack a confirmed winner)")
    else:
        print(f"Final total P/L:     {pnl_money(known_pnl)}")
        if total_spend:
            print(f"Final ROI:           {known_pnl / total_spend * 100:+.2f}%")
    return 0 if not unresolved else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Polymarket trade logs using explicit CLOB winner flags.")
    parser.add_argument("csv_file", type=Path, help="trade-fill CSV to analyze")
    parser.add_argument("--timeout", type=float, default=10.0, help="API timeout in seconds (default: 10)")
    parser.add_argument("--retries", type=int, default=2, help="API retries after the first attempt (default: 2)")
    args = parser.parse_args()
    if args.timeout <= 0 or args.retries < 0:
        parser.error("--timeout must be positive and --retries cannot be negative")
    return args


def main() -> int:
    args = parse_args()
    try:
        return analyze(args.csv_file, ApiClient(args.timeout, args.retries))
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
