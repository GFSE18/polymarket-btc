#!/usr/bin/env python3
"""Collect synchronized BTC 5-minute market quotes and Chainlink 60s TWAP.

The collector maintains the latest Up/Down top-of-book prices from the public
CLOB WebSocket and the latest BTC/USD 60-second TWAP from the public RTDS
WebSocket.  It snapshots that in-memory state four times per second, producing
exactly 1,200 indexed sample slots for every five-minute market.

Price-to-beat is fetched directly from the same Polymarket web endpoint used by
the market page.  It is not inferred from the opening TWAP.

Run:
    python btc_5m_twap_statistics.py

Stop with Ctrl+C.  The active transaction is committed and the daily SQLite
database is checkpointed before exit.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import websockets
from websockets.exceptions import ConnectionClosed

from fetch_current_btc_5m import (
    WINDOW_SECONDS,
    _parse_json_list,
    find_current_market,
)


# ============================================================
# SETTINGS
# ============================================================

SAMPLE_INTERVAL_SECONDS = 0.250
SAMPLES_PER_MARKET = int(WINDOW_SECONDS / SAMPLE_INTERVAL_SECONDS)
MAX_SAMPLE_LATENESS_SECONDS = 0.125
TWAP_MAX_RECEIPT_AGE_SECONDS = 10.0
DATABASE_COMMIT_EVERY_SAMPLES = 20

CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
TWAP_TOPIC = "crypto_prices_twap_sixty"
TWAP_SYMBOL = "btc/usd"
PRICE_TO_BEAT_URL = "https://polymarket.com/api/crypto/crypto-price"

DATA_DIR = Path(__file__).resolve().parent / "btc_5m_twap_statistics_data"
HTTP_TIMEOUT_SECONDS = 5
MARKET_RETRY_SECONDS = 0.5
PRICE_TO_BEAT_RETRY_SECONDS = 0.5
RECONNECT_MAX_SECONDS = 30
USER_AGENT = "polymarket-btc-5m-twap-statistics/1.0"


# ============================================================
# MARKET AND FEED STATE
# ============================================================


@dataclass(frozen=True)
class MarketContext:
    slug: str
    window_epoch: int
    window_datetime_utc: str
    token_ids: dict[str, str]

    @property
    def end_epoch(self) -> int:
        return self.window_epoch + WINDOW_SECONDS


@dataclass
class QuoteState:
    best_bid: float | None = None
    best_ask: float | None = None
    initialized: bool = False


@dataclass
class MarketBookState:
    token_to_outcome: dict[str, str]
    quotes: dict[str, QuoteState] = field(
        default_factory=lambda: {"Up": QuoteState(), "Down": QuoteState()}
    )
    connected: bool = False

    def reset_for_connection(self) -> None:
        self.connected = False
        self.quotes = {"Up": QuoteState(), "Down": QuoteState()}


@dataclass
class TwapState:
    price: float | None = None
    connected: bool = False
    received_monotonic: float | None = None

    def reset_for_connection(self) -> None:
        self.price = None
        self.connected = False
        self.received_monotonic = None

    def is_usable(self, now_monotonic: float) -> bool:
        return (
            self.connected
            and self.price is not None
            and self.received_monotonic is not None
            and now_monotonic - self.received_monotonic
            <= TWAP_MAX_RECEIPT_AGE_SECONDS
        )


def market_context(market: dict[str, Any]) -> MarketContext:
    slug = str(market.get("slug") or "")
    try:
        window_epoch = int(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"market had an invalid BTC five-minute slug: {slug!r}") from exc

    outcomes = [
        str(value).strip().lower()
        for value in _parse_json_list(market.get("outcomes"), "outcomes")
    ]
    token_ids = [
        str(value).strip()
        for value in _parse_json_list(market.get("clobTokenIds"), "clobTokenIds")
    ]
    if len(outcomes) != 2 or len(token_ids) != 2:
        raise ValueError("market did not provide two outcome/token pairs")

    outcome_tokens = {
        outcome.capitalize(): token_id
        for outcome, token_id in zip(outcomes, token_ids)
    }
    if set(outcome_tokens) != {"Up", "Down"}:
        raise ValueError("market outcomes were not exactly Up and Down")

    crypto_config = market.get("cryptoMarketConfig") or {}
    if not isinstance(crypto_config, dict):
        raise ValueError("market cryptoMarketConfig was not an object")
    if crypto_config.get("twapEnabled") is not True:
        raise ValueError("market did not have TWAP enabled")
    if crypto_config.get("twapLookbackSeconds") != 60:
        raise ValueError("market did not use a 60-second TWAP")

    window_datetime = datetime.fromtimestamp(window_epoch, timezone.utc)
    return MarketContext(
        slug=slug,
        window_epoch=window_epoch,
        window_datetime_utc=window_datetime.isoformat(timespec="seconds"),
        token_ids=outcome_tokens,
    )


# ============================================================
# PRICE PARSING
# ============================================================


def parse_contract_price(value: Any) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(price) or not 0.0 <= price <= 1.0:
        return None
    return price


def best_price(levels: Any, *, highest: bool) -> float | None:
    if not isinstance(levels, list):
        return None
    prices = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = parse_contract_price(level.get("price"))
        if price is not None:
            prices.append(price)
    if not prices:
        return None
    return max(prices) if highest else min(prices)


def apply_clob_event(state: MarketBookState, event: Any) -> None:
    """Apply a CLOB book/BBO event to the in-memory top-of-book state."""
    if isinstance(event, list):
        for item in event:
            apply_clob_event(state, item)
        return
    if not isinstance(event, dict):
        return

    event_type = event.get("event_type") or event.get("type")
    if event_type == "book":
        token_id = str(event.get("asset_id") or "")
        outcome = state.token_to_outcome.get(token_id)
        if outcome is None:
            return
        quote = state.quotes[outcome]
        quote.best_bid = best_price(event.get("bids"), highest=True)
        quote.best_ask = best_price(event.get("asks"), highest=False)
        quote.initialized = True
        return

    if event_type == "price_change":
        changes = event.get("price_changes")
        if not isinstance(changes, list):
            changes = event.get("changes")
        if not isinstance(changes, list):
            return
        for change in changes:
            if not isinstance(change, dict):
                continue
            token_id = str(change.get("asset_id") or "")
            outcome = state.token_to_outcome.get(token_id)
            if outcome is None:
                continue
            quote = state.quotes[outcome]
            if "best_bid" in change:
                quote.best_bid = parse_contract_price(change.get("best_bid"))
            if "best_ask" in change:
                quote.best_ask = parse_contract_price(change.get("best_ask"))
            quote.initialized = True
        return

    if event_type == "best_bid_ask":
        token_id = str(event.get("asset_id") or "")
        outcome = state.token_to_outcome.get(token_id)
        if outcome is None:
            return
        quote = state.quotes[outcome]
        quote.best_bid = parse_contract_price(event.get("best_bid"))
        quote.best_ask = parse_contract_price(event.get("best_ask"))
        quote.initialized = True


def parse_twap_event(event: Any) -> float | None:
    if not isinstance(event, dict):
        return None
    if event.get("topic") != TWAP_TOPIC or event.get("type") != "update":
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return None
    if payload.get("symbol") != TWAP_SYMBOL or payload.get("window_s") != 60:
        return None

    full_accuracy = payload.get("full_accuracy_value")
    if isinstance(full_accuracy, str):
        try:
            price = float(Decimal(full_accuracy) / (Decimal(10) ** 18))
        except (InvalidOperation, OverflowError, ValueError):
            return None
    else:
        try:
            price = float(payload.get("value"))
        except (TypeError, ValueError):
            return None
    return price if math.isfinite(price) and price > 0 else None


# ============================================================
# DIRECT POLYMARKET PRICE-TO-BEAT LOOKUP
# ============================================================


def price_to_beat_request_url(market: MarketContext) -> str:
    start = datetime.fromtimestamp(market.window_epoch, timezone.utc)
    end = start + timedelta(seconds=WINDOW_SECONDS)
    params = {
        "symbol": "BTC",
        "eventStartTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "variant": "fiveminute",
        "endDate": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        # These must match the parameters used by the Polymarket webpage.
        # Without them the endpoint returns the legacy non-TWAP opening price.
        "twapEnabled": "true",
        "twapLookbackSeconds": 60,
    }
    return f"{PRICE_TO_BEAT_URL}?{urlencode(params)}"


def fetch_price_to_beat_once(market: MarketContext) -> float | None:
    request = Request(
        price_to_beat_request_url(market),
        headers={
            "Accept": "application/json",
            "Referer": "https://polymarket.com/",
            "User-Agent": USER_AGENT,
        },
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        result = json.load(response)
    if not isinstance(result, dict):
        return None
    try:
        price = float(result.get("openPrice"))
    except (TypeError, ValueError):
        return None
    return price if math.isfinite(price) and price > 0 else None


# ============================================================
# SQLITE
# ============================================================


class DailyDatabase:
    def __init__(self, dataset_date: date) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.dataset_date = dataset_date
        self.path = DATA_DIR / f"btc_5m_twap_statistics_{dataset_date.isoformat()}.sqlite3"
        # Sample batches are written from a background thread so an SD-card
        # commit cannot delay the 4 Hz sampler. All access is serialized by
        # _lock; check_same_thread=False only permits that controlled handoff.
        self.connection = sqlite3.connect(
            self.path,
            timeout=30,
            check_same_thread=False,
        )
        self._lock = threading.RLock()
        self.pending_samples = 0
        try:
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.connection.execute("PRAGMA journal_mode = WAL")
            self.connection.execute("PRAGMA synchronous = NORMAL")
            self._create_schema()
            self._set_metadata()
            self.run_id = self._start_run()
        except Exception:
            self.connection.close()
            raise

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS dataset_metadata (
                key                 TEXT PRIMARY KEY,
                value               TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                id                  INTEGER PRIMARY KEY,
                started_at_utc      TEXT NOT NULL,
                ended_at_utc        TEXT
            );

            CREATE TABLE IF NOT EXISTS markets (
                id                  INTEGER PRIMARY KEY,
                slug                TEXT UNIQUE NOT NULL,
                window_datetime_utc TEXT NOT NULL,
                price_to_beat       REAL
            );

            CREATE TABLE IF NOT EXISTS samples (
                market_id           INTEGER NOT NULL,
                sample_index        INTEGER NOT NULL
                                      CHECK(sample_index BETWEEN 0 AND 1199),
                up_bid              REAL,
                up_ask              REAL,
                down_bid            REAL,
                down_ask            REAL,
                twap_price          REAL,
                sample_valid        BOOLEAN NOT NULL
                                      CHECK(sample_valid IN (0, 1)),

                PRIMARY KEY (market_id, sample_index),
                FOREIGN KEY (market_id) REFERENCES markets(id)
            ) WITHOUT ROWID;
            """
        )

    def _set_metadata(self) -> None:
        metadata = {
            "utc_date": self.dataset_date.isoformat(),
            "sample_interval_seconds": str(SAMPLE_INTERVAL_SECONDS),
            "samples_per_market": str(SAMPLES_PER_MARKET),
            "twap_topic": TWAP_TOPIC,
            "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        for key, value in metadata.items():
            self.connection.execute(
                "INSERT OR IGNORE INTO dataset_metadata(key, value) VALUES (?, ?)",
                (key, value),
            )
        self.connection.commit()

    def _start_run(self) -> int:
        cursor = self.connection.execute(
            "INSERT INTO runs(started_at_utc) VALUES (?)",
            (datetime.now(timezone.utc).isoformat(timespec="seconds"),),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def ensure_market(self, market: MarketContext) -> int:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO markets(slug, window_datetime_utc)
            VALUES (?, ?)
            """,
            (market.slug, market.window_datetime_utc),
        )
        row = self.connection.execute(
            "SELECT id FROM markets WHERE slug = ?",
            (market.slug,),
        ).fetchone()
        if row is None:
            raise sqlite3.DatabaseError("market insert did not produce a row")
        self.connection.commit()
        return int(row[0])

    def set_price_to_beat(self, market_id: int, price: float) -> None:
        with self._lock:
            self.connection.execute(
                "UPDATE markets SET price_to_beat = ? WHERE id = ?",
                (price, market_id),
            )
            self.connection.commit()

    def record_sample(
        self,
        market_id: int,
        sample_index: int,
        up_bid: float | None,
        up_ask: float | None,
        down_bid: float | None,
        down_ask: float | None,
        twap_price: float | None,
        sample_valid: bool,
    ) -> None:
        with self._lock:
            inserted = self._record_sample(
                market_id,
                sample_index,
                up_bid,
                up_ask,
                down_bid,
                down_ask,
                twap_price,
                sample_valid,
            )
            if inserted and self.pending_samples >= DATABASE_COMMIT_EVERY_SAMPLES:
                self.commit()

    def _record_sample(
        self,
        market_id: int,
        sample_index: int,
        up_bid: float | None,
        up_ask: float | None,
        down_bid: float | None,
        down_ask: float | None,
        twap_price: float | None,
        sample_valid: bool,
    ) -> bool:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO samples(
                market_id, sample_index,
                up_bid, up_ask, down_bid, down_ask,
                twap_price, sample_valid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market_id,
                sample_index,
                up_bid,
                up_ask,
                down_bid,
                down_ask,
                twap_price,
                int(sample_valid),
            ),
        )
        if cursor.rowcount == 0:
            return False
        self.pending_samples += 1
        return True

    def record_samples(self, rows: list[tuple[Any, ...]]) -> None:
        """Write and commit one ordered batch from the background writer."""
        with self._lock:
            for row in rows:
                self._record_sample(*row)
            self.commit()

    def record_missing_range(self, market_id: int, stop_index: int) -> None:
        for sample_index in range(max(0, min(stop_index, SAMPLES_PER_MARKET))):
            self.record_sample(
                market_id,
                sample_index,
                None,
                None,
                None,
                None,
                None,
                False,
            )

    def commit(self) -> None:
        with self._lock:
            self.connection.commit()
            self.pending_samples = 0

    def close(self) -> None:
        with self._lock:
            self.connection.execute(
                "UPDATE runs SET ended_at_utc = ? WHERE id = ?",
                (
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    self.run_id,
                ),
            )
            self.connection.commit()
            self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self.connection.close()


# ============================================================
# WEBSOCKET WORKERS
# ============================================================


async def send_text_pings(websocket, interval_seconds: float) -> None:
    try:
        while True:
            await asyncio.sleep(interval_seconds)
            await websocket.send("PING")
    except ConnectionClosed:
        # The receive loop owns reconnecting. A ping that races with a normal
        # socket close must not terminate the entire collector.
        return


async def stop_ping_task(task: asyncio.Task | None) -> None:
    """Stop a connection's ping task without leaking close-race errors."""
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except (ConnectionClosed, OSError, TimeoutError):
        pass
    except Exception as exc:
        # Cleanup errors are diagnostic; they must not stop overnight capture.
        print(
            f"WARNING: ping task cleanup failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )


def cancellation_requested() -> bool:
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def report_reconnect(feed: str, exc: BaseException, delay: float) -> None:
    print(
        f"{feed} connection lost ({type(exc).__name__}: {exc}); "
        f"reconnecting in {delay:g}s",
        file=sys.stderr,
    )


async def clob_worker(market: MarketContext, state: MarketBookState) -> None:
    reconnect_delay = 1
    subscription = {
        "assets_ids": [market.token_ids["Up"], market.token_ids["Down"]],
        "type": "market",
        "custom_feature_enabled": True,
    }

    while True:
        ping_task: asyncio.Task | None = None
        reconnect_problem: BaseException | None = None
        try:
            state.reset_for_connection()
            async with websockets.connect(
                CLOB_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=None,
            ) as websocket:
                await websocket.send(json.dumps(subscription))
                state.connected = True
                reconnect_delay = 1
                ping_task = asyncio.create_task(send_text_pings(websocket, 8))

                async for raw_message in websocket:
                    if raw_message in {"PONG", "pong"}:
                        continue
                    try:
                        event = json.loads(raw_message)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    apply_clob_event(state, event)
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, OSError, TimeoutError) as exc:
            # A close timeout can replace CancelledError while an async context
            # manager exits. Preserve an intentional market-end cancellation.
            if cancellation_requested():
                raise asyncio.CancelledError from None
            reconnect_problem = exc
        finally:
            state.connected = False
            await stop_ping_task(ping_task)

        if reconnect_problem is not None:
            report_reconnect("CLOB", reconnect_problem, reconnect_delay)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_SECONDS)


async def twap_worker(state: TwapState) -> None:
    reconnect_delay = 1
    subscription = {
        "action": "subscribe",
        "subscriptions": [{"topic": TWAP_TOPIC, "type": "update"}],
    }

    while True:
        ping_task: asyncio.Task | None = None
        reconnect_problem: BaseException | None = None
        try:
            state.reset_for_connection()
            async with websockets.connect(
                RTDS_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=None,
            ) as websocket:
                await websocket.send(json.dumps(subscription))
                state.connected = True
                reconnect_delay = 1
                ping_task = asyncio.create_task(send_text_pings(websocket, 4))

                async for raw_message in websocket:
                    if raw_message in {"PONG", "pong"}:
                        continue
                    try:
                        event = json.loads(raw_message)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    price = parse_twap_event(event)
                    if price is not None:
                        state.price = price
                        state.received_monotonic = time.monotonic()
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, OSError, TimeoutError) as exc:
            if cancellation_requested():
                raise asyncio.CancelledError from None
            reconnect_problem = exc
        finally:
            state.connected = False
            await stop_ping_task(ping_task)

        if reconnect_problem is not None:
            report_reconnect("TWAP", reconnect_problem, reconnect_delay)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_SECONDS)


async def price_to_beat_worker(
    database: DailyDatabase,
    market_id: int,
    market: MarketContext,
) -> None:
    deadline = market.end_epoch
    while time.time() < deadline:
        try:
            price = await asyncio.to_thread(fetch_price_to_beat_once, market)
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError):
            price = None
        if price is not None:
            await asyncio.to_thread(database.set_price_to_beat, market_id, price)
            return
        await asyncio.sleep(PRICE_TO_BEAT_RETRY_SECONDS)


# ============================================================
# FIXED 4 HZ SAMPLER
# ============================================================


def quote_values(state: MarketBookState) -> tuple[float | None, ...]:
    up = state.quotes["Up"]
    down = state.quotes["Down"]
    return up.best_bid, up.best_ask, down.best_bid, down.best_ask


def book_is_usable(state: MarketBookState) -> bool:
    if not state.connected:
        return False
    up = state.quotes["Up"]
    down = state.quotes["Down"]
    return (
        up.initialized
        and down.initialized
        and up.best_bid is not None
        and up.best_ask is not None
        and down.best_bid is not None
        and down.best_ask is not None
    )


def snapshot_state(
    book_state: MarketBookState,
    twap_state: TwapState,
) -> tuple[tuple[float | None, ...], float | None, bool]:
    """Read both latest feed states without yielding between the reads."""
    now_monotonic = time.monotonic()
    values = quote_values(book_state)
    twap_price = twap_state.price
    valid = book_is_usable(book_state) and twap_state.is_usable(now_monotonic)
    return values, twap_price, valid


async def write_sample_batch_after(
    previous_write: asyncio.Task | None,
    database: DailyDatabase,
    rows: list[tuple[Any, ...]],
) -> None:
    """Serialize sample batches off the event loop in submission order."""
    if previous_write is not None:
        await previous_write
    await asyncio.to_thread(database.record_samples, rows)


def render_sample(
    market: MarketContext,
    sample_index: int,
    values: tuple[float | None, ...],
    twap_price: float | None,
    valid: bool,
) -> None:
    if not sys.stdout.isatty() or sample_index % 4:
        return

    def display(value: float | None, digits: int = 3) -> str:
        return "N/A" if value is None else f"{value:.{digits}f}"

    elapsed = sample_index * SAMPLE_INTERVAL_SECONDS
    up_bid, up_ask, down_bid, down_ask = values
    line = (
        f"\r\033[2K{market.slug} | {elapsed:6.2f}s | "
        f"Up {display(up_bid)}/{display(up_ask)} | "
        f"Down {display(down_bid)}/{display(down_ask)} | "
        f"TWAP {display(twap_price, 8)} | "
        f"{'VALID' if valid else 'INVALID'}"
    )
    print(line, end="", flush=True)


async def collect_market(
    database: DailyDatabase,
    market: MarketContext,
    twap_state: TwapState,
) -> None:
    market_id = database.ensure_market(market)
    book_state = MarketBookState(
        token_to_outcome={
            market.token_ids["Up"]: "Up",
            market.token_ids["Down"]: "Down",
        }
    )

    clob_task = asyncio.create_task(clob_worker(market, book_state))
    price_task = asyncio.create_task(
        price_to_beat_worker(database, market_id, market)
    )

    pending_rows: list[tuple[Any, ...]] = []
    last_write_task: asyncio.Task | None = None

    def queue_sample(row: tuple[Any, ...]) -> None:
        nonlocal pending_rows, last_write_task
        pending_rows.append(row)
        if len(pending_rows) >= DATABASE_COMMIT_EVERY_SAMPLES:
            batch = pending_rows
            pending_rows = []
            last_write_task = asyncio.create_task(
                write_sample_batch_after(last_write_task, database, batch)
            )

    def queue_missing_range(stop_index: int) -> None:
        for missing_index in range(
            max(0, min(stop_index, SAMPLES_PER_MARKET))
        ):
            queue_sample(
                (
                    market_id,
                    missing_index,
                    None,
                    None,
                    None,
                    None,
                    None,
                    False,
                )
            )

    async def finish_writes() -> None:
        nonlocal pending_rows, last_write_task
        if pending_rows:
            batch = pending_rows
            pending_rows = []
            last_write_task = asyncio.create_task(
                write_sample_batch_after(last_write_task, database, batch)
            )
        if last_write_task is not None:
            await last_write_task

    # Convert the UTC market boundary to the monotonic clock once. Subsequent
    # clock corrections cannot compress, stretch, or skip the 250 ms grid.
    now = time.time()
    now_monotonic = time.monotonic()
    market_start_monotonic = now_monotonic + (market.window_epoch - now)
    first_live_index = math.ceil(
        max(0.0, now - market.window_epoch) / SAMPLE_INTERVAL_SECONDS
    )
    first_live_index = max(0, min(first_live_index, SAMPLES_PER_MARKET))
    queue_missing_range(first_live_index)

    try:
        for sample_index in range(first_live_index, SAMPLES_PER_MARKET):
            target_monotonic = (
                market_start_monotonic
                + sample_index * SAMPLE_INTERVAL_SECONDS
            )
            delay = target_monotonic - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)

            lateness = time.monotonic() - target_monotonic
            if lateness > MAX_SAMPLE_LATENESS_SECONDS:
                queue_sample(
                    (
                        market_id,
                        sample_index,
                        None,
                        None,
                        None,
                        None,
                        None,
                        False,
                    )
                )
                render_sample(
                    market,
                    sample_index,
                    (None, None, None, None),
                    None,
                    False,
                )
                continue

            values, twap_price, valid = snapshot_state(book_state, twap_state)
            queue_sample(
                (
                    market_id,
                    sample_index,
                    *values,
                    twap_price,
                    valid,
                )
            )
            render_sample(market, sample_index, values, twap_price, valid)

        remaining = (
            market_start_monotonic + WINDOW_SECONDS - time.monotonic()
        )
        if remaining > 0:
            await asyncio.sleep(remaining)
    finally:
        clob_task.cancel()
        price_task.cancel()
        worker_results = await asyncio.gather(
            clob_task,
            price_task,
            return_exceptions=True,
        )
        await finish_writes()
        for result in worker_results:
            if isinstance(result, BaseException) and not isinstance(
                result,
                asyncio.CancelledError,
            ):
                raise result


async def discover_market() -> MarketContext:
    while True:
        try:
            market = await asyncio.to_thread(find_current_market)
            return market_context(market)
        except asyncio.CancelledError:
            raise
        except (OSError, TimeoutError, ValueError, LookupError) as exc:
            if sys.stdout.isatty():
                print(
                    f"\r\033[2KWaiting for current market: "
                    f"{type(exc).__name__}: {exc}",
                    end="",
                    flush=True,
                )
            await asyncio.sleep(MARKET_RETRY_SECONDS)


async def run_collector() -> Path:
    dataset_date = datetime.now(timezone.utc).date()
    database = DailyDatabase(dataset_date)
    last_path = database.path
    twap_state = TwapState()
    twap_task = asyncio.create_task(twap_worker(twap_state))

    print(f"Dataset: {database.path}")
    print("Collecting at 4 Hz. Press Ctrl+C to save and stop.")

    try:
        while True:
            current_date = datetime.now(timezone.utc).date()
            if current_date != dataset_date:
                database.close()
                dataset_date = current_date
                database = DailyDatabase(dataset_date)
                last_path = database.path

            market = await discover_market()
            await collect_market(database, market, twap_state)
    finally:
        twap_task.cancel()
        try:
            with contextlib.suppress(asyncio.CancelledError):
                await twap_task
        finally:
            database.close()
        if sys.stdout.isatty():
            print("\r\033[2K", end="")

    return last_path


def main() -> int:
    try:
        saved_path = asyncio.run(run_collector())
    except KeyboardInterrupt:
        # asyncio.run cancels the collector first, so its finally block has
        # already committed and closed the database by the time we get here.
        latest = max(
            DATA_DIR.glob("btc_5m_twap_statistics_*.sqlite3"),
            default=None,
            key=lambda path: path.stat().st_mtime,
        )
        print(f"Stopped. Dataset saved: {latest if latest is not None else DATA_DIR}")
        return 0
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    else:
        print(f"Dataset saved: {saved_path}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
