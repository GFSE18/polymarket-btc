#!/usr/bin/env python3
"""Collect one local day of BTC five-minute top-of-book crossing statistics."""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import msvcrt as _terminal_keyboard
except ImportError:  # pragma: no cover - Windows is the target runtime.
    _terminal_keyboard = None

from fetch_current_btc_5m import (
    WINDOW_SECONDS,
    _parse_json_list,
    _parse_timestamp,
    find_current_market,
)


# ============================================================
# USER-ADJUSTABLE SETTINGS
# ============================================================

SCAN_INTERVAL_SECONDS = 0.250
THRESHOLDS = (0.45, 0.40, 0.35)
RUNTIME_PRINTOUT = True  # Startup state; press P in the terminal to toggle it.


# ============================================================
# OTHER SETTINGS
# ============================================================

CLOB_API = "https://clob.polymarket.com"
BOOK_TIMEOUT_SECONDS = 3
MARKET_RETRY_DELAY_SECONDS = 0.5
USER_AGENT = "polymarket-btc-5m-statistics/1.0"
DATA_DIR = Path(__file__).resolve().parent / "btc_5m_statistics_data"
PRICE_SCALE = 1_000_000
OUTCOMES = ("Up", "Down")


def utc_iso(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, timezone.utc).isoformat(
        timespec="milliseconds"
    )


def local_day_end_epoch(dataset_date: date) -> float:
    """Return the next system-local midnight, including DST transitions."""
    next_midnight = datetime.combine(
        dataset_date + timedelta(days=1),
        datetime_time.min,
    )
    return next_midnight.astimezone().timestamp()


def price_to_micros(value: Any) -> int:
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid price: {value!r}") from exc
    if not Decimal("0") <= price <= Decimal("1"):
        raise ValueError(f"price was outside [0, 1]: {value!r}")
    scaled = price * PRICE_SCALE
    if scaled != scaled.to_integral_value():
        raise ValueError(f"price had more than six decimal places: {value!r}")
    return int(scaled)


def display_price(price_micros: int | None) -> str:
    if price_micros is None:
        return "   N/A"
    return f"{price_micros / PRICE_SCALE:0.3f}"


def threshold_configuration() -> tuple[tuple[float, ...], tuple[int, ...]]:
    configured = tuple(float(value) for value in THRESHOLDS)
    threshold_micros = tuple(price_to_micros(value) for value in configured)
    if not configured:
        raise ValueError("THRESHOLDS cannot be empty")
    if len(set(threshold_micros)) != len(threshold_micros):
        raise ValueError("THRESHOLDS cannot contain duplicates")
    if any(value <= 0 or value >= PRICE_SCALE for value in threshold_micros):
        raise ValueError("each threshold must be strictly between 0 and 1")
    return configured, threshold_micros


@dataclass(frozen=True)
class BookTop:
    best_bid_micros: int | None
    best_ask_micros: int | None
    received_epoch_ms: int
    source_epoch_ms: int | None


@dataclass(frozen=True)
class MarketContext:
    slug: str
    question: str
    condition_id: str
    window_start_epoch: int
    end_epoch: float
    token_ids: dict[str, str]

    @property
    def duration_ms(self) -> int:
        return max(0, int(round((self.end_epoch - self.window_start_epoch) * 1000)))


def market_context(market: dict[str, Any]) -> MarketContext:
    slug = str(market.get("slug") or "")
    try:
        window_start = int(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"market had an invalid BTC five-minute slug: {slug!r}") from exc

    outcomes = [
        str(outcome).strip().lower()
        for outcome in _parse_json_list(market.get("outcomes"), "outcomes")
    ]
    token_ids = [
        str(token_id).strip()
        for token_id in _parse_json_list(market.get("clobTokenIds"), "clobTokenIds")
    ]
    if len(outcomes) != 2 or len(token_ids) != 2:
        raise ValueError("market did not provide two outcome/token pairs")

    outcome_tokens = {
        outcome.capitalize(): token_id
        for outcome, token_id in zip(outcomes, token_ids)
    }
    if set(outcome_tokens) != set(OUTCOMES):
        raise ValueError("market outcomes were not exactly Up and Down")

    question = market.get("question") or market.get("title")
    if not isinstance(question, str) or not question.strip():
        raise ValueError("market response did not contain a name")

    end_epoch = _parse_timestamp(
        market.get("endDate") or market.get("endDateIso"),
        "endDate",
    )
    if abs(end_epoch - window_start - WINDOW_SECONDS) > 1:
        raise ValueError("market expiry did not match its five-minute slug window")

    return MarketContext(
        slug=slug,
        question=question.strip(),
        condition_id=str(market.get("conditionId") or ""),
        window_start_epoch=window_start,
        end_epoch=end_epoch,
        token_ids=outcome_tokens,
    )


def _valid_book_prices(levels: Any) -> list[int]:
    if not isinstance(levels, list):
        return []
    prices: list[int] = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        try:
            prices.append(price_to_micros(level.get("price")))
        except ValueError:
            continue
    return prices


def _source_epoch_ms(value: Any) -> int | None:
    try:
        timestamp = int(str(value))
    except (TypeError, ValueError):
        return None
    if timestamp < 10_000_000_000:
        timestamp *= 1000
    return timestamp if timestamp > 0 else None


def fetch_book_top(token_id: str) -> BookTop:
    url = f"{CLOB_API}/book?{urlencode({'token_id': token_id})}"
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    with urlopen(request, timeout=BOOK_TIMEOUT_SECONDS) as response:
        book = json.load(response)
    received_epoch_ms = time.time_ns() // 1_000_000

    if not isinstance(book, dict):
        raise ValueError("CLOB returned a non-object order book")
    asset_id = book.get("asset_id")
    if asset_id is not None and str(asset_id) != token_id:
        raise ValueError("CLOB returned an order book for a different token")

    bids = _valid_book_prices(book.get("bids"))
    asks = _valid_book_prices(book.get("asks"))
    return BookTop(
        best_bid_micros=max(bids) if bids else None,
        best_ask_micros=min(asks) if asks else None,
        received_epoch_ms=received_epoch_ms,
        source_epoch_ms=_source_epoch_ms(book.get("timestamp")),
    )


class DailyDatabase:
    def __init__(
        self,
        path: Path,
        dataset_date: date,
        scan_interval: float,
        thresholds: tuple[float, ...],
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path, timeout=30)
        try:
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.connection.execute("PRAGMA journal_mode = WAL")
            self.connection.execute("PRAGMA synchronous = NORMAL")
            self._create_schema()
            self._set_dataset_metadata(dataset_date)
            self.run_id = self._start_run(scan_interval, thresholds)
        except Exception:
            self.connection.close()
            raise

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS dataset_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY,
                started_at_utc TEXT NOT NULL,
                ended_at_utc TEXT,
                scan_interval_seconds REAL NOT NULL,
                thresholds_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS markets (
                id INTEGER PRIMARY KEY,
                run_id INTEGER NOT NULL REFERENCES runs(id),
                slug TEXT NOT NULL,
                question TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                window_start_epoch_ms INTEGER NOT NULL,
                end_epoch_ms INTEGER NOT NULL,
                up_token_id TEXT NOT NULL,
                down_token_id TEXT NOT NULL,
                observed_start_utc TEXT NOT NULL,
                observed_end_utc TEXT,
                initial_up_elapsed_ms INTEGER,
                initial_up_bid_micros INTEGER,
                initial_up_ask_micros INTEGER,
                initial_down_elapsed_ms INTEGER,
                initial_down_bid_micros INTEGER,
                initial_down_ask_micros INTEGER,
                UNIQUE(run_id, slug)
            );

            CREATE TABLE IF NOT EXISTS book_quotes (
                id INTEGER PRIMARY KEY,
                market_id INTEGER NOT NULL REFERENCES markets(id),
                outcome TEXT NOT NULL CHECK(outcome IN ('Up', 'Down')),
                observed_elapsed_ms INTEGER NOT NULL,
                observed_epoch_ms INTEGER NOT NULL,
                source_epoch_ms INTEGER,
                best_bid_micros INTEGER,
                best_ask_micros INTEGER
            );

            CREATE TABLE IF NOT EXISTS crossings (
                id INTEGER PRIMARY KEY,
                market_id INTEGER NOT NULL REFERENCES markets(id),
                outcome TEXT NOT NULL CHECK(outcome IN ('Up', 'Down')),
                threshold_micros INTEGER NOT NULL,
                start_elapsed_ms INTEGER NOT NULL,
                duration_ms INTEGER,
                CHECK(duration_ms IS NULL OR duration_ms >= 0)
            );

            CREATE INDEX IF NOT EXISTS idx_quotes_market_outcome_time
                ON book_quotes(market_id, outcome, observed_elapsed_ms);
            CREATE INDEX IF NOT EXISTS idx_crossings_market_outcome_threshold
                ON crossings(market_id, outcome, threshold_micros);
            """
        )

    def _set_dataset_metadata(self, dataset_date: date) -> None:
        existing = self.connection.execute(
            "SELECT value FROM dataset_metadata WHERE key = 'local_date'"
        ).fetchone()
        expected = dataset_date.isoformat()
        if existing is not None and existing[0] != expected:
            raise ValueError(
                f"database is for local date {existing[0]}, not {expected}"
            )
        metadata = {
            "local_date": expected,
            "local_timezone_at_creation": datetime.now().astimezone().tzname() or "",
            "created_at_utc": utc_iso(time.time()),
            "price_scale": str(PRICE_SCALE),
        }
        for key, value in metadata.items():
            self.connection.execute(
                "INSERT OR IGNORE INTO dataset_metadata(key, value) VALUES (?, ?)",
                (key, value),
            )
        self.connection.commit()

    def _start_run(
        self,
        scan_interval: float,
        thresholds: tuple[float, ...],
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO runs(started_at_utc, scan_interval_seconds, thresholds_json)
            VALUES (?, ?, ?)
            """,
            (utc_iso(time.time()), scan_interval, json.dumps(thresholds)),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def start_market(self, market: MarketContext, observed_epoch: float) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO markets(
                run_id, slug, question, condition_id,
                window_start_epoch_ms, end_epoch_ms,
                up_token_id, down_token_id, observed_start_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.run_id,
                market.slug,
                market.question,
                market.condition_id,
                market.window_start_epoch * 1000,
                int(round(market.end_epoch * 1000)),
                market.token_ids["Up"],
                market.token_ids["Down"],
                utc_iso(observed_epoch),
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def finish_market(self, market_id: int, observed_epoch: float) -> None:
        self.connection.execute(
            "UPDATE markets SET observed_end_utc = ? WHERE id = ?",
            (utc_iso(observed_epoch), market_id),
        )

    def record_initial_quote(
        self,
        market_id: int,
        outcome: str,
        elapsed_ms: int,
        book: BookTop,
    ) -> None:
        if outcome == "Up":
            columns = (
                "initial_up_elapsed_ms = ?, initial_up_bid_micros = ?, "
                "initial_up_ask_micros = ?"
            )
        elif outcome == "Down":
            columns = (
                "initial_down_elapsed_ms = ?, initial_down_bid_micros = ?, "
                "initial_down_ask_micros = ?"
            )
        else:
            raise ValueError(f"unsupported outcome: {outcome!r}")
        self.connection.execute(
            f"UPDATE markets SET {columns} WHERE id = ?",
            (
                elapsed_ms,
                book.best_bid_micros,
                book.best_ask_micros,
                market_id,
            ),
        )

    def record_quote(
        self,
        market_id: int,
        outcome: str,
        elapsed_ms: int,
        book: BookTop,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO book_quotes(
                market_id, outcome, observed_elapsed_ms, observed_epoch_ms,
                source_epoch_ms, best_bid_micros, best_ask_micros
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market_id,
                outcome,
                elapsed_ms,
                book.received_epoch_ms,
                book.source_epoch_ms,
                book.best_bid_micros,
                book.best_ask_micros,
            ),
        )

    def start_crossing(
        self,
        market_id: int,
        outcome: str,
        threshold_micros: int,
        elapsed_ms: int,
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO crossings(
                market_id, outcome, threshold_micros, start_elapsed_ms
            ) VALUES (?, ?, ?, ?)
            """,
            (market_id, outcome, threshold_micros, elapsed_ms),
        )
        return int(cursor.lastrowid)

    def finish_crossing(
        self,
        crossing_id: int,
        start_elapsed_ms: int,
        end_elapsed_ms: int,
    ) -> None:
        duration_ms = max(0, end_elapsed_ms - start_elapsed_ms)
        self.connection.execute(
            "UPDATE crossings SET duration_ms = ? WHERE id = ?",
            (duration_ms, crossing_id),
        )

    def commit(self) -> None:
        self.connection.commit()

    def close(self, ended_epoch: float) -> None:
        self.connection.execute(
            "UPDATE runs SET ended_at_utc = ? WHERE id = ?",
            (utc_iso(ended_epoch), self.run_id),
        )
        self.connection.commit()
        self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.connection.close()


@dataclass
class OutcomeTracker:
    outcome: str
    thresholds_micros: tuple[int, ...]
    initialized: bool = False
    initial_recorded: bool = False
    previous_ask_micros: int | None = None
    current_bid_micros: int | None = None
    current_ask_micros: int | None = None
    last_good_elapsed_ms: int | None = None
    last_quote: tuple[int | None, int | None] | None = None
    counts: dict[int, int] = field(default_factory=dict)
    active_crossings: dict[int, tuple[int, int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.counts = {threshold: 0 for threshold in self.thresholds_micros}

    def _finish_active(self, database: DailyDatabase, elapsed_ms: int) -> None:
        for threshold, (crossing_id, start_elapsed_ms) in list(
            self.active_crossings.items()
        ):
            database.finish_crossing(crossing_id, start_elapsed_ms, elapsed_ms)
            self.active_crossings.pop(threshold, None)

    def observe(
        self,
        database: DailyDatabase,
        market_id: int,
        market: MarketContext,
        book: BookTop,
    ) -> None:
        elapsed_ms = max(
            0,
            min(
                market.duration_ms,
                book.received_epoch_ms - market.window_start_epoch * 1000,
            ),
        )

        if not self.initial_recorded:
            database.record_initial_quote(
                market_id,
                self.outcome,
                elapsed_ms,
                book,
            )
            self.initial_recorded = True

        quote = (book.best_bid_micros, book.best_ask_micros)
        if quote != self.last_quote:
            database.record_quote(
                market_id,
                self.outcome,
                elapsed_ms,
                book,
            )
            self.last_quote = quote

        ask = book.best_ask_micros
        self.current_bid_micros = book.best_bid_micros
        self.current_ask_micros = ask

        if ask is None:
            self._finish_active(database, elapsed_ms)
            self.initialized = False
            self.previous_ask_micros = None
            self.last_good_elapsed_ms = elapsed_ms
            return

        if self.initialized and self.previous_ask_micros is not None:
            previous = self.previous_ask_micros
            for threshold in self.thresholds_micros:
                if previous > threshold >= ask:
                    crossing_id = database.start_crossing(
                        market_id,
                        self.outcome,
                        threshold,
                        elapsed_ms,
                    )
                    self.active_crossings[threshold] = (crossing_id, elapsed_ms)
                    self.counts[threshold] += 1
                elif previous <= threshold < ask:
                    active = self.active_crossings.pop(threshold, None)
                    if active is not None:
                        crossing_id, start_elapsed_ms = active
                        database.finish_crossing(
                            crossing_id,
                            start_elapsed_ms,
                            elapsed_ms,
                        )

        self.initialized = True
        self.previous_ask_micros = ask
        self.last_good_elapsed_ms = elapsed_ms

    def data_gap(
        self,
        database: DailyDatabase,
        market_id: int,
        market: MarketContext,
        observed_epoch_ms: int,
    ) -> None:
        close_elapsed_ms = self.last_good_elapsed_ms
        if close_elapsed_ms is None:
            close_elapsed_ms = max(
                0,
                min(
                    market.duration_ms,
                    observed_epoch_ms - market.window_start_epoch * 1000,
                ),
            )
        self._finish_active(database, close_elapsed_ms)
        self.initialized = False
        self.previous_ask_micros = None
        self.current_bid_micros = None
        self.current_ask_micros = None

        if self.last_quote != (None, None):
            database.record_quote(
                market_id,
                self.outcome,
                close_elapsed_ms,
                BookTop(None, None, observed_epoch_ms, None),
            )
            self.last_quote = (None, None)

    def close(self, database: DailyDatabase, elapsed_ms: int) -> None:
        self._finish_active(database, elapsed_ms)
        self.initialized = False
        self.previous_ask_micros = None


def render_dashboard(
    dataset_date: date,
    database_path: Path,
    scan_interval: float,
    thresholds: tuple[float, ...],
    market: MarketContext | None,
    trackers: dict[str, OutcomeTracker],
    status: str,
    runtime_printout: bool,
) -> None:
    if not runtime_printout:
        return

    lines = [
        "BTC 5-MINUTE CROSSING STATISTICS",
        f"Dataset: {dataset_date.isoformat()} | Scan: {scan_interval * 1000:.0f} ms",
        f"File: {database_path}",
        "",
    ]
    if market is None:
        lines.extend(["Market: waiting for a current market", "", f"Status: {status}"])
    else:
        now = time.time()
        elapsed = max(
            0.0,
            min(
                market.end_epoch - market.window_start_epoch,
                now - market.window_start_epoch,
            ),
        )
        lines.extend(
            [
                f"Market: {market.question}",
                f"Slug:   {market.slug}",
                f"Elapsed: {elapsed:6.1f}s / {market.end_epoch - market.window_start_epoch:.1f}s",
                "",
                "TOP OF BOOK       BEST BID   BEST ASK",
                f"Up               {display_price(trackers['Up'].current_bid_micros):>8}   "
                f"{display_price(trackers['Up'].current_ask_micros):>8}",
                f"Down             {display_price(trackers['Down'].current_bid_micros):>8}   "
                f"{display_price(trackers['Down'].current_ask_micros):>8}",
                "",
                "DOWNWARD ASK CROSSINGS       UP     DOWN",
            ]
        )
        for threshold, threshold_micros in zip(
            thresholds,
            trackers["Up"].thresholds_micros,
        ):
            lines.append(
                f"{threshold:0.3f}                    "
                f"{trackers['Up'].counts[threshold_micros]:4d}   "
                f"{trackers['Down'].counts[threshold_micros]:4d}"
            )
        lines.extend(["", f"Status: {status}"])

    lines.extend(["", "P: pause display | Ctrl+C: save and stop"])

    sys.stdout.write("\x1b[2J\x1b[H" + "\n".join(lines) + "\n")
    sys.stdout.flush()


def render_paused_notice() -> None:
    sys.stdout.write(
        "\x1b[2J\x1b[H"
        "Runtime display paused. Collection is still running.\n"
        "P: resume display | Ctrl+C: save and stop\n"
    )
    sys.stdout.flush()


def poll_runtime_printout(runtime_printout: bool) -> tuple[bool, bool]:
    """Consume pending console keys and toggle the display when P is pressed."""
    if _terminal_keyboard is None:
        return runtime_printout, False

    toggled = False
    try:
        while _terminal_keyboard.kbhit():
            key = _terminal_keyboard.getwch()
            if key in {"\x00", "\xe0"}:
                if _terminal_keyboard.kbhit():
                    _terminal_keyboard.getwch()
                continue
            if key == "\x03":
                raise KeyboardInterrupt
            if key.lower() == "p":
                runtime_printout = not runtime_printout
                toggled = True
    except OSError:
        return runtime_printout, toggled
    return runtime_printout, toggled


def validate_settings() -> tuple[tuple[float, ...], tuple[int, ...]]:
    if SCAN_INTERVAL_SECONDS <= 0:
        raise ValueError("SCAN_INTERVAL_SECONDS must be positive")
    if BOOK_TIMEOUT_SECONDS <= 0:
        raise ValueError("BOOK_TIMEOUT_SECONDS must be positive")
    return threshold_configuration()


def daily_database_path(dataset_date: date) -> Path:
    return DATA_DIR / f"btc_5m_statistics_{dataset_date.isoformat()}.sqlite3"


def finish_market_observation(
    database: DailyDatabase,
    market: MarketContext | None,
    market_id: int | None,
    trackers: dict[str, OutcomeTracker],
    ended_epoch: float,
) -> None:
    if market is None or market_id is None:
        return
    elapsed_ms = max(
        0,
        min(
            market.duration_ms,
            int(round((ended_epoch - market.window_start_epoch) * 1000)),
        ),
    )
    for tracker in trackers.values():
        tracker.close(database, elapsed_ms)
    database.finish_market(market_id, ended_epoch)
    database.commit()


def rotate_daily_database(
    database: DailyDatabase,
    previous_date: date,
    next_date: date,
    scan_interval: float,
    thresholds: tuple[float, ...],
    market: MarketContext | None,
    market_id: int | None,
    trackers: dict[str, OutcomeTracker],
) -> tuple[DailyDatabase, Path, float]:
    previous_day_end = local_day_end_epoch(previous_date)
    finish_market_observation(
        database,
        market,
        market_id,
        trackers,
        previous_day_end,
    )
    database.close(previous_day_end)

    next_path = daily_database_path(next_date)
    next_database = DailyDatabase(
        next_path,
        next_date,
        scan_interval,
        thresholds,
    )
    return next_database, next_path, local_day_end_epoch(next_date)


def main() -> int:
    try:
        thresholds, thresholds_micros = validate_settings()
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    started_local = datetime.now().astimezone()
    dataset_date = started_local.date()
    day_end_epoch = local_day_end_epoch(dataset_date)
    database_path = daily_database_path(dataset_date)

    try:
        database: DailyDatabase | None = DailyDatabase(
            database_path,
            dataset_date,
            SCAN_INTERVAL_SECONDS,
            thresholds,
        )
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(f"ERROR: could not open dataset: {exc}", file=sys.stderr)
        return 1

    market: MarketContext | None = None
    market_id: int | None = None
    trackers: dict[str, OutcomeTracker] = {}
    status = "starting"
    stop_message = "stopped"
    exit_code = 0
    runtime_printout = bool(RUNTIME_PRINTOUT)
    last_saved_path = database_path
    next_scan_monotonic = time.monotonic()

    executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="book-scan")
    if not runtime_printout:
        render_paused_notice()
    try:
        while True:
            runtime_printout, toggled = poll_runtime_printout(runtime_printout)
            if toggled:
                if runtime_printout:
                    render_dashboard(
                        dataset_date,
                        database_path,
                        SCAN_INTERVAL_SECONDS,
                        thresholds,
                        market,
                        trackers,
                        status,
                        runtime_printout,
                    )
                else:
                    render_paused_notice()

            now = time.time()
            if now >= day_end_epoch or datetime.now().astimezone().date() != dataset_date:
                assert database is not None
                previous_date = dataset_date
                previous_database = database
                previous_market = market
                previous_market_id = market_id
                previous_trackers = trackers
                database = None
                last_saved_path = database_path
                market = None
                market_id = None

                dataset_date = datetime.now().astimezone().date()
                try:
                    database, database_path, day_end_epoch = rotate_daily_database(
                        previous_database,
                        previous_date,
                        dataset_date,
                        SCAN_INTERVAL_SECONDS,
                        thresholds,
                        previous_market,
                        previous_market_id,
                        previous_trackers,
                    )
                except (OSError, sqlite3.Error, ValueError) as exc:
                    stop_message = (
                        f"daily rotation failed after saving {previous_date}: {exc}"
                    )
                    exit_code = 1
                    break

                trackers = {}
                status = f"started new daily dataset after {previous_date}"
                next_scan_monotonic = time.monotonic()
                continue

            assert database is not None

            if market is not None and now >= market.end_epoch:
                assert market_id is not None
                for tracker in trackers.values():
                    tracker.close(database, market.duration_ms)
                database.finish_market(market_id, market.end_epoch)
                database.commit()
                market = None
                market_id = None
                trackers = {}

            if market is None:
                try:
                    selected_market = find_current_market()
                    selected_context = market_context(selected_market)
                except (OSError, ValueError, LookupError) as exc:
                    status = f"market lookup failed: {type(exc).__name__}: {exc}"
                    render_dashboard(
                        dataset_date,
                        database_path,
                        SCAN_INTERVAL_SECONDS,
                        thresholds,
                        None,
                        {},
                        status,
                        runtime_printout,
                    )
                    time.sleep(MARKET_RETRY_DELAY_SECONDS)
                    continue
                else:
                    if (
                        datetime.now().astimezone().date() != dataset_date
                        or selected_context.window_start_epoch >= day_end_epoch
                    ):
                        continue
                    market = selected_context
                    market_id = database.start_market(market, time.time())
                    trackers = {
                        outcome: OutcomeTracker(outcome, thresholds_micros)
                        for outcome in OUTCOMES
                    }
                    status = "market selected; waiting for first books"
                    next_scan_monotonic = time.monotonic()

            assert market is not None and market_id is not None
            futures = {
                outcome: executor.submit(fetch_book_top, market.token_ids[outcome])
                for outcome in OUTCOMES
            }
            errors: list[str] = []
            for outcome, future in futures.items():
                try:
                    book = future.result()
                except (OSError, ValueError, TimeoutError) as exc:
                    observed_epoch_ms = time.time_ns() // 1_000_000
                    trackers[outcome].data_gap(
                        database,
                        market_id,
                        market,
                        observed_epoch_ms,
                    )
                    errors.append(f"{outcome}: {type(exc).__name__}: {exc}")
                else:
                    if book.received_epoch_ms >= int(round(market.end_epoch * 1000)):
                        trackers[outcome].close(database, market.duration_ms)
                    else:
                        trackers[outcome].observe(
                            database,
                            market_id,
                            market,
                            book,
                        )

            database.commit()
            status = "; ".join(errors) if errors else "scanning"
            render_dashboard(
                dataset_date,
                database_path,
                SCAN_INTERVAL_SECONDS,
                thresholds,
                market,
                trackers,
                status,
                runtime_printout,
            )

            next_scan_monotonic += SCAN_INTERVAL_SECONDS
            remaining = next_scan_monotonic - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            else:
                next_scan_monotonic = time.monotonic()

    except KeyboardInterrupt:
        stop_message = "Ctrl+C received"
    finally:
        if database is not None:
            ended_epoch = min(time.time(), day_end_epoch)
            finish_market_observation(
                database,
                market,
                market_id,
                trackers,
                ended_epoch,
            )
            database.close(ended_epoch)
            last_saved_path = database_path
        executor.shutdown(wait=True, cancel_futures=True)

    if runtime_printout:
        sys.stdout.write("\n")
    print(f"{stop_message}. Dataset saved: {last_saved_path}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
