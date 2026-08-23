import asyncio
import csv
import json
import time
from collections import deque, OrderedDict
from datetime import datetime, timedelta
from pathlib import Path

import requests
import websockets
from web3 import Web3


# ============================================================
# CONFIG
# ============================================================

# Paste your Alchemy Polygon Mainnet WebSocket endpoint here.
# Example:
# POLYGON_WS = "wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"
POLYGON_WS = "wss://polygon-mainnet.g.alchemy.com/v2/alch_j_FMlUK3FPov0DuI8KEpT"

# HTTP RPC endpoint derived from the same Alchemy key.
POLYGON_HTTP = POLYGON_WS.replace("wss://", "https://")

# Polymarket public API.
GAMMA_API = "https://gamma-api.polymarket.com"

# Wallets to watch.
WATCHED = {
    # "pspspsps5": "0xb0f85baa97990910a3e8ac2b4a58a322f01ecef5",
    "std0": "0xdf7930e89a2c47560165331863c31deca0733dcd",
}

# Polymarket V2 exchange contracts on Polygon.
EXCHANGES = {
    "CTF_EXCHANGE_V2": "0xE111180000d2663C0091e4f400237545B87B996B",
    "NEG_RISK_EXCHANGE_V2": "0xe2222d279d744050d28e00520010520000310F59",
}

# In Polymarket V2 OrderFilled, filtering the indexed maker field is the
# cleanest way to track the watched wallet's own order fills.
#
# Leave this False unless you specifically want to ALSO listen for events
# where the watched wallet appears in the event's taker field.
WATCH_TAKER_FIELD_TOO = False

# First time a new token is seen:
#   1) immediately print a fast "RECEIVED" line
#   2) resolve the market name via Gamma in the background
#   3) print the full resolved trade
PRINT_FAST_LINE_ON_CACHE_MISS = True

# Request timeouts.
GAMMA_TIMEOUT_SECONDS = 5
BLOCK_RPC_TIMEOUT_SECONDS = 2

# Limit token -> market mappings in RAM.
# Oldest/least-recently-used tokens are evicted automatically.
MAX_TOKEN_CACHE = 5_000

# Keep only a bounded amount of duplicate-event history in RAM.
MAX_SEEN_EVENTS = 10_000

# CSV logging.
# Logs live in a /log subfolder next to this Python file.
LOG_DIR = Path(__file__).resolve().parent / "log"

# On Ctrl+C, wait briefly for already-received fills to finish metadata/block
# lookup and be written to CSV before closing the active hourly file.
SHUTDOWN_DRAIN_SECONDS = 5


# ============================================================
# GLOBAL RAM STATE
# ============================================================

# RAM ONLY LRU CACHE.
#
# token_id -> market metadata
#
# This cache disappears completely when the program exits.
# It is also capped at MAX_TOKEN_CACHE entries so it cannot grow forever.
token_map = OrderedDict()

# If multiple fills for the same previously unseen token arrive at once,
# only launch one Gamma lookup for that token.
resolution_tasks = {}

# block_number -> timestamp
block_timestamp_cache = {}

# Duplicate event filter.
seen_event_keys = set()
seen_event_queue = deque()

# Fill-processing tasks currently running. Used for graceful Ctrl+C shutdown.
active_fill_tasks = set()


# ============================================================
# HOURLY CSV LOGGER
# ============================================================

CSV_FIELDS = [
    "detect_datetime_local",
    "detect_timestamp",
    "trader",
    "matched_field",
    "side",
    "outcome",
    "market",
    "asset",
    "duration",
    "price_to_beat",
    "market_slug",
    "condition_id",
    "mapping_source",
    "shares",
    "price",
    "cash",
    "fee",
    "token_id",
    "maker",
    "taker",
    "contract_name",
    "contract",
    "block",
    "block_time_local",
    "block_age_seconds",
    "log_index",
    "tx",
]


class HourlyCSVLogger:
    """
    Keeps exactly one "current hour" CSV open.

    Files are stored beside this script in:
        ./log/

    Example:
        ./log/trades_2026-08-23_00.csv
        ./log/trades_2026-08-23_01.csv

    Behavior:
      - every record is flushed immediately
      - at the next exact local-clock hour, old file is flushed/closed
      - a new hourly CSV is opened
      - Ctrl+C flushes/closes the current file
      - if the program restarts during the same hour, it APPENDS to that
        hour's existing file rather than overwriting it
    """

    def __init__(self, log_dir):
        self.log_dir = Path(log_dir)
        self.file = None
        self.writer = None
        self.current_hour = None
        self.current_path = None
        self.lock = asyncio.Lock()
        self.rollover_task = None
        self.closed = False

    @staticmethod
    def hour_key(dt):
        return dt.replace(
            minute=0,
            second=0,
            microsecond=0,
        )

    def path_for_hour(self, hour_dt):
        return self.log_dir / (
            f"trades_{hour_dt.strftime('%Y-%m-%d_%H')}.csv"
        )

    def _open_for_hour_sync(self, hour_dt):
        self.log_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        path = self.path_for_hour(hour_dt)

        has_data = (
            path.exists()
            and path.stat().st_size > 0
        )

        # utf-8-sig makes the CSV convenient to open in Excel.
        f = path.open(
            "a",
            newline="",
            encoding="utf-8-sig",
            buffering=1,
        )

        writer = csv.DictWriter(
            f,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore",
        )

        if not has_data:
            writer.writeheader()
            f.flush()

        self.file = f
        self.writer = writer
        self.current_hour = hour_dt
        self.current_path = path

    def _close_sync(self):
        if self.file is not None:
            try:
                self.file.flush()
            finally:
                self.file.close()

        self.file = None
        self.writer = None
        self.current_path = None

    async def start(self):
        async with self.lock:
            now_hour = self.hour_key(
                datetime.now()
            )
            self._open_for_hour_sync(
                now_hour
            )

        self.rollover_task = asyncio.create_task(
            self._rollover_loop()
        )

        print(
            f"CSV logging:   {self.current_path}",
            flush=True,
        )

    async def write_row(self, row, event_dt):
        """
        Write a completed trade row.

        event_dt is the ORIGINAL local Polygon-detection datetime. If metadata
        lookup finishes just after the hour changes, the row is still appended
        to the correct previous-hour CSV.
        """
        if self.closed:
            return

        event_hour = self.hour_key(
            event_dt
        )

        async with self.lock:
            if self.closed:
                return

            # Normal case: event belongs to currently open hour.
            if event_hour == self.current_hour:
                self.writer.writerow(row)
                self.file.flush()
                return

            # A fill detected in the previous hour may finish Gamma resolution
            # just after rollover. Append it to its proper older file.
            if (
                self.current_hour is not None
                and event_hour < self.current_hour
            ):
                path = self.path_for_hour(
                    event_hour
                )

                self.log_dir.mkdir(
                    parents=True,
                    exist_ok=True,
                )

                has_data = (
                    path.exists()
                    and path.stat().st_size > 0
                )

                with path.open(
                    "a",
                    newline="",
                    encoding="utf-8-sig",
                ) as f:
                    writer = csv.DictWriter(
                        f,
                        fieldnames=CSV_FIELDS,
                        extrasaction="ignore",
                    )

                    if not has_data:
                        writer.writeheader()

                    writer.writerow(row)
                    f.flush()

                return

            # If the clock jumped forward or the rollover coroutine woke late,
            # rotate here before writing.
            self._close_sync()
            self._open_for_hour_sync(
                event_hour
            )

            self.writer.writerow(row)
            self.file.flush()

    async def rotate_to_hour(self, new_hour):
        if self.closed:
            return

        async with self.lock:
            if self.closed:
                return

            if self.current_hour == new_hour:
                return

            old_path = self.current_path

            self._close_sync()
            self._open_for_hour_sync(
                new_hour
            )

            print(
                f"[CSV] saved hourly log: {old_path}",
                flush=True,
            )
            print(
                f"[CSV] new hourly log:   {self.current_path}",
                flush=True,
            )

    async def _rollover_loop(self):
        try:
            while not self.closed:
                now = datetime.now()

                next_hour = (
                    now.replace(
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                    + timedelta(hours=1)
                )

                sleep_seconds = max(
                    0.0,
                    (next_hour - now).total_seconds(),
                )

                await asyncio.sleep(
                    sleep_seconds
                )

                if self.closed:
                    break

                await self.rotate_to_hour(
                    self.hour_key(
                        datetime.now()
                    )
                )

        except asyncio.CancelledError:
            pass

    async def close(self):
        if self.closed:
            return

        self.closed = True

        if self.rollover_task is not None:
            self.rollover_task.cancel()

            try:
                await self.rollover_task
            except asyncio.CancelledError:
                pass

        async with self.lock:
            path = self.current_path
            self._close_sync()

        if path is not None:
            print(
                f"[CSV] final log saved:   {path}",
                flush=True,
            )


csv_logger = HourlyCSVLogger(LOG_DIR)


# ============================================================
# ORDERFILLED EVENT TOPIC
# ============================================================

EVENT_SIGNATURE = (
    "OrderFilled("
    "bytes32,"
    "address,"
    "address,"
    "uint8,"
    "uint256,"
    "uint256,"
    "uint256,"
    "uint256,"
    "bytes32,"
    "bytes32"
    ")"
)


def make_topic(signature):
    h = Web3.keccak(text=signature).hex()

    if not h.startswith("0x"):
        h = "0x" + h

    return h


ORDER_FILLED_TOPIC = make_topic(EVENT_SIGNATURE)


# ============================================================
# BASIC HELPERS
# ============================================================

def _parse_jsonish(value):
    """
    Gamma may return array fields as either:
        ["Up", "Down"]
    or:
        '["Up", "Down"]'
    """
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    return []


def address_topic(address):
    clean = address.lower().replace("0x", "")

    if len(clean) != 40:
        raise ValueError(
            f"Invalid Ethereum address: {address}"
        )

    return "0x" + ("0" * 24) + clean


def topic_to_address(topic):
    clean = topic.lower().replace("0x", "")
    return "0x" + clean[-40:]


def hex_to_int(value):
    if isinstance(value, int):
        return value

    return int(value, 16)


def opposite_side(side):
    if side == "BUY":
        return "SELL"

    if side == "SELL":
        return "BUY"

    return side


def shorten_token(token_id, front=10, back=8):
    token_id = str(token_id)

    if len(token_id) <= front + back + 3:
        return token_id

    return f"{token_id[:front]}...{token_id[-back:]}"


# ============================================================
# TOKEN CACHE HELPERS
# ============================================================

def get_cached_token(token_id):
    """
    Return cached token metadata and mark it as recently used.
    """
    token_id = str(token_id)

    info = token_map.get(token_id)

    if info is not None:
        token_map.move_to_end(token_id)

    return info


def cache_market_mapping(mapping):
    """
    Add all token mappings returned for one market.

    The cache behaves as an LRU cache:
      - existing/hit tokens move to the newest end
      - newly resolved tokens are appended at the newest end
      - oldest tokens are evicted when MAX_TOKEN_CACHE is exceeded
    """
    for token_id, info in mapping.items():

        token_id = str(token_id)

        if token_id in token_map:
            token_map.move_to_end(token_id)

        token_map[token_id] = info

    while len(token_map) > MAX_TOKEN_CACHE:
        token_map.popitem(last=False)


# ============================================================
# EXACT TOKEN -> MARKET RESOLUTION
# ============================================================

def fetch_gamma_mapping(token_id):
    """
    Exact token lookup:

        GET /markets?clob_token_ids=<tokenId>

    One returned market normally contains BOTH outcome tokens.
    Therefore one Gamma request lets us cache both Up and Down.
    """

    token_id = str(token_id)

    response = requests.get(
        f"{GAMMA_API}/markets",
        params={
            "clob_token_ids": token_id,
        },
        timeout=GAMMA_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    markets = response.json()

    if not isinstance(markets, list):
        markets = [markets]

    for market in markets:

        if not isinstance(market, dict):
            continue

        token_ids = [
            str(x)
            for x in _parse_jsonish(
                market.get("clobTokenIds")
            )
        ]

        outcomes = [
            str(x)
            for x in _parse_jsonish(
                market.get("outcomes")
            )
        ]

        # Safety check:
        # make sure Gamma really returned the market containing our token.
        if token_id not in token_ids:
            continue

        title = (
            market.get("question")
            or market.get("title")
            or "UNKNOWN MARKET"
        )

        slug = market.get("slug", "") or ""
        condition_id = market.get("conditionId", "") or ""

        crypto_config = (
            market.get("cryptoMarketConfig")
            or {}
        )

        asset = crypto_config.get("asset")
        duration = crypto_config.get("duration")

        events = market.get("events") or []

        event_slug = ""
        price_to_beat = None
        event_start_time = None

        if isinstance(events, list) and events:

            event = events[0] or {}

            if isinstance(event, dict):

                event_slug = (
                    event.get("slug", "")
                    or ""
                )

                event_start_time = (
                    event.get("startTime")
                )

                event_metadata = (
                    event.get("eventMetadata")
                    or {}
                )

                if isinstance(
                    event_metadata,
                    dict,
                ):
                    price_to_beat = (
                        event_metadata.get(
                            "priceToBeat"
                        )
                    )

        common = {
            "title": title,
            "slug": slug,
            "event_slug": event_slug,
            "condition_id": condition_id,
            "asset": asset,
            "duration": duration,
            "price_to_beat": price_to_beat,
            "event_start_time": event_start_time,
            "end_date": market.get("endDate"),
            "resolution_source": market.get(
                "resolutionSource"
            ),
        }

        mapping = {}

        for i, tid in enumerate(token_ids):

            outcome = (
                outcomes[i]
                if i < len(outcomes)
                else "UNKNOWN"
            )

            mapping[tid] = {
                **common,
                "outcome": outcome,
            }

        return mapping

    return {}


async def resolve_token_exact(token_id):
    """
    Resolve tokenId -> market/outcome without blocking the Polygon websocket.

    Returns:
        (market_info_or_none, source)

    source:
        "memory-cache"
        "gamma"
        "unresolved"
    """

    token_id = str(token_id)

    cached = get_cached_token(token_id)

    if cached is not None:
        return cached, "memory-cache"

    # If a lookup for this exact token is already running,
    # share that same task.
    task = resolution_tasks.get(token_id)

    if task is None:

        task = asyncio.create_task(
            asyncio.to_thread(
                fetch_gamma_mapping,
                token_id,
            )
        )

        resolution_tasks[token_id] = task

    try:

        mapping = await task

    except Exception as e:

        print(
            f"[market lookup error] "
            f"token {shorten_token(token_id)}: "
            f"{e}",
            flush=True,
        )

        return None, "unresolved"

    finally:

        if resolution_tasks.get(token_id) is task:
            resolution_tasks.pop(
                token_id,
                None,
            )

    if not mapping:
        return None, "unresolved"

    # IMPORTANT:
    # Cache all outcome tokens returned for the market.
    #
    # For a normal crypto Up/Down market this means one Gamma lookup
    # caches both the Up token and the Down token.
    cache_market_mapping(mapping)

    return get_cached_token(token_id), "gamma"


def unknown_market():
    return {
        "title": "UNKNOWN MARKET",
        "outcome": "?",
        "slug": "",
        "event_slug": "",
        "condition_id": "",
        "asset": None,
        "duration": None,
        "price_to_beat": None,
        "event_start_time": None,
        "end_date": None,
        "resolution_source": None,
    }


# ============================================================
# BLOCK TIMESTAMP
# ============================================================

def get_block_timestamp(block_number):
    """
    Fetch Polygon block timestamp from Alchemy.

    This is only a ROUGH time reference.

    A slightly negative:
        local_receive_time - block_timestamp
    is possible because the clocks are not perfectly synchronized and
    block timestamps have only second-level resolution.
    """

    if block_number in block_timestamp_cache:
        return block_timestamp_cache[
            block_number
        ]

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [
            hex(block_number),
            False,
        ],
    }

    response = requests.post(
        POLYGON_HTTP,
        json=payload,
        timeout=BLOCK_RPC_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    data = response.json()

    if (
        "result" not in data
        or data["result"] is None
    ):
        raise RuntimeError(
            f"Could not get block "
            f"{block_number}"
        )

    timestamp = int(
        data["result"]["timestamp"],
        16,
    )

    block_timestamp_cache[
        block_number
    ] = timestamp

    # Keep this RAM cache bounded.
    if len(block_timestamp_cache) > 1000:

        oldest = sorted(
            block_timestamp_cache.keys()
        )[:500]

        for key in oldest:
            block_timestamp_cache.pop(
                key,
                None,
            )

    return timestamp


# ============================================================
# DECODE ORDERFILLED
# ============================================================

def decode_order_filled(log):

    topics = log["topics"]

    if len(topics) < 4:
        raise ValueError(
            f"Unexpected topic count: "
            f"{len(topics)}"
        )

    order_hash = topics[1]

    maker = topic_to_address(
        topics[2]
    )

    taker = topic_to_address(
        topics[3]
    )

    data_hex = log["data"]

    if data_hex.startswith("0x"):
        data_hex = data_hex[2:]

    raw = bytes.fromhex(
        data_hex
    )

    words = [
        raw[i:i + 32]
        for i in range(
            0,
            len(raw),
            32,
        )
    ]

    if len(words) < 7:
        raise ValueError(
            f"Expected 7 data words, "
            f"got {len(words)}"
        )

    side_raw = int.from_bytes(
        words[0],
        "big",
    )

    token_id = int.from_bytes(
        words[1],
        "big",
    )

    maker_amount_raw = int.from_bytes(
        words[2],
        "big",
    )

    taker_amount_raw = int.from_bytes(
        words[3],
        "big",
    )

    fee_raw = int.from_bytes(
        words[4],
        "big",
    )

    builder = (
        "0x"
        + words[5].hex()
    )

    metadata = (
        "0x"
        + words[6].hex()
    )

    if side_raw == 0:
        order_side = "BUY"

    elif side_raw == 1:
        order_side = "SELL"

    else:
        order_side = (
            f"UNKNOWN({side_raw})"
        )

    # V2 pUSD / CTF fill accounting uses 6 decimals.
    maker_amount = (
        maker_amount_raw
        / 1_000_000
    )

    taker_amount = (
        taker_amount_raw
        / 1_000_000
    )

    fee = (
        fee_raw
        / 1_000_000
    )

    # side describes the order represented by this OrderFilled event.
    if order_side == "BUY":

        cash = maker_amount
        shares = taker_amount

    elif order_side == "SELL":

        shares = maker_amount
        cash = taker_amount

    else:

        shares = 0
        cash = 0

    price = (
        cash / shares
        if shares > 0
        else 0
    )

    return {
        "order_hash": order_hash,
        "maker": maker,
        "taker": taker,
        "order_side": order_side,
        "token_id": str(token_id),
        "shares": shares,
        "cash": cash,
        "price": price,
        "fee": fee,
        "builder": builder,
        "metadata": metadata,
        "tx": log.get(
            "transactionHash",
            "",
        ),
        "block": hex_to_int(
            log.get(
                "blockNumber",
                "0x0",
            )
        ),
        "log_index": hex_to_int(
            log.get(
                "logIndex",
                "0x0",
            )
        ),
        "contract": log.get(
            "address",
            "",
        ),
    }


def watched_trader_side(
    fill,
    matched_field,
):
    """
    MAKER subscription:
        the event side directly describes the watched wallet's order.

    TAKER subscription:
        the event describes the other order, so the watched trader is
        on the opposite side.
    """

    if matched_field == "MAKER":
        return fill["order_side"]

    if matched_field == "TAKER":
        return opposite_side(
            fill["order_side"]
        )

    return fill["order_side"]


# ============================================================
# DUPLICATE EVENT FILTER
# ============================================================

def remember_event(log):

    tx = log.get(
        "transactionHash",
        "",
    )

    log_index = log.get(
        "logIndex",
        "",
    )

    key = f"{tx}:{log_index}"

    if key in seen_event_keys:
        return False

    seen_event_keys.add(
        key
    )

    seen_event_queue.append(
        key
    )

    while (
        len(seen_event_queue)
        > MAX_SEEN_EVENTS
    ):
        old = seen_event_queue.popleft()

        seen_event_keys.discard(
            old
        )

    return True


# ============================================================
# FILL TASK TRACKING / GRACEFUL SHUTDOWN
# ============================================================

def track_fill_task(task):
    active_fill_tasks.add(task)
    task.add_done_callback(
        active_fill_tasks.discard
    )


async def drain_fill_tasks():
    """
    Give already-received trades a short grace period to finish Gamma/block
    lookups and write complete rows before the current CSV is closed.
    """
    if not active_fill_tasks:
        return

    pending = list(active_fill_tasks)

    print(
        f"Draining {len(pending)} in-flight trade task(s)...",
        flush=True,
    )

    try:
        await asyncio.wait_for(
            asyncio.gather(
                *pending,
                return_exceptions=True,
            ),
            timeout=SHUTDOWN_DRAIN_SECONDS,
        )

    except asyncio.TimeoutError:
        remaining = [
            task
            for task in pending
            if not task.done()
        ]

        print(
            f"Shutdown drain timeout; cancelling "
            f"{len(remaining)} remaining task(s).",
            flush=True,
        )

        for task in remaining:
            task.cancel()

        await asyncio.gather(
            *remaining,
            return_exceptions=True,
        )


# ============================================================
# PRINTING
# ============================================================

def print_fast_unresolved(
    trader_name,
    matched_field,
    fill,
    receive_timestamp,
):

    detect_time = (
        datetime.fromtimestamp(
            receive_timestamp
        ).strftime(
            "%H:%M:%S.%f"
        )[:-3]
    )

    side = watched_trader_side(
        fill,
        matched_field,
    )

    print(
        f"[{detect_time}] "
        f"RECEIVED "
        f"{trader_name} | "
        f"{matched_field} | "
        f"{side} | "
        f"{fill['shares']:.6f} sh "
        f"@ ~{fill['price']:.4f} | "
        f"${fill['cash']:.4f} | "
        f"token "
        f"{shorten_token(fill['token_id'])} | "
        f"resolving market...",
        flush=True,
    )


async def handle_fill(
    trader_name,
    matched_field,
    contract_name,
    fill,
    receive_timestamp,
):

    detect_time = (
        datetime.fromtimestamp(
            receive_timestamp
        ).strftime(
            "%H:%M:%S.%f"
        )[:-3]
    )

    token_id = fill[
        "token_id"
    ]

    already_cached = (
        token_id in token_map
    )

    if (
        not already_cached
        and PRINT_FAST_LINE_ON_CACHE_MISS
    ):
        print_fast_unresolved(
            trader_name,
            matched_field,
            fill,
            receive_timestamp,
        )

    # Run market lookup and block timestamp lookup concurrently.
    market_task = asyncio.create_task(
        resolve_token_exact(
            token_id
        )
    )

    block_task = asyncio.create_task(
        asyncio.to_thread(
            get_block_timestamp,
            fill["block"],
        )
    )

    market = None
    market_source = "unresolved"

    try:

        (
            market,
            market_source,
        ) = await market_task

    except Exception as e:

        print(
            f"[market task error] "
            f"{e}",
            flush=True,
        )

    if market is None:
        market = unknown_market()

    block_age = None
    block_time = "UNKNOWN"

    try:

        block_timestamp = (
            await block_task
        )

        block_age = (
            receive_timestamp
            - block_timestamp
        )

        block_time = (
            datetime.fromtimestamp(
                block_timestamp
            ).strftime(
                "%H:%M:%S"
            )
        )

    except Exception as e:

        print(
            f"[block timestamp error] "
            f"{e}",
            flush=True,
        )

    side = watched_trader_side(
        fill,
        matched_field,
    )

    # --------------------------------------------------------
    # CSV RECORD
    # --------------------------------------------------------

    event_dt = datetime.fromtimestamp(
        receive_timestamp
    )

    csv_row = {
        "detect_datetime_local": event_dt.strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3],
        "detect_timestamp": f"{receive_timestamp:.6f}",
        "trader": trader_name,
        "matched_field": matched_field,
        "side": side,
        "outcome": market.get("outcome") or "",
        "market": market.get("title") or "",
        "asset": (
            str(market.get("asset")).upper()
            if market.get("asset")
            else ""
        ),
        "duration": market.get("duration") or "",
        "price_to_beat": (
            market.get("price_to_beat")
            if market.get("price_to_beat") is not None
            else ""
        ),
        "market_slug": market.get("slug") or "",
        "condition_id": market.get("condition_id") or "",
        "mapping_source": market_source,
        "shares": f"{fill['shares']:.6f}",
        "price": f"{fill['price']:.6f}",
        "cash": f"{fill['cash']:.6f}",
        "fee": f"{fill['fee']:.6f}",
        "token_id": fill["token_id"],
        "maker": fill["maker"],
        "taker": fill["taker"],
        "contract_name": contract_name,
        "contract": fill["contract"],
        "block": fill["block"],
        "block_time_local": block_time,
        "block_age_seconds": (
            f"{block_age:.6f}"
            if block_age is not None
            else ""
        ),
        "log_index": fill["log_index"],
        "tx": fill["tx"],
    }

    await csv_logger.write_row(
        csv_row,
        event_dt,
    )

    print()
    print(
        "=" * 120
    )

    print(
        f"[{detect_time}] "
        f"{trader_name} | "
        f"{matched_field} | "
        f"{side} "
        f"{market['outcome']} | "
        f"{contract_name}"
    )

    print(
        f"    MARKET:       "
        f"{market['title']}"
    )

    if (
        market.get("asset")
        or market.get("duration")
    ):

        asset = (
            str(
                market.get("asset")
            ).upper()
            if market.get("asset")
            else "?"
        )

        duration = (
            market.get("duration")
            or "?"
        )

        print(
            f"    TYPE:         "
            f"{asset} | "
            f"{duration}"
        )

    if (
        market.get("price_to_beat")
        is not None
    ):
        print(
            f"    PRICE TO BEAT:"
            f" "
            f"{market['price_to_beat']}"
        )

    if market.get("slug"):
        print(
            f"    market slug:  "
            f"{market['slug']}"
        )

    if market.get(
        "condition_id"
    ):
        print(
            f"    condition id: "
            f"{market['condition_id']}"
        )

    print(
        f"    mapping:      "
        f"{market_source}"
    )

    print(
        f"    RAM mappings: "
        f"{len(token_map)}"
        f"/{MAX_TOKEN_CACHE}"
    )

    print(
        f"    block time:   "
        f"{block_time}"
    )

    print(
        f"    detect time:  "
        f"{detect_time}"
    )

    if block_age is None:

        print(
            "    BLOCK AGE:    "
            "UNKNOWN"
        )

    else:

        print(
            f"    BLOCK AGE:    "
            f"{block_age:+.3f} s "
            f"(rough; clocks/timestamps "
            f"can differ)"
        )

    print(
        f"    shares:       "
        f"{fill['shares']:.6f}"
    )

    print(
        f"    price:        "
        f"~{fill['price']:.6f}"
    )

    print(
        f"    cash:         "
        f"${fill['cash']:.6f}"
    )

    print(
        f"    fee:          "
        f"{fill['fee']:.6f}"
    )

    print(
        f"    tokenId:      "
        f"{fill['token_id']}"
    )

    print(
        f"    maker:        "
        f"{fill['maker']}"
    )

    print(
        f"    taker:        "
        f"{fill['taker']}"
    )

    print(
        f"    contract:     "
        f"{fill['contract']}"
    )

    print(
        f"    block:        "
        f"{fill['block']}"
    )

    print(
        f"    log index:    "
        f"{fill['log_index']}"
    )

    print(
        f"    tx:           "
        f"{fill['tx']}"
    )

    print(
        "=" * 120,
        flush=True,
    )


# ============================================================
# SUBSCRIPTION BUILDING
# ============================================================

def build_subscription_requests():
    """
    Build every subscription first.

    Then the websocket setup code sends all requests and matches JSON-RPC
    responses by request id instead of assuming the next websocket message
    must be the response to the request just sent.
    """

    requests_to_send = []

    request_id = 1

    roles = [
        "MAKER"
    ]

    if WATCH_TAKER_FIELD_TOO:
        roles.append(
            "TAKER"
        )

    for (
        trader_name,
        wallet,
    ) in WATCHED.items():

        wallet_topic = (
            address_topic(
                wallet
            )
        )

        for (
            contract_name,
            contract_address,
        ) in EXCHANGES.items():

            for role in roles:

                if role == "MAKER":

                    topics = [
                        ORDER_FILLED_TOPIC,
                        None,
                        wallet_topic,
                    ]

                else:

                    topics = [
                        ORDER_FILLED_TOPIC,
                        None,
                        None,
                        wallet_topic,
                    ]

                rpc = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address":
                                contract_address,
                            "topics":
                                topics,
                        },
                    ],
                }

                meta = {
                    "trader_name":
                        trader_name,
                    "role":
                        role,
                    "contract_name":
                        contract_name,
                }

                requests_to_send.append(
                    (
                        request_id,
                        rpc,
                        meta,
                    )
                )

                request_id += 1

    return requests_to_send


async def subscribe_wallets(ws):

    request_specs = (
        build_subscription_requests()
    )

    pending = {}
    subscriptions = {}

    # Very unlikely, but if a notification arrives while subscription
    # responses are still being collected, preserve it.
    buffered_notifications = []

    for (
        request_id,
        rpc,
        meta,
    ) in request_specs:

        pending[
            request_id
        ] = meta

        await ws.send(
            json.dumps(
                rpc
            )
        )

    while pending:

        raw = await ws.recv()

        receive_timestamp = (
            time.time()
        )

        message = json.loads(
            raw
        )

        # JSON-RPC response to eth_subscribe.
        if (
            "id" in message
            and message.get("id")
            in pending
        ):

            request_id = message[
                "id"
            ]

            meta = pending.pop(
                request_id
            )

            if "result" in message:

                subscription_id = (
                    message["result"]
                )

                subscriptions[
                    subscription_id
                ] = (
                    meta["trader_name"],
                    meta["role"],
                    meta["contract_name"],
                )

                print(
                    f"[OK] "
                    f"{meta['trader_name']:<15} "
                    f"{meta['contract_name']:<24} "
                    f"{meta['role']:<6} "
                    f"{subscription_id}"
                )

            else:

                print()
                print(
                    "[ERROR] "
                    "subscription failed"
                )

                print(
                    f"Trader:   "
                    f"{meta['trader_name']}"
                )

                print(
                    f"Contract: "
                    f"{meta['contract_name']}"
                )

                print(
                    f"Role:     "
                    f"{meta['role']}"
                )

                print(
                    "Alchemy response:"
                )

                print(
                    json.dumps(
                        message,
                        indent=2,
                    )
                )

                print()

            continue

        if (
            message.get("method")
            == "eth_subscription"
        ):
            buffered_notifications.append(
                (
                    message,
                    receive_timestamp,
                )
            )

    return (
        subscriptions,
        buffered_notifications,
    )


# ============================================================
# EVENT DISPATCH
# ============================================================

def dispatch_subscription_message(
    message,
    receive_timestamp,
    subscriptions,
):

    if (
        message.get("method")
        != "eth_subscription"
    ):
        return

    params = message.get(
        "params",
        {},
    )

    subscription_id = (
        params.get(
            "subscription"
        )
    )

    if (
        subscription_id
        not in subscriptions
    ):
        return

    log = params.get(
        "result"
    )

    if not log:
        return

    if not remember_event(
        log
    ):
        return

    (
        trader_name,
        role,
        contract_name,
    ) = subscriptions[
        subscription_id
    ]

    try:

        fill = (
            decode_order_filled(
                log
            )
        )

        # Everything involving Gamma or block RPC runs outside the
        # websocket receive critical path.
        task = asyncio.create_task(
            handle_fill(
                trader_name,
                role,
                contract_name,
                fill,
                receive_timestamp,
            )
        )

        track_fill_task(task)

    except Exception as e:

        print(
            f"[decode error] "
            f"{e}",
            flush=True,
        )


# ============================================================
# DEBUG / STARTUP
# ============================================================

def print_debug_config():

    print(
        "=" * 100
    )

    print(
        "DEBUG CONFIG"
    )

    print(
        "=" * 100
    )

    print(
        f"ORDER_FILLED_TOPIC:\n"
        f"{ORDER_FILLED_TOPIC}"
    )

    print(
        f"topic length: "
        f"{len(ORDER_FILLED_TOPIC)} chars"
    )

    print()

    for (
        name,
        wallet,
    ) in WATCHED.items():

        wt = address_topic(
            wallet
        )

        print(
            f"{name} wallet:\n"
            f"{wallet}"
        )

        print(
            f"wallet topic:\n"
            f"{wt}"
        )

        print(
            f"wallet topic length: "
            f"{len(wt)} chars"
        )

        print()

    print(
        f"WATCH_TAKER_FIELD_TOO: "
        f"{WATCH_TAKER_FIELD_TOO}"
    )

    print(
        "TOKEN CACHE: RAM ONLY"
    )

    print(
        f"Token cache limit: "
        f"{MAX_TOKEN_CACHE}"
    )

    print(
        "Persistent token cache: DISABLED"
    )

    print(
        f"CSV log folder: {LOG_DIR}"
    )

    print(
        "CSV rotation: every local-clock hour"
    )

    print(
        "=" * 100
    )

    print()


async def watch():

    global POLYGON_HTTP

    if not POLYGON_WS:

        raise RuntimeError(
            "POLYGON_WS is blank. "
            "Paste your Alchemy Polygon "
            "Mainnet WSS endpoint into "
            "POLYGON_WS."
        )

    POLYGON_HTTP = (
        POLYGON_WS.replace(
            "wss://",
            "https://",
        )
    )

    print_debug_config()

    print(
        "Market mapping mode:"
    )

    print(
        "    Polygon tokenId"
    )

    print(
        "        -> RAM cache"
    )

    print(
        "        -> exact Gamma lookup "
        "if missing"
    )

    print(
        "        -> cache all outcome tokens "
        "for that market in RAM"
    )

    print()

    print(
        "No full-market scan."
    )

    print(
        "No 60-second refresh."
    )

    print(
        "No token_cache.json."
    )

    print(
        "Token mappings are LRU-capped at "
        f"{MAX_TOKEN_CACHE} entries and "
        "disappear when this program exits."
    )

    print()

    await csv_logger.start()

    print()

    while True:

        try:

            print(
                "Connecting to Polygon..."
            )

            async with (
                websockets.connect(
                    POLYGON_WS,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=None,
                )
                as ws
            ):

                print(
                    "Connected."
                )

                (
                    subscriptions,
                    buffered_notifications,
                ) = await subscribe_wallets(
                    ws
                )

                print()
                print(
                    "=" * 120
                )

                print(
                    "POLYGON WATCHER LIVE"
                )

                print(
                    "=" * 120
                )

                print(
                    f"Watching "
                    f"{len(WATCHED)} "
                    f"wallet(s)"
                )

                print()

                for (
                    name,
                    wallet,
                ) in WATCHED.items():

                    print(
                        f"    "
                        f"{name:<20} "
                        f"{wallet}"
                    )

                print()

                print(
                    f"Active subscriptions: "
                    f"{len(subscriptions)}"
                )

                if (
                    len(subscriptions)
                    == 0
                ):

                    print()
                    print(
                        "NO SUBSCRIPTIONS "
                        "SUCCEEDED."
                    )

                    print(
                        "Watcher cannot "
                        "receive events."
                    )

                    await drain_fill_tasks()
                    await csv_logger.close()

                    return

                print()

                print(
                    "Waiting for Polymarket "
                    "OrderFilled events..."
                )

                print()

                # Process any notification that arrived during setup.
                for (
                    message,
                    receive_timestamp,
                ) in buffered_notifications:

                    dispatch_subscription_message(
                        message,
                        receive_timestamp,
                        subscriptions,
                    )

                while True:

                    raw = await ws.recv()

                    # Record arrival immediately.
                    # Do this BEFORE JSON parsing, Gamma lookup,
                    # block timestamp RPC, or console formatting.
                    receive_timestamp = (
                        time.time()
                    )

                    message = json.loads(
                        raw
                    )

                    dispatch_subscription_message(
                        message,
                        receive_timestamp,
                        subscriptions,
                    )

        except KeyboardInterrupt:

            print()

            await drain_fill_tasks()
            await csv_logger.close()

            print(
                "Stopped."
            )

            return

        except asyncio.CancelledError:

            # Python 3.11/3.12 asyncio.run() normally delivers Ctrl+C by
            # cancelling the main coroutine first. Handle that path too so
            # the active hourly CSV is always flushed and closed cleanly.
            print()

            await drain_fill_tasks()
            await csv_logger.close()

            raise

        except Exception as e:

            print()

            print(
                f"[connection error] "
                f"{type(e).__name__}: "
                f"{e}"
            )

            print(
                "Reconnecting in "
                "2 seconds..."
            )

            await asyncio.sleep(
                2
            )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            watch()
        )

    except KeyboardInterrupt:

        print()
        print(
            "Stopped."
        )