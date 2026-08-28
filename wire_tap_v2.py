"""Watch Polymarket OrderFilled events and write compact hourly trade logs.

V2 changes:
* The Polygon endpoint is read from POLYGON_WS instead of source code.
* CSV output contains only human/P&L fields agreed for the compact log.
* processing_latency_seconds uses a monotonic clock and measures the local
  WebSocket-receive -> row-ready interval. It is not mislabeled block age.

PowerShell startup example:
    $env:POLYGON_WS = "wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"
    python wire_tap_v2.py
"""

import asyncio
import csv
import json
import os
import time
from collections import OrderedDict, deque
from datetime import datetime
from pathlib import Path

import requests
import websockets
from web3 import Web3


# ============================================================
# CONFIG
# ============================================================

POLYGON_WS = "wss://polygon-mainnet.g.alchemy.com/v2/alch_j_FMlUK3FPov0DuI8KEpT"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

WATCHED = {
    # "pspspsps5": "0xb0f85baa97990910a3e8ac2b4a58a322f01ecef5",
    # "std0": "0xdf7930e89a2c47560165331863c31deca0733dcd",
    "HighTempTation": "0x6011655c4afb76f36dd1b08a137a1ba73466b31e",
}

EXCHANGES = {
    "CTF_EXCHANGE_V2": "0xE111180000d2663C0091e4f400237545B87B996B",
    "NEG_RISK_EXCHANGE_V2": "0xe2222d279d744050d28e00520010520000310F59",
}

WATCH_TAKER_FIELD_TOO = False
GAMMA_TIMEOUT_SECONDS = 5
TRADE_HISTORY_TIMEOUT_SECONDS = 5
ORDER_BOOK_TIMEOUT_SECONDS = 3
MAX_TOKEN_CACHE = 5_000
MAX_SEEN_EVENTS = 10_000
SHUTDOWN_DRAIN_SECONDS = 5
LOG_DIR = Path(__file__).resolve().parent / "log_v2"

CSV_FIELDS = [
    "detect_datetime_local",
    "trader",
    "processing_latency_seconds",
    "side",
    "outcome",
    "market",
    "duration",
    "market_slug",
    "shares",
    "price",
    "cash",
    "fee",
    "best_bid_price",
    "best_bid_quantity",
    "best_ask_price",
    "best_ask_quantity",
]


# ============================================================
# STATE
# ============================================================

token_map = OrderedDict()
resolution_tasks = {}
seen_event_keys = set()
seen_event_queue = deque()
active_fill_tasks = set()


# ============================================================
# HOURLY CSV LOGGER
# ============================================================

class HourlyCSVLogger:
    def __init__(self, log_dir):
        self.log_dir = Path(log_dir)
        self.lock = asyncio.Lock()
        self.files = {}
        self.closed = False

    @staticmethod
    def hour_key(event_dt):
        return event_dt.strftime("%Y-%m-%d_%H")

    def _writer_for(self, event_dt):
        hour = self.hour_key(event_dt)
        existing = self.files.get(hour)
        if existing is not None:
            return existing

        self.log_dir.mkdir(parents=True, exist_ok=True)
        path = self.log_dir / f"trades_{hour}.csv"
        has_data = path.exists() and path.stat().st_size > 0
        file = path.open(
            "a",
            newline="",
            encoding="utf-8-sig",
            buffering=1,
        )
        writer = csv.DictWriter(
            file,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore",
        )
        if not has_data:
            writer.writeheader()
            file.flush()

        self.files[hour] = (file, writer, path)
        print(f"[CSV] logging: {path}", flush=True)
        return self.files[hour]

    async def write_row(self, row, event_dt):
        async with self.lock:
            if self.closed:
                return
            file, writer, _ = self._writer_for(event_dt)
            writer.writerow(row)
            file.flush()

            # Retain only the current event hour. A late previous-hour fill
            # can safely reopen and append to its original file later.
            current_hour = self.hour_key(event_dt)
            for hour in list(self.files):
                if hour != current_hour:
                    old_file, _, old_path = self.files.pop(hour)
                    old_file.flush()
                    old_file.close()
                    print(f"[CSV] closed:  {old_path}", flush=True)

    async def close(self):
        async with self.lock:
            if self.closed:
                return
            self.closed = True
            for file, _, path in self.files.values():
                file.flush()
                file.close()
                print(f"[CSV] saved:   {path}", flush=True)
            self.files.clear()


csv_logger = HourlyCSVLogger(LOG_DIR)


# ============================================================
# EVENT AND VALUE HELPERS
# ============================================================

EVENT_SIGNATURE = (
    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,"
    "uint256,uint256,bytes32,bytes32)"
)
ORDER_FILLED_TOPIC = Web3.keccak(text=EVENT_SIGNATURE).hex()
if not ORDER_FILLED_TOPIC.startswith("0x"):
    ORDER_FILLED_TOPIC = "0x" + ORDER_FILLED_TOPIC


def _parse_jsonish(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
    return []


def address_topic(address):
    clean = address.lower().removeprefix("0x")
    if len(clean) != 40:
        raise ValueError(f"Invalid Ethereum address: {address}")
    return "0x" + ("0" * 24) + clean


def topic_to_address(topic):
    return "0x" + topic.lower().removeprefix("0x")[-40:]


def hex_to_int(value):
    return value if isinstance(value, int) else int(value, 16)


def opposite_side(side):
    if side == "BUY":
        return "SELL"
    if side == "SELL":
        return "BUY"
    return side


# ============================================================
# STARTUP TRADE-HISTORY CONFIRMATION
# ============================================================

def fetch_latest_previous_trade(wallet):
    """Return the newest public Data API trade for a watched wallet."""
    response = requests.get(
        f"{DATA_API}/trades",
        params={
            "user": wallet,
            "limit": 1,
            # Include trades where the wallet supplied maker liquidity too.
            "takerOnly": "false",
        },
        timeout=TRADE_HISTORY_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    trades = response.json()

    if not isinstance(trades, list):
        raise ValueError("Data API returned an unexpected trade-history response")
    if not trades:
        return None
    if not isinstance(trades[0], dict):
        raise ValueError("Data API returned an invalid trade-history row")
    return trades[0]


def _trade_number(trade, field):
    try:
        return float(trade.get(field, 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def print_previous_trade(trader_name, wallet, trade):
    """Print history without treating it as a newly detected live fill."""
    timestamp = _trade_number(trade, "timestamp")
    trade_time = (
        datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
        if timestamp > 0
        else "UNKNOWN"
    )
    size = _trade_number(trade, "size")
    price = _trade_number(trade, "price")

    print("-" * 120)
    print(
        f"[CONNECTION CONFIRMED] Latest previous trade for {trader_name}",
        flush=True,
    )
    print(f"    wallet:       {wallet}")
    print(f"    trade time:   {trade_time} (local)")
    print(
        f"    trade:        {trade.get('side') or '?'} "
        f"{trade.get('outcome') or '?'}"
    )
    print(f"    market:       {trade.get('title') or 'UNKNOWN MARKET'}")
    print(f"    shares:       {size:.6f}")
    print(f"    price:        ~{price:.6f}")
    print(f"    cash:         ${size * price:.6f}")
    if trade.get("transactionHash"):
        print(f"    tx:           {trade['transactionHash']}")
    print("    source:       Polymarket Data API (historical; not logged)")
    print("-" * 120, flush=True)


async def print_latest_previous_trades():
    """Fetch one historical confirmation after subscriptions succeed."""
    print(
        "Checking the latest previous trade for each watched wallet...",
        flush=True,
    )
    for trader_name, wallet in WATCHED.items():
        try:
            trade = await asyncio.to_thread(fetch_latest_previous_trade, wallet)
            if trade is None:
                print(
                    f"[CONNECTION CONFIRMED] {trader_name}: subscriptions "
                    "are live; no previous public trade found.",
                    flush=True,
                )
                continue
            print_previous_trade(trader_name, wallet, trade)
        except Exception as exc:
            # History is only a visual confirmation; live event tracking must
            # continue when the public Data API is unavailable.
            print(
                f"[history lookup warning] {trader_name}: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )


# ============================================================
# TOKEN -> MARKET RESOLUTION
# ============================================================

def get_cached_token(token_id):
    token_id = str(token_id)
    info = token_map.get(token_id)
    if info is not None:
        token_map.move_to_end(token_id)
    return info


def cache_market_mapping(mapping):
    for token_id, info in mapping.items():
        token_id = str(token_id)
        token_map[token_id] = info
        token_map.move_to_end(token_id)
    while len(token_map) > MAX_TOKEN_CACHE:
        token_map.popitem(last=False)


def fetch_gamma_mapping(token_id):
    token_id = str(token_id)
    response = requests.get(
        f"{GAMMA_API}/markets",
        params={"clob_token_ids": token_id},
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
            str(value) for value in _parse_jsonish(market.get("clobTokenIds"))
        ]
        outcomes = [
            str(value) for value in _parse_jsonish(market.get("outcomes"))
        ]
        if token_id not in token_ids:
            continue

        crypto_config = market.get("cryptoMarketConfig") or {}
        common = {
            "title": market.get("question") or market.get("title") or "UNKNOWN MARKET",
            "slug": market.get("slug") or "",
            "duration": crypto_config.get("duration") or "",
        }
        return {
            market_token: {
                **common,
                "outcome": outcomes[index] if index < len(outcomes) else "UNKNOWN",
            }
            for index, market_token in enumerate(token_ids)
        }
    return {}


async def resolve_token_exact(token_id):
    token_id = str(token_id)
    cached = get_cached_token(token_id)
    if cached is not None:
        return cached

    task = resolution_tasks.get(token_id)
    if task is None:
        task = asyncio.create_task(
            asyncio.to_thread(fetch_gamma_mapping, token_id)
        )
        resolution_tasks[token_id] = task

    try:
        mapping = await task
    finally:
        if resolution_tasks.get(token_id) is task:
            resolution_tasks.pop(token_id, None)

    if not mapping:
        return None
    cache_market_mapping(mapping)
    return get_cached_token(token_id)


# ============================================================
# ORDER BOOK SNAPSHOT
# ============================================================

def fetch_order_book_top(token_id):
    """Return the best bid/ask for the traded outcome token.

    The CLOB response documents bids as descending and asks as ascending, but
    selecting by price keeps this correct even if response ordering changes.
    Empty sides are represented by None rather than inventing a zero quote.
    """
    response = requests.get(
        f"{CLOB_API}/book",
        params={"token_id": str(token_id)},
        timeout=ORDER_BOOK_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    book = response.json()

    if not isinstance(book, dict):
        raise ValueError("Order book response was not an object")

    def valid_levels(side):
        levels = []
        for level in book.get(side) or []:
            try:
                price = float(level["price"])
                quantity = float(level["size"])
            except (KeyError, TypeError, ValueError):
                continue
            levels.append((price, quantity))
        return levels

    bids = valid_levels("bids")
    asks = valid_levels("asks")

    best_bid = max(bids, key=lambda level: level[0]) if bids else None
    best_ask = min(asks, key=lambda level: level[0]) if asks else None

    return {
        "best_bid_price": best_bid[0] if best_bid else None,
        "best_bid_quantity": best_bid[1] if best_bid else None,
        "best_ask_price": best_ask[0] if best_ask else None,
        "best_ask_quantity": best_ask[1] if best_ask else None,
    }


# ============================================================
# ORDERFILLED DECODING
# ============================================================

def decode_order_filled(log):
    topics = log["topics"]
    if len(topics) < 4:
        raise ValueError(f"Unexpected topic count: {len(topics)}")

    raw_hex = log["data"].removeprefix("0x")
    words = [
        bytes.fromhex(raw_hex)[index:index + 32]
        for index in range(0, len(raw_hex) // 2, 32)
    ]
    if len(words) < 7:
        raise ValueError(f"Expected 7 data words, got {len(words)}")

    side_raw = int.from_bytes(words[0], "big")
    token_id = int.from_bytes(words[1], "big")
    maker_amount = int.from_bytes(words[2], "big") / 1_000_000
    taker_amount = int.from_bytes(words[3], "big") / 1_000_000
    fee = int.from_bytes(words[4], "big") / 1_000_000

    if side_raw == 0:
        order_side = "BUY"
        cash, shares = maker_amount, taker_amount
    elif side_raw == 1:
        order_side = "SELL"
        shares, cash = maker_amount, taker_amount
    else:
        order_side = f"UNKNOWN({side_raw})"
        shares = cash = 0

    return {
        "maker": topic_to_address(topics[2]),
        "taker": topic_to_address(topics[3]),
        "order_side": order_side,
        "token_id": str(token_id),
        "shares": shares,
        "cash": cash,
        "price": cash / shares if shares else 0,
        "fee": fee,
    }


def watched_trader_side(fill, matched_field):
    if matched_field == "TAKER":
        return opposite_side(fill["order_side"])
    return fill["order_side"]


def remember_event(log):
    key = f"{log.get('transactionHash', '')}:{log.get('logIndex', '')}"
    if key in seen_event_keys:
        return False
    seen_event_keys.add(key)
    seen_event_queue.append(key)
    while len(seen_event_queue) > MAX_SEEN_EVENTS:
        seen_event_keys.discard(seen_event_queue.popleft())
    return True


# ============================================================
# TRADE HANDLING
# ============================================================

def track_fill_task(task):
    active_fill_tasks.add(task)
    task.add_done_callback(active_fill_tasks.discard)


async def drain_fill_tasks():
    if not active_fill_tasks:
        return
    pending = list(active_fill_tasks)
    print(f"Draining {len(pending)} in-flight trade(s)...", flush=True)
    try:
        await asyncio.wait_for(
            asyncio.gather(*pending, return_exceptions=True),
            timeout=SHUTDOWN_DRAIN_SECONDS,
        )
    except asyncio.TimeoutError:
        remaining = [task for task in pending if not task.done()]
        for task in remaining:
            task.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)


async def handle_fill(
    trader_name,
    matched_field,
    fill,
    receive_wall_timestamp,
    receive_monotonic,
):
    # Start the public CLOB request first so the snapshot is taken as close as
    # possible to WebSocket receipt. Gamma resolution runs at the same time.
    order_book_task = asyncio.create_task(
        asyncio.to_thread(fetch_order_book_top, fill["token_id"])
    )

    try:
        market = await resolve_token_exact(fill["token_id"])
    except Exception as exc:
        print(f"[market lookup error] {exc}", flush=True)
        market = None

    if market is None:
        market = {
            "title": "UNKNOWN MARKET",
            "outcome": "?",
            "slug": "",
            "duration": "",
        }

    try:
        order_book = await order_book_task
    except Exception as exc:
        print(f"[order book error] token {fill['token_id']}: {exc}", flush=True)
        order_book = {
            "best_bid_price": None,
            "best_bid_quantity": None,
            "best_ask_price": None,
            "best_ask_quantity": None,
        }

    event_dt = datetime.fromtimestamp(receive_wall_timestamp)
    processing_latency = time.perf_counter() - receive_monotonic
    side = watched_trader_side(fill, matched_field)

    def book_value(name):
        value = order_book[name]
        return "" if value is None else f"{value:.6f}"

    row = {
        "detect_datetime_local": event_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "trader": trader_name,
        "processing_latency_seconds": f"{processing_latency:.6f}",
        "side": side,
        "outcome": market["outcome"],
        "market": market["title"],
        "duration": market["duration"],
        "market_slug": market["slug"],
        "shares": f"{fill['shares']:.6f}",
        "price": f"{fill['price']:.6f}",
        "cash": f"{fill['cash']:.6f}",
        "fee": f"{fill['fee']:.6f}",
        "best_bid_price": book_value("best_bid_price"),
        "best_bid_quantity": book_value("best_bid_quantity"),
        "best_ask_price": book_value("best_ask_price"),
        "best_ask_quantity": book_value("best_ask_quantity"),
    }
    await csv_logger.write_row(row, event_dt)

    bid_text = (
        f"{row['best_bid_price']} x {row['best_bid_quantity']}"
        if row["best_bid_price"]
        else "NONE"
    )
    ask_text = (
        f"{row['best_ask_price']} x {row['best_ask_quantity']}"
        if row["best_ask_price"]
        else "NONE"
    )

    print(
        f"[{row['detect_datetime_local']}] {trader_name} | {side} "
        f"{market['outcome']} | {market['title']} | "
        f"{fill['shares']:.6f} sh @ {fill['price']:.6f} | "
        f"${fill['cash']:.6f} | processing {processing_latency:.3f}s",
        flush=True,
    )
    print(
        f"    BOOK {market['outcome']}: bid {bid_text} | ask {ask_text}",
        flush=True,
    )


# ============================================================
# SUBSCRIPTIONS AND EVENT LOOP
# ============================================================

def build_subscription_requests():
    requests_to_send = []
    request_id = 1
    roles = ["MAKER"]
    if WATCH_TAKER_FIELD_TOO:
        roles.append("TAKER")

    for trader_name, wallet in WATCHED.items():
        wallet_topic = address_topic(wallet)
        for contract_name, contract_address in EXCHANGES.items():
            for role in roles:
                topics = (
                    [ORDER_FILLED_TOPIC, None, wallet_topic]
                    if role == "MAKER"
                    else [ORDER_FILLED_TOPIC, None, None, wallet_topic]
                )
                rpc = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "eth_subscribe",
                    "params": ["logs", {"address": contract_address, "topics": topics}],
                }
                requests_to_send.append(
                    (request_id, rpc, (trader_name, role, contract_name))
                )
                request_id += 1
    return requests_to_send


async def subscribe_wallets(ws):
    pending = {}
    subscriptions = {}
    buffered = []

    for request_id, rpc, meta in build_subscription_requests():
        pending[request_id] = meta
        await ws.send(json.dumps(rpc))

    while pending:
        raw = await ws.recv()
        receive_wall = time.time()
        receive_monotonic = time.perf_counter()
        message = json.loads(raw)

        request_id = message.get("id")
        if request_id in pending:
            meta = pending.pop(request_id)
            if "result" in message:
                subscriptions[message["result"]] = meta
                print(f"[OK] {meta[0]} | {meta[2]} | {meta[1]}", flush=True)
            else:
                print(f"[subscription error] {meta}: {message}", flush=True)
        elif message.get("method") == "eth_subscription":
            buffered.append((message, receive_wall, receive_monotonic))

    return subscriptions, buffered


def dispatch_subscription_message(
    message,
    receive_wall,
    receive_monotonic,
    subscriptions,
):
    if message.get("method") != "eth_subscription":
        return
    params = message.get("params") or {}
    meta = subscriptions.get(params.get("subscription"))
    log = params.get("result")
    if meta is None or not log or not remember_event(log):
        return

    trader_name, role, _contract_name = meta
    try:
        fill = decode_order_filled(log)
        task = asyncio.create_task(
            handle_fill(
                trader_name,
                role,
                fill,
                receive_wall,
                receive_monotonic,
            )
        )
        track_fill_task(task)
    except Exception as exc:
        print(f"[decode error] {exc}", flush=True)


def validate_config():
    if not POLYGON_WS:
        raise RuntimeError(
            "POLYGON_WS is not set. In PowerShell run:\n"
            "  $env:POLYGON_WS = "
            "\"wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY\""
        )
    if not POLYGON_WS.startswith(("wss://", "ws://")):
        raise RuntimeError("POLYGON_WS must start with wss:// or ws://")


async def watch():
    validate_config()
    print(f"CSV folder: {LOG_DIR}", flush=True)
    print(f"Watching {len(WATCHED)} trader(s)", flush=True)

    try:
        while True:
            try:
                print("Connecting to Polygon...", flush=True)
                async with websockets.connect(
                    POLYGON_WS,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=None,
                ) as ws:
                    subscriptions, buffered = await subscribe_wallets(ws)
                    if not subscriptions:
                        raise RuntimeError("No subscriptions succeeded")
                    print("Watcher live.", flush=True)
                    await print_latest_previous_trades()

                    for message, receive_wall, receive_monotonic in buffered:
                        dispatch_subscription_message(
                            message,
                            receive_wall,
                            receive_monotonic,
                            subscriptions,
                        )

                    while True:
                        raw = await ws.recv()
                        # Capture both clocks immediately after receipt. Wall
                        # time is for readable dates; monotonic time is latency.
                        receive_wall = time.time()
                        receive_monotonic = time.perf_counter()
                        message = json.loads(raw)
                        dispatch_subscription_message(
                            message,
                            receive_wall,
                            receive_monotonic,
                            subscriptions,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[connection error] {type(exc).__name__}: {exc}", flush=True)
                print("Reconnecting in 2 seconds...", flush=True)
                await asyncio.sleep(2)
    finally:
        await drain_fill_tasks()
        await csv_logger.close()


if __name__ == "__main__":
    try:
        asyncio.run(watch())
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
