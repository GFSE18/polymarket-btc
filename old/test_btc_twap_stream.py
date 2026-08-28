"""Flash Polymarket's public BTC/USD 60-second Chainlink TWAP in a terminal.

Run:
    python test_btc_twap_stream.py

Stop with Ctrl+C. No API key or Chainlink subscription is required.
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import websockets


RTDS_URL = "wss://ws-live-data.polymarket.com"
TWAP_TOPIC = "crypto_prices_twap_sixty"
SYMBOL = "btc/usd"
E18 = Decimal(10) ** 18

SUBSCRIPTION = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": TWAP_TOPIC,
            "type": "update",
        }
    ],
}

RESET = "\033[0m"
GREEN_FLASH = "\033[30;102m"
RED_FLASH = "\033[97;101m"
YELLOW_FLASH = "\033[30;103m"
CLEAR_LINE = "\r\033[2K"


def parse_btc_twap(message):
    """Return (timestamp_ms, exact_price) for a valid BTC 60s update."""
    if message.get("topic") != TWAP_TOPIC or message.get("type") != "update":
        return None

    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None
    if payload.get("symbol") != SYMBOL or payload.get("window_s") != 60:
        return None

    timestamp_ms = payload.get("timestamp")
    full_accuracy_value = payload.get("full_accuracy_value")
    if not isinstance(timestamp_ms, int) or not isinstance(full_accuracy_value, str):
        return None

    try:
        price = Decimal(full_accuracy_value) / E18
    except (InvalidOperation, ValueError):
        return None

    return timestamp_ms, price


def render_tick(timestamp_ms, price, previous_price):
    if previous_price is None or price == previous_price:
        color = YELLOW_FLASH
        arrow = "="
        change = Decimal(0)
    elif price > previous_price:
        color = GREEN_FLASH
        arrow = "UP"
        change = price - previous_price
    else:
        color = RED_FLASH
        arrow = "DOWN"
        change = price - previous_price

    source_time = datetime.fromtimestamp(
        timestamp_ms / 1000,
        tz=timezone.utc,
    ).strftime("%H:%M:%S.%f")[:-3]
    latency_ms = int(time.time() * 1000) - timestamp_ms

    line = (
        f" {arrow:^4} "
        f" BTC/USD TWAP 60s  ${price:,.18f}  "
        f"delta {change:+.8f}  "
        f"source {source_time} UTC  "
        f"lag {latency_ms:+d} ms "
    )

    if sys.stdout.isatty():
        print(f"{CLEAR_LINE}{color}{line}{RESET}", end="", flush=True)
    else:
        print(line, flush=True)


def print_status(text):
    if sys.stdout.isatty():
        print(f"{CLEAR_LINE}{text}", end="", flush=True)
    else:
        print(text, flush=True)


async def watch_twap():
    previous_price = None
    reconnect_delay = 1

    while True:
        try:
            print_status("Connecting to Polymarket BTC/USD TWAP 60s...")
            async with websockets.connect(
                RTDS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=None,
            ) as websocket:
                await websocket.send(json.dumps(SUBSCRIPTION))
                reconnect_delay = 1

                async for raw_message in websocket:
                    try:
                        message = json.loads(raw_message)
                    except (json.JSONDecodeError, TypeError):
                        continue

                    tick = parse_btc_twap(message)
                    if tick is None:
                        continue

                    timestamp_ms, price = tick
                    render_tick(timestamp_ms, price, previous_price)
                    previous_price = price

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if sys.stdout.isatty():
                print(CLEAR_LINE, end="")
            print(
                f"Connection lost ({type(exc).__name__}: {exc}). "
                f"Retrying in {reconnect_delay}s...",
                flush=True,
            )
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)


if __name__ == "__main__":
    try:
        asyncio.run(watch_twap())
    except KeyboardInterrupt:
        if sys.stdout.isatty():
            print(CLEAR_LINE, end="")
        print("Stopped.")
