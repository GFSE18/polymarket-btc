#!/usr/bin/env python3
"""Print the distribution of valid sample records per market.

Change DATASET_DATE below, then run:
    python btc_5m_twap_valid_record_summary.py
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


# Change this value to inspect another daily dataset.
DATASET_DATE = "2026-08-29"

DATA_DIR = Path(__file__).resolve().parent / "btc_5m_twap_statistics_data"
DATABASE_TEMPLATE = "btc_5m_twap_statistics_{date}.sqlite3"


def read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro",
        uri=True,
        timeout=5,
    )
    connection.execute("PRAGMA query_only = ON")
    return connection


def bucket_label(valid_records: int) -> str:
    if valid_records == 0:
        return "0"
    if valid_records <= 1200:
        lower = ((valid_records - 1) // 100) * 100 + 1
        upper = lower + 99
        return f"{lower}-{upper}"
    return "1201+"


def et_time(utc_text: str) -> str:
    value = datetime.fromisoformat(utc_text).astimezone(
        ZoneInfo("America/New_York")
    )
    return value.strftime("%Y-%m-%d %I:%M:%S %p ET")


def main() -> None:
    database = DATA_DIR / DATABASE_TEMPLATE.format(date=DATASET_DATE)
    if not database.is_file():
        raise SystemExit(f"Dataset not found: {database}")

    connection = read_only_connection(database)
    try:
        counts = connection.execute(
            """
            SELECT m.id, m.slug, m.window_datetime_utc,
                   COUNT(s.sample_index) AS valid_records
            FROM markets AS m
            LEFT JOIN samples AS s
              ON s.market_id = m.id
             AND s.sample_valid = 1
            GROUP BY m.id
            ORDER BY m.id
            """
        ).fetchall()
    finally:
        connection.close()

    requested_buckets = ["0"] + [f"{start}-{start + 99}" for start in range(1, 1201, 100)]
    labels = requested_buckets + (
        ["1201+"] if any(int(row[3]) > 1200 for row in counts) else []
    )
    grouped = {label: 0 for label in labels}
    for _, _, _, count in counts:
        grouped[bucket_label(int(count))] += 1

    print(f"Dataset: {DATASET_DATE}")
    print(f"Total markets: {len(counts)}")
    print()
    print(f"{'Valid records':<18}Markets")
    print(f"{'-' * 18}{'-' * 7}")
    for label in labels:
        print(f"{label:<18}{grouped[label]:>7}")

    zero_markets = [row for row in counts if int(row[3]) == 0]
    print()
    print(f"Markets with 0 valid records ({len(zero_markets)}):")
    if zero_markets:
        print(f"{'Market start (ET)':<27}Slug")
        print(f"{'-' * 27}{'-' * 4}")
        for _, slug, window_datetime_utc, _ in zero_markets:
            print(f"{et_time(window_datetime_utc):<27}{slug}")
    else:
        print("None")


if __name__ == "__main__":
    main()
