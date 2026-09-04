#!/usr/bin/env python3
"""Print the distribution of complete order-book readings per market.

Run for the configured inclusive date range:
    python btc_5m_twap_valid_record_summary.py

Or provide one or more dates:
    python btc_5m_twap_valid_record_summary.py 2026-09-01 2026-09-02
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date, timedelta
from pathlib import Path


# Change these inclusive bounds to inspect another range by default.
START_DATASET_DATE = "2026-08-28"
END_DATASET_DATE = "2026-09-04"

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


def bucket_label(good_book_records: int) -> str:
    if good_book_records == 0:
        return "0"
    if good_book_records <= 1200:
        lower = ((good_book_records - 1) // 100) * 100 + 1
        upper = lower + 99
        return f"{lower}-{upper}"
    return "1201+"


def configured_dataset_dates() -> list[str]:
    """Return every ISO date in the configured inclusive range."""
    try:
        start = date.fromisoformat(START_DATASET_DATE)
        end = date.fromisoformat(END_DATASET_DATE)
    except ValueError as exc:
        raise SystemExit(
            "START_DATASET_DATE and END_DATASET_DATE must use YYYY-MM-DD."
        ) from exc
    if end < start:
        raise SystemExit("END_DATASET_DATE must be on or after START_DATASET_DATE.")

    result = []
    current = start
    while current <= end:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def print_summary(dataset_date: str) -> None:
    database = DATA_DIR / DATABASE_TEMPLATE.format(date=dataset_date)
    if not database.is_file():
        print(f"Dataset: {dataset_date} (not found; skipped)")
        return

    connection = read_only_connection(database)
    try:
        counts = connection.execute(
            """
            SELECT m.id,
                   COUNT(s.sample_index) AS good_book_records
            FROM markets AS m
            LEFT JOIN samples AS s
              ON s.market_id = m.id
             AND s.up_bid IS NOT NULL
             AND s.up_ask IS NOT NULL
             AND s.down_bid IS NOT NULL
             AND s.down_ask IS NOT NULL
            GROUP BY m.id
            ORDER BY m.id
            """
        ).fetchall()
    finally:
        connection.close()

    requested_buckets = ["0"] + [f"{start}-{start + 99}" for start in range(1, 1201, 100)]
    labels = requested_buckets + (
        ["1201+"] if any(int(row[1]) > 1200 for row in counts) else []
    )
    grouped = {label: 0 for label in labels}
    for _, count in counts:
        grouped[bucket_label(int(count))] += 1

    print(f"Dataset: {dataset_date}")
    print(f"Total markets: {len(counts)}")
    print()
    print(f"{'Good book readings':<20}Markets")
    print(f"{'-' * 20}{'-' * 7}")
    for label in labels:
        print(f"{label:<20}{grouped[label]:>7}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "dates",
        nargs="*",
        metavar="YYYY-MM-DD",
        help="one or more dataset dates; overrides the configured date range",
    )
    args = parser.parse_args()

    dates = args.dates or configured_dataset_dates()
    for index, dataset_date in enumerate(dates):
        if index:
            print()
        print_summary(dataset_date)


if __name__ == "__main__":
    main()
