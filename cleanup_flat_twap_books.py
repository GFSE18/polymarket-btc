#!/usr/bin/env python3
"""Remove BTC 5-minute markets whose recorded book mid-prices stay at 0.500.

By default this is a read-only report.  Pass --apply to back up and remove the
matching market windows from every SQLite database in the data directory.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


DATA_DIR = Path(__file__).resolve().parent / "btc_5m_twap_statistics_data"
BACKUP_DIR = DATA_DIR / "pre_flat_book_cleanup"
FULL_WINDOW_SAMPLES = 1_200
MIDPOINT_TOLERANCE = 1e-9


def find_flat_book_markets(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return complete windows with no recorded book midpoint away from 0.500.

    A quote snapshot counts only when all four best bid/ask fields are present.
    ``sample_valid`` is deliberately not used: it includes TWAP availability,
    which is irrelevant when cleaning order-book data.
    """
    return connection.execute(
        """
        WITH market_books AS (
            SELECT
                m.id,
                m.slug,
                m.window_datetime_utc,
                COUNT(*) AS sample_count,
                SUM(
                    s.up_bid IS NOT NULL
                    AND s.up_ask IS NOT NULL
                    AND s.down_bid IS NOT NULL
                    AND s.down_ask IS NOT NULL
                ) AS complete_book_samples,
                SUM(
                    s.up_bid IS NOT NULL
                    AND s.up_ask IS NOT NULL
                    AND s.down_bid IS NOT NULL
                    AND s.down_ask IS NOT NULL
                    AND ABS((s.up_bid + s.up_ask) - 1.0) <= ?
                    AND ABS((s.down_bid + s.down_ask) - 1.0) <= ?
                ) AS midpoint_at_half_samples
            FROM markets AS m
            JOIN samples AS s ON s.market_id = m.id
            GROUP BY m.id
        )
        SELECT id, slug, window_datetime_utc, complete_book_samples
        FROM market_books
        WHERE sample_count = ?
          AND complete_book_samples > 0
          AND complete_book_samples = midpoint_at_half_samples
        ORDER BY id
        """,
        (MIDPOINT_TOLERANCE, MIDPOINT_TOLERANCE, FULL_WINDOW_SAMPLES),
    ).fetchall()


def back_up_database(source: sqlite3.Connection, destination: Path) -> None:
    """Create a transactionally consistent SQLite backup before modification."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(f"Backup already exists: {destination}")
    with sqlite3.connect(destination) as backup:
        source.backup(backup)


def remove_markets(connection: sqlite3.Connection, market_ids: list[int]) -> None:
    placeholders = ", ".join("?" for _ in market_ids)
    with connection:
        connection.execute(
            f"DELETE FROM samples WHERE market_id IN ({placeholders})", market_ids
        )
        connection.execute(
            f"DELETE FROM markets WHERE id IN ({placeholders})", market_ids
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="back up and remove matching markets"
    )
    args = parser.parse_args()

    databases = sorted(DATA_DIR.glob("btc_5m_twap_statistics_*.sqlite3"))
    if not databases:
        raise SystemExit(f"No datasets found in {DATA_DIR}")

    total = 0
    for database in databases:
        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        try:
            candidates = find_flat_book_markets(connection)
            if not candidates:
                continue

            total += len(candidates)
            print(f"{database.name}: {len(candidates)} matching markets")
            for row in candidates:
                print(
                    f"  {row['window_datetime_utc']}  {row['slug']}"
                    f"  ({row['complete_book_samples']} complete book samples)"
                )

            if not args.apply:
                continue

            backup_path = BACKUP_DIR / database.name
            back_up_database(connection, backup_path)
            market_ids = [int(row["id"]) for row in candidates]
            remove_markets(connection, market_ids)
            connection.execute("VACUUM")
            remaining = find_flat_book_markets(connection)
            if remaining:
                raise RuntimeError(
                    f"Verification failed for {database}: {len(remaining)} matches remain"
                )
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise RuntimeError(f"Integrity check failed for {database}: {integrity}")
            print(f"  Removed and backed up to {backup_path}")
        finally:
            connection.close()

    action = "Removed" if args.apply else "Found"
    print(f"{action} {total} matching markets.")
    if not args.apply:
        print("Run again with --apply to back up and remove them.")


if __name__ == "__main__":
    main()
