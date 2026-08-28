import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import btc_5m_statistics as statistics
from btc_5m_statistics import (
    BookTop,
    DailyDatabase,
    MarketContext,
    OutcomeTracker,
)


THRESHOLDS = (450_000, 400_000, 350_000)
WINDOW_START = 1_800_000_000


class CrossingTrackerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = DailyDatabase(
            Path(self.temp_dir.name) / "test.sqlite3",
            date(2026, 8, 24),
            0.250,
            (0.45, 0.40, 0.35),
        )
        self.market = MarketContext(
            slug=f"btc-updown-5m-{WINDOW_START}",
            question="Test BTC market",
            condition_id="condition",
            window_start_epoch=WINDOW_START,
            end_epoch=WINDOW_START + 300,
            token_ids={"Up": "up-token", "Down": "down-token"},
        )
        self.market_id = self.database.start_market(self.market, WINDOW_START)

    def tearDown(self) -> None:
        self.database.close(WINDOW_START + 60)
        self.temp_dir.cleanup()

    @staticmethod
    def book(elapsed_ms: int, ask_micros: int | None) -> BookTop:
        return BookTop(
            best_bid_micros=None if ask_micros is None else ask_micros - 10_000,
            best_ask_micros=ask_micros,
            received_epoch_ms=WINDOW_START * 1000 + elapsed_ms,
            source_epoch_ms=None,
        )

    def crossing_rows(self) -> list[tuple[int, int, int | None]]:
        return self.database.connection.execute(
            """
            SELECT threshold_micros, start_elapsed_ms, duration_ms
            FROM crossings
            ORDER BY threshold_micros DESC
            """
        ).fetchall()

    def test_jump_crosses_all_thresholds_and_durations_close_independently(self) -> None:
        tracker = OutcomeTracker("Up", THRESHOLDS)

        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(10_000, 470_000),
        )
        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(20_000, 340_000),
        )
        self.assertEqual(tracker.counts, {450_000: 1, 400_000: 1, 350_000: 1})

        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(30_000, 360_000),
        )
        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(40_000, 410_000),
        )
        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(50_000, 460_000),
        )

        self.assertEqual(
            self.crossing_rows(),
            [
                (450_000, 20_000, 30_000),
                (400_000, 20_000, 20_000),
                (350_000, 20_000, 10_000),
            ],
        )

    def test_initial_snapshot_and_post_gap_snapshot_do_not_count(self) -> None:
        tracker = OutcomeTracker("Down", THRESHOLDS)

        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(10_000, 380_000),
        )
        self.assertEqual(sum(tracker.counts.values()), 0)

        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(20_000, 420_000),
        )
        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(30_000, 380_000),
        )
        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(35_000, 390_000),
        )
        tracker.data_gap(
            self.database,
            self.market_id,
            self.market,
            WINDOW_START * 1000 + 40_000,
        )
        tracker.observe(
            self.database,
            self.market_id,
            self.market,
            self.book(45_000, 380_000),
        )

        self.assertEqual(tracker.counts, {450_000: 0, 400_000: 1, 350_000: 0})
        self.assertEqual(self.crossing_rows(), [(400_000, 30_000, 5_000)])

    def test_database_rejects_a_different_local_date(self) -> None:
        self.database.commit()
        path = self.database.path
        second_connection = sqlite3.connect(path)
        second_connection.execute(
            "UPDATE dataset_metadata SET value = '2026-08-23' WHERE key = 'local_date'"
        )
        second_connection.commit()
        second_connection.close()

        with self.assertRaisesRegex(ValueError, "database is for local date"):
            DailyDatabase(path, date(2026, 8, 24), 0.250, (0.45, 0.40, 0.35))

    def test_daily_rotation_closes_old_file_and_opens_new_file(self) -> None:
        previous_path = self.database.path
        next_date = date(2026, 8, 25)
        with patch.object(statistics, "DATA_DIR", Path(self.temp_dir.name)):
            next_database, next_path, _ = statistics.rotate_daily_database(
                self.database,
                date(2026, 8, 24),
                next_date,
                0.250,
                (0.45, 0.40, 0.35),
                self.market,
                self.market_id,
                {},
            )

        self.database = next_database
        previous_connection = sqlite3.connect(previous_path)
        try:
            ended = previous_connection.execute(
                "SELECT ended_at_utc FROM runs ORDER BY id DESC LIMIT 1"
            ).fetchone()[0]
        finally:
            previous_connection.close()
        self.assertIsNotNone(ended)
        self.assertEqual(next_path.name, "btc_5m_statistics_2026-08-25.sqlite3")
        next_local_date = next_database.connection.execute(
            "SELECT value FROM dataset_metadata WHERE key = 'local_date'"
        ).fetchone()[0]
        self.assertEqual(next_local_date, "2026-08-25")

    def test_p_key_toggles_runtime_printout(self) -> None:
        class FakeKeyboard:
            def __init__(self, keys: str) -> None:
                self.keys = list(keys)

            def read_pending(self) -> list[str]:
                keys = self.keys
                self.keys = []
                return keys

        enabled, toggled = statistics.poll_runtime_printout(
            True,
            FakeKeyboard("p"),
        )
        self.assertFalse(enabled)
        self.assertTrue(toggled)

        enabled, toggled = statistics.poll_runtime_printout(
            False,
            FakeKeyboard("P"),
        )
        self.assertTrue(enabled)
        self.assertTrue(toggled)


if __name__ == "__main__":
    unittest.main()
