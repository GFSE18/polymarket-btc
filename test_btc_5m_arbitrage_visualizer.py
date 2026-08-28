from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from btc_5m_arbitrage_visualizer import (
    analyze_day,
    analyze_strategy,
    load_daily_data,
    plot_opportunity_timing,
    plot_success_timeline,
    plot_window_lengths,
    qualifying_windows,
)


SCHEMA = """
CREATE TABLE dataset_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE markets (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
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
    initial_down_ask_micros INTEGER
);
CREATE TABLE book_quotes (
    id INTEGER PRIMARY KEY,
    market_id INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    observed_elapsed_ms INTEGER NOT NULL,
    observed_epoch_ms INTEGER NOT NULL,
    source_epoch_ms INTEGER,
    best_bid_micros INTEGER,
    best_ask_micros INTEGER
);
"""


def utc_iso(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).isoformat(
        timespec="milliseconds"
    )


class ArbitrageVisualizerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.path = Path(__file__).resolve().parent / ".btc_5m_arbitrage_test.sqlite3"
        self.path.unlink(missing_ok=True)
        self.day_start_ms = 1_787_803_200_000
        connection = sqlite3.connect(self.path)
        try:
            connection.executescript(SCHEMA)
            connection.executemany(
                "INSERT INTO dataset_metadata(key, value) VALUES (?, ?)",
                [("local_date", "2026-08-27"), ("price_scale", "1000000")],
            )
            for market_id, offset_ms, slug in (
                (1, 0, "market-a"),
                (2, 300_000, "market-b"),
                (3, 600_000, "market-c"),
            ):
                start_ms = self.day_start_ms + offset_ms
                connection.execute(
                    "INSERT INTO markets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        market_id,
                        1,
                        slug,
                        slug,
                        f"condition-{market_id}",
                        start_ms,
                        start_ms + 300_000,
                        f"up-{market_id}",
                        f"down-{market_id}",
                        utc_iso(start_ms),
                        utc_iso(start_ms + 300_000),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                )

            quote_rows = []
            quote_id = 1

            def add(market_id: int, outcome: str, elapsed: int, ask: int | None) -> None:
                nonlocal quote_id
                market_start = self.day_start_ms + (market_id - 1) * 300_000
                quote_rows.append(
                    (
                        quote_id,
                        market_id,
                        outcome,
                        elapsed,
                        market_start + elapsed,
                        market_start + elapsed - 25,
                        None if ask is None else max(0, ask - 10_000),
                        ask,
                    )
                )
                quote_id += 1

            # Market A: initial-below Up quote, an above-threshold break, and a data gap.
            add(1, "Up", 0, 400_000)
            add(1, "Up", 10_000, 500_000)
            add(1, "Up", 20_000, 440_000)
            add(1, "Up", 30_000, None)
            add(1, "Up", 40_000, 430_000)
            add(1, "Down", 5_000, 600_000)
            add(1, "Down", 50_000, 450_000)
            add(1, "Down", 60_000, 440_000)
            add(1, "Down", 80_000, 500_000)

            # Market B: only Up qualifies, so it must not enter strategy statistics.
            add(2, "Up", 0, 440_000)
            add(2, "Up", 100_000, 500_000)
            add(2, "Down", 0, 600_000)

            # Market C: both legs qualify simultaneously; Down remains available longer.
            add(3, "Up", 0, 600_000)
            add(3, "Up", 10_000, 450_000)
            add(3, "Up", 20_000, 500_000)
            add(3, "Down", 0, 600_000)
            add(3, "Down", 10_000, 450_000)
            add(3, "Down", 30_000, 500_000)

            connection.executemany(
                "INSERT INTO book_quotes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                quote_rows,
            )
            connection.commit()
        finally:
            connection.close()

    def tearDown(self) -> None:
        self.path.unlink(missing_ok=True)

    def test_initial_quote_and_data_gap_form_distinct_windows(self) -> None:
        data = load_daily_data(self.path)
        windows = qualifying_windows(data, 0.45)
        up_a = windows.loc[
            (windows["slug"] == "market-a") & (windows["outcome"] == "Up")
        ]

        self.assertEqual(
            up_a[["start_ms", "end_ms"]].values.tolist(),
            [[0, 10_000], [20_000, 30_000], [40_000, 300_000]],
        )

    def test_adjacent_below_threshold_quotes_merge(self) -> None:
        data = load_daily_data(self.path)
        windows = qualifying_windows(data, 0.45)
        down_a = windows.loc[
            (windows["slug"] == "market-a") & (windows["outcome"] == "Down")
        ]

        self.assertEqual(len(down_a), 1)
        self.assertEqual(down_a.iloc[0]["start_ms"], 50_000)
        self.assertEqual(down_a.iloc[0]["end_ms"], 80_000)
        self.assertEqual(down_a.iloc[0]["duration_ms"], 30_000)

    def test_opportunity_counts_require_both_outcomes(self) -> None:
        _, _, table = analyze_day(self.path)
        counts = table.set_index("Strategy")["Successful markets"].to_dict()

        self.assertEqual(counts, {"45/45": 2, "40/40": 0, "35/35": 0})
        self.assertTrue((table["Observed markets"] == 3).all())

    def test_statistics_exclude_one_sided_markets(self) -> None:
        data = load_daily_data(self.path)
        analysis = analyze_strategy(data, "45/45")

        self.assertEqual(analysis.successful_slugs, {"market-a", "market-c"})
        self.assertNotIn("market-b", set(analysis.windows["slug"]))
        stats = analysis.window_statistics.set_index("Outcome")
        self.assertEqual(stats.loc["Up", "Markets"], 2)
        self.assertEqual(stats.loc["Down", "Markets"], 2)

    def test_first_leg_and_last_finish_timing(self) -> None:
        data = load_daily_data(self.path)
        timing = analyze_strategy(data, "45/45").timing.set_index("slug")

        self.assertEqual(timing.loc["market-a", "first_leg"], "Up")
        self.assertEqual(timing.loc["market-a", "finish_leg"], "Down")
        self.assertEqual(timing.loc["market-a", "earliest_first_seconds"], 0)
        self.assertEqual(timing.loc["market-a", "last_finish_seconds"], 80)
        self.assertEqual(timing.loc["market-c", "first_leg"], "Simultaneous")
        self.assertEqual(timing.loc["market-c", "last_finish_seconds"], 30)

    def test_matplotlib_figures_render(self) -> None:
        import matplotlib.pyplot as plt

        data = load_daily_data(self.path)
        analysis = analyze_strategy(data, "45/45")
        figures = [
            plot_success_timeline(data, analysis, dark=False),
            plot_window_lengths(analysis, dark=False),
            plot_opportunity_timing(analysis, dark=True),
        ]
        try:
            for figure in figures:
                figure.canvas.draw()
                self.assertGreater(figure.get_figwidth(), 0)
        finally:
            for figure in figures:
                plt.close(figure)


if __name__ == "__main__":
    unittest.main()
