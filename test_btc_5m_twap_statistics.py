import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import btc_5m_twap_statistics as stats


class MarketAndFeedTests(unittest.TestCase):
    def make_market(self):
        return {
            "slug": "btc-updown-5m-1787886900",
            "outcomes": '["Up", "Down"]',
            "clobTokenIds": '["up-token", "down-token"]',
            "cryptoMarketConfig": {
                "twapEnabled": True,
                "twapLookbackSeconds": 60,
            },
        }

    def test_market_time_is_derived_from_slug(self):
        market = stats.market_context(self.make_market())
        self.assertEqual(market.window_epoch, 1787886900)
        self.assertEqual(market.window_datetime_utc, "2026-08-28T03:15:00+00:00")
        self.assertEqual(market.end_epoch, 1787887200)

    def test_price_to_beat_url_requests_the_60_second_twap_target(self):
        market = stats.market_context(self.make_market())
        url = stats.price_to_beat_request_url(market)
        self.assertIn("symbol=BTC", url)
        self.assertIn("variant=fiveminute", url)
        self.assertIn("eventStartTime=2026-08-28T03%3A15%3A00Z", url)
        self.assertIn("endDate=2026-08-28T03%3A20%3A00Z", url)
        self.assertIn("twapEnabled=true", url)
        self.assertIn("twapLookbackSeconds=60", url)

    def test_twap_uses_full_accuracy_value(self):
        event = {
            "topic": stats.TWAP_TOPIC,
            "type": "update",
            "payload": {
                "symbol": "btc/usd",
                "window_s": 60,
                "value": 1.0,
                "full_accuracy_value": "79890719388275420000000",
            },
        }
        self.assertAlmostEqual(stats.parse_twap_event(event), 79890.71938827542)

    def test_clob_book_and_price_change_update_bbo(self):
        state = stats.MarketBookState(
            token_to_outcome={"up-token": "Up", "down-token": "Down"}
        )
        stats.apply_clob_event(
            state,
            {
                "event_type": "book",
                "asset_id": "up-token",
                "bids": [{"price": "0.41"}, {"price": "0.42"}],
                "asks": [{"price": "0.45"}, {"price": "0.44"}],
            },
        )
        self.assertEqual(state.quotes["Up"].best_bid, 0.42)
        self.assertEqual(state.quotes["Up"].best_ask, 0.44)

        stats.apply_clob_event(
            state,
            {
                "event_type": "price_change",
                "price_changes": [
                    {
                        "asset_id": "up-token",
                        "best_bid": "0.43",
                        "best_ask": "0.46",
                    }
                ],
            },
        )
        self.assertEqual(state.quotes["Up"].best_bid, 0.43)
        self.assertEqual(state.quotes["Up"].best_ask, 0.46)


class DatabaseTests(unittest.TestCase):
    def test_schema_uses_boolean_validity_and_composite_sample_key(self):
        market = stats.market_context(MarketAndFeedTests().make_market())
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(stats, "DATA_DIR", Path(temp_dir)):
                database = stats.DailyDatabase(
                    stats.datetime.fromtimestamp(
                        market.window_epoch, stats.timezone.utc
                    ).date()
                )
                table_sql = database.connection.execute(
                    "SELECT sql FROM sqlite_master WHERE name = 'samples'"
                ).fetchone()[0]
                key_columns = database.connection.execute(
                    "PRAGMA table_info(samples)"
                ).fetchall()

                self.assertIn("sample_valid        BOOLEAN", table_sql)
                primary_key = {
                    row[1]: row[5]
                    for row in key_columns
                    if row[5]
                }
                self.assertEqual(primary_key, {"market_id": 1, "sample_index": 2})
                database.close()

    def test_duplicate_sample_is_not_inserted_twice(self):
        market = stats.market_context(MarketAndFeedTests().make_market())
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(stats, "DATA_DIR", Path(temp_dir)):
                database = stats.DailyDatabase(
                    stats.datetime.fromtimestamp(
                        market.window_epoch, stats.timezone.utc
                    ).date()
                )
                market_id = database.ensure_market(market)
                database.record_sample(
                    market_id, 0, 0.40, 0.42, 0.58, 0.60, 79890.0, True
                )
                database.record_sample(
                    market_id, 0, 0.10, 0.20, 0.80, 0.90, 1.0, False
                )
                database.commit()

                row = database.connection.execute(
                    """
                    SELECT up_bid, twap_price, sample_valid
                    FROM samples WHERE market_id = ? AND sample_index = 0
                    """,
                    (market_id,),
                ).fetchone()
                self.assertEqual(row, (0.40, 79890.0, 1))

                with self.assertRaises(sqlite3.IntegrityError):
                    database.connection.execute(
                        """
                        INSERT INTO samples(
                            market_id, sample_index, sample_valid
                        ) VALUES (?, ?, ?)
                        """,
                        (market_id, 1, 2),
                    )
                database.connection.rollback()
                database.close()


if __name__ == "__main__":
    unittest.main()
