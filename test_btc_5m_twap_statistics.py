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

    def test_full_accuracy_twap_rejects_bad_values(self):
        event = {
            "topic": stats.TWAP_TOPIC,
            "type": "update",
            "payload": {
                "symbol": "btc/usd",
                "window_s": 60,
                "full_accuracy_value": "0",
            },
        }
        for bad_value in ("0", "-1000000000000000000", "Infinity"):
            event["payload"]["full_accuracy_value"] = bad_value
            self.assertIsNone(stats.parse_twap_event(event))

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
                # An ignored duplicate is not a pending database write.
                self.assertEqual(database.pending_samples, 1)
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

    def test_unchanged_prices_at_different_indices_are_all_recorded(self):
        market = stats.market_context(MarketAndFeedTests().make_market())
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(stats, "DATA_DIR", Path(temp_dir)):
                database = stats.DailyDatabase(
                    stats.datetime.fromtimestamp(
                        market.window_epoch, stats.timezone.utc
                    ).date()
                )
                market_id = database.ensure_market(market)
                values = (0.40, 0.42, 0.58, 0.60, 79890.0, True)
                database.record_sample(market_id, 0, *values)
                database.record_sample(market_id, 1, *values)
                database.commit()

                rows = database.connection.execute(
                    """
                    SELECT sample_index, up_bid, twap_price
                    FROM samples
                    WHERE market_id = ?
                    ORDER BY sample_index
                    """,
                    (market_id,),
                ).fetchall()
                self.assertEqual(
                    rows,
                    [(0, 0.40, 79890.0), (1, 0.40, 79890.0)],
                )
                database.close()


class ConnectionCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_timeout_from_finished_ping_task_is_not_fatal(self):
        async def failed_ping():
            raise TimeoutError("timed out while closing connection")

        task = stats.asyncio.create_task(failed_ping())
        await stats.asyncio.sleep(0)
        await stats.stop_ping_task(task)

    async def test_market_worker_cancellation_survives_close_timeout(self):
        class FakeWebsocket:
            async def send(self, _message):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                await stats.asyncio.Future()

        class ClosingConnection:
            async def __aenter__(self):
                return FakeWebsocket()

            async def __aexit__(self, exc_type, _exc, _traceback):
                if exc_type is stats.asyncio.CancelledError:
                    raise TimeoutError("timed out while closing connection")
                return False

        market = stats.market_context(MarketAndFeedTests().make_market())
        state = stats.MarketBookState(
            token_to_outcome={"up-token": "Up", "down-token": "Down"}
        )
        with patch.object(
            stats.websockets,
            "connect",
            return_value=ClosingConnection(),
        ):
            task = stats.asyncio.create_task(stats.clob_worker(market, state))
            await stats.asyncio.sleep(0)
            await stats.asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(stats.asyncio.CancelledError):
                await stats.asyncio.wait_for(task, timeout=1)

    async def test_sample_batch_can_be_written_off_event_loop(self):
        market = stats.market_context(MarketAndFeedTests().make_market())
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(stats, "DATA_DIR", Path(temp_dir)):
                database = stats.DailyDatabase(
                    stats.datetime.fromtimestamp(
                        market.window_epoch, stats.timezone.utc
                    ).date()
                )
                market_id = database.ensure_market(market)
                rows = [
                    (
                        market_id,
                        index,
                        0.40,
                        0.42,
                        0.58,
                        0.60,
                        79890.0,
                        True,
                    )
                    for index in range(20)
                ]
                await stats.write_sample_batch_after(None, database, rows)
                count = database.connection.execute(
                    "SELECT COUNT(*) FROM samples WHERE market_id = ?",
                    (market_id,),
                ).fetchone()[0]
                self.assertEqual(count, 20)
                database.close()

    async def test_collector_makes_even_grid_despite_wall_clock_jump(self):
        real_time = stats.time.time
        real_monotonic = stats.time.monotonic

        class JumpingWallClock:
            calls = 0

            def time(self):
                self.calls += 1
                jump = 100.0 if self.calls > 1 else 0.0
                return real_time() + jump

            @staticmethod
            def monotonic():
                return real_monotonic()

        async def initialized_book_worker(_market, state):
            state.connected = True
            for quote in state.quotes.values():
                quote.best_bid = 0.40
                quote.best_ask = 0.42
                quote.initialized = True
            await stats.asyncio.Future()

        async def idle_price_worker(_database, _market_id, _market):
            await stats.asyncio.Future()

        clock = JumpingWallClock()
        start = real_time() + 0.02
        market = stats.MarketContext(
            slug=f"btc-updown-5m-{int(start)}",
            window_epoch=start,
            window_datetime_utc="test-window",
            token_ids={"Up": "up-token", "Down": "down-token"},
        )
        twap = stats.TwapState(
            price=79890.0,
            connected=True,
            received_monotonic=real_monotonic(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(stats, "DATA_DIR", Path(temp_dir)):
                database = stats.DailyDatabase(stats.date(2026, 8, 28))
                with (
                    patch.object(stats, "time", clock),
                    patch.object(stats, "WINDOW_SECONDS", 0.04),
                    patch.object(stats, "SAMPLE_INTERVAL_SECONDS", 0.01),
                    patch.object(stats, "SAMPLES_PER_MARKET", 4),
                    patch.object(stats, "DATABASE_COMMIT_EVERY_SAMPLES", 2),
                    patch.object(stats, "clob_worker", initialized_book_worker),
                    patch.object(stats, "price_to_beat_worker", idle_price_worker),
                ):
                    await stats.collect_market(database, market, twap)

                indices = database.connection.execute(
                    "SELECT sample_index FROM samples ORDER BY sample_index"
                ).fetchall()
                self.assertEqual(indices, [(0,), (1,), (2,), (3,)])
                # Scheduling reads wall time once, then stays on monotonic time.
                self.assertEqual(clock.calls, 1)
                database.close()


if __name__ == "__main__":
    unittest.main()
