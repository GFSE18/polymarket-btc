import unittest

import numpy as np

import btc_5m_twap_strategy_visualizer as strategy


def market(
    up_bid,
    up_ask,
    down_bid,
    down_ask,
    *,
    outcome_up=True,
):
    length = len(up_bid)
    return strategy.MarketSeries(
        key="test:1",
        slug="test-market",
        sample_index=np.arange(length),
        up_bid=np.asarray(up_bid, dtype=float),
        up_ask=np.asarray(up_ask, dtype=float),
        down_bid=np.asarray(down_bid, dtype=float),
        down_ask=np.asarray(down_ask, dtype=float),
        outcome_up=outcome_up,
    )


class StrategyTests(unittest.TestCase):
    def test_one_contract_hits_take_profit_at_bid(self):
        series = market(
            [0.48, 0.58, 0.62, 0.72],
            [0.50, 0.61, 0.64, 0.74],
            [0.48, 0.38, 0.34, 0.24],
            [0.50, 0.40, 0.36, 0.26],
        )
        result = strategy.evaluate_strategy(
            [series], 0.60, np.array([0.10]), np.array([0.10])
        )
        expected = 0.72 - strategy.taker_fee(0.72) - 0.61 - strategy.taker_fee(0.61)
        self.assertEqual(result.entered_markets, 1)
        self.assertEqual(result.take_profit_exits[0, 0], 1)
        self.assertAlmostEqual(result.net_profit[0, 0], expected)

    def test_stop_loss_uses_actual_bid(self):
        series = market(
            [0.48, 0.58, 0.55, 0.47],
            [0.50, 0.61, 0.58, 0.50],
            [0.48, 0.38, 0.41, 0.49],
            [0.50, 0.40, 0.44, 0.52],
            outcome_up=False,
        )
        result = strategy.evaluate_strategy(
            [series], 0.60, np.array([0.10]), np.array([0.10])
        )
        expected = 0.47 - strategy.taker_fee(0.47) - 0.61 - strategy.taker_fee(0.61)
        self.assertEqual(result.stop_loss_exits[0, 0], 1)
        self.assertAlmostEqual(result.net_profit[0, 0], expected)

    def test_same_sample_entry_tie_is_skipped(self):
        series = market(
            [0.48, 0.58],
            [0.50, 0.61],
            [0.48, 0.58],
            [0.50, 0.61],
        )
        result = strategy.evaluate_strategy(
            [series], 0.60, np.array([0.10]), np.array([0.10])
        )
        self.assertEqual(result.entered_markets, 0)
        self.assertEqual(result.tied_entry_markets, 1)
        self.assertEqual(result.net_profit[0, 0], 0)

    def test_parameter_grid_never_uses_zero_stop(self):
        tp, sl = strategy.parameter_grid(0.51)
        self.assertGreaterEqual(float(np.min(0.51 - sl)), strategy.MIN_STOP_PRICE)
        self.assertLessEqual(float(np.max(0.51 + tp)), strategy.MAX_TAKE_PROFIT_PRICE)


if __name__ == "__main__":
    unittest.main()
