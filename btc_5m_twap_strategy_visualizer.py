#!/usr/bin/env python3
"""Interactive one-contract BTC 5-minute entry/exit strategy explorer.

Run with:
    streamlit run btc_5m_twap_strategy_visualizer.py
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


DATA_DIR = Path(__file__).resolve().parent / "btc_5m_twap_statistics_data"
DATABASE_TEMPLATE = "btc_5m_twap_statistics_{date}.sqlite3"

# Inclusive dataset range. Missing dates are skipped.
START_DATASET_DATE = "2026-08-28"
END_DATASET_DATE = "2026-09-04"

MIN_GOOD_BOOK_READINGS = 1_101
ENTRY_PRICE_MIN = 0.51
ENTRY_PRICE_MAX = 0.90
DEFAULT_ENTRY_PRICE = 0.60
MIN_STOP_PRICE = 0.10
MAX_TAKE_PROFIT_PRICE = 0.95
MAX_TAKE_PROFIT_DISTANCE = 0.30
MAX_STOP_LOSS_DISTANCE = 0.30
GRID_STEP = 0.01
TAKER_FEE_RATE = 0.07
PRICE_TOLERANCE = 1e-9


@dataclass(frozen=True)
class MarketSeries:
    key: str
    slug: str
    sample_index: np.ndarray
    up_bid: np.ndarray
    up_ask: np.ndarray
    down_bid: np.ndarray
    down_ask: np.ndarray
    outcome_up: bool


@dataclass(frozen=True)
class BacktestResult:
    net_profit: np.ndarray
    profitable_trades: np.ndarray
    take_profit_exits: np.ndarray
    stop_loss_exits: np.ndarray
    settlement_exits: np.ndarray
    eligible_markets: int
    entered_markets: int
    no_entry_markets: int
    tied_entry_markets: int


def configured_dates() -> list[str]:
    try:
        start = date.fromisoformat(START_DATASET_DATE)
        end = date.fromisoformat(END_DATASET_DATE)
    except ValueError as exc:
        raise ValueError("Dataset dates must use YYYY-MM-DD.") from exc
    if end < start:
        raise ValueError("END_DATASET_DATE must not precede START_DATASET_DATE.")

    values = []
    current = start
    while current <= end:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def discover_databases() -> list[Path]:
    return [
        path
        for value in configured_dates()
        if (path := DATA_DIR / DATABASE_TEMPLATE.format(date=value)).is_file()
    ]


def database_signature(paths: list[Path]) -> tuple[tuple[str, int, int], ...]:
    result = []
    for path in paths:
        for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
            try:
                stat = candidate.stat()
            except FileNotFoundError:
                continue
            result.append((str(candidate), stat.st_mtime_ns, stat.st_size))
    return tuple(result)


def read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=10
    )
    connection.execute("PRAGMA query_only = ON")
    return connection


def load_market_series(paths: list[Path]) -> list[MarketSeries]:
    markets: list[MarketSeries] = []
    for path in paths:
        connection = read_only_connection(path)
        try:
            market_info = pd.read_sql_query(
                """
                WITH eligible AS (
                    SELECT market_id
                    FROM samples
                    GROUP BY market_id
                    HAVING SUM(
                        up_bid IS NOT NULL AND up_ask IS NOT NULL
                        AND down_bid IS NOT NULL AND down_ask IS NOT NULL
                    ) >= ?
                ), last_twap_index AS (
                    SELECT market_id, MAX(sample_index) AS sample_index
                    FROM samples
                    WHERE twap_price IS NOT NULL
                    GROUP BY market_id
                )
                SELECT m.id, m.slug, m.price_to_beat, final.twap_price AS final_twap
                FROM markets AS m
                JOIN eligible AS e ON e.market_id = m.id
                JOIN last_twap_index AS last
                  ON last.market_id = m.id
                JOIN samples AS final
                  ON final.market_id = last.market_id
                 AND final.sample_index = last.sample_index
                WHERE m.price_to_beat IS NOT NULL
                ORDER BY m.id
                """,
                connection,
                params=(MIN_GOOD_BOOK_READINGS,),
            )
            samples = pd.read_sql_query(
                """
                WITH eligible AS (
                    SELECT market_id
                    FROM samples
                    GROUP BY market_id
                    HAVING SUM(
                        up_bid IS NOT NULL AND up_ask IS NOT NULL
                        AND down_bid IS NOT NULL AND down_ask IS NOT NULL
                    ) >= ?
                )
                SELECT s.market_id, s.sample_index,
                       s.up_bid, s.up_ask, s.down_bid, s.down_ask
                FROM samples AS s
                JOIN eligible AS e ON e.market_id = s.market_id
                WHERE s.up_bid IS NOT NULL AND s.up_ask IS NOT NULL
                  AND s.down_bid IS NOT NULL AND s.down_ask IS NOT NULL
                ORDER BY s.market_id, s.sample_index
                """,
                connection,
                params=(MIN_GOOD_BOOK_READINGS,),
            )
        finally:
            connection.close()

        groups = {int(key): group for key, group in samples.groupby("market_id")}
        for row in market_info.itertuples(index=False):
            group = groups.get(int(row.id))
            if group is None:
                continue
            markets.append(
                MarketSeries(
                    key=f"{path.name}:{row.id}",
                    slug=str(row.slug),
                    sample_index=group["sample_index"].to_numpy(dtype=np.int16),
                    up_bid=group["up_bid"].to_numpy(dtype=np.float64),
                    up_ask=group["up_ask"].to_numpy(dtype=np.float64),
                    down_bid=group["down_bid"].to_numpy(dtype=np.float64),
                    down_ask=group["down_ask"].to_numpy(dtype=np.float64),
                    outcome_up=float(row.final_twap) >= float(row.price_to_beat),
                )
            )
    return markets


def taker_fee(price: np.ndarray | float) -> np.ndarray | float:
    return TAKER_FEE_RATE * price * (1.0 - price)


def first_upward_crossing(values: np.ndarray, threshold: float) -> int | None:
    if len(values) < 2:
        return None
    hits = np.flatnonzero(
        (values[:-1] < threshold - PRICE_TOLERANCE)
        & (values[1:] >= threshold - PRICE_TOLERANCE)
    )
    return None if not len(hits) else int(hits[0] + 1)


def first_passage(
    values: np.ndarray, thresholds: np.ndarray, *, rising: bool
) -> tuple[np.ndarray, np.ndarray]:
    sentinel = len(values) + 1
    if not len(values):
        return (
            np.full(len(thresholds), sentinel, dtype=np.int32),
            np.full(len(thresholds), np.nan),
        )
    if rising:
        hit_matrix = values[:, None] >= thresholds[None, :] - PRICE_TOLERANCE
    else:
        hit_matrix = values[:, None] <= thresholds[None, :] + PRICE_TOLERANCE
    any_hit = hit_matrix.any(axis=0)
    positions = np.where(any_hit, hit_matrix.argmax(axis=0), sentinel).astype(np.int32)
    prices = np.full(len(thresholds), np.nan)
    prices[any_hit] = values[positions[any_hit]]
    return positions, prices


def evaluate_strategy(
    markets: list[MarketSeries],
    entry_price: float,
    take_profit_distances: np.ndarray,
    stop_loss_distances: np.ndarray,
) -> BacktestResult:
    shape = (len(stop_loss_distances), len(take_profit_distances))
    net_profit = np.zeros(shape)
    profitable = np.zeros(shape, dtype=np.int32)
    tp_exits = np.zeros(shape, dtype=np.int32)
    sl_exits = np.zeros(shape, dtype=np.int32)
    settlement_exits = np.zeros(shape, dtype=np.int32)
    entered = no_entry = tied_entry = 0

    take_profit_prices = entry_price + take_profit_distances
    stop_loss_prices = entry_price - stop_loss_distances

    for market in markets:
        up_entry = first_upward_crossing(market.up_ask, entry_price)
        down_entry = first_upward_crossing(market.down_ask, entry_price)
        if up_entry is None and down_entry is None:
            no_entry += 1
            continue
        if up_entry is not None and down_entry is not None and up_entry == down_entry:
            tied_entry += 1
            continue

        choose_up = down_entry is None or (
            up_entry is not None and up_entry < down_entry
        )
        entry_position = up_entry if choose_up else down_entry
        assert entry_position is not None
        asks = market.up_ask if choose_up else market.down_ask
        bids = market.up_bid if choose_up else market.down_bid
        fill_price = float(asks[entry_position])
        post_entry_bids = bids[entry_position + 1 :]
        entered += 1

        tp_positions, tp_prices = first_passage(
            post_entry_bids, take_profit_prices, rising=True
        )
        sl_positions, sl_prices = first_passage(
            post_entry_bids, stop_loss_prices, rising=False
        )
        tp_first = tp_positions[None, :] < sl_positions[:, None]
        sl_first = sl_positions[:, None] < tp_positions[None, :]
        settle = ~(tp_first | sl_first)

        settlement_value = float(market.outcome_up == choose_up)
        tp_proceeds = tp_prices - taker_fee(tp_prices)
        sl_proceeds = sl_prices - taker_fee(sl_prices)
        proceeds = np.where(
            tp_first,
            tp_proceeds[None, :],
            np.where(sl_first, sl_proceeds[:, None], settlement_value),
        )
        trade_profit = proceeds - fill_price - taker_fee(fill_price)
        net_profit += trade_profit
        profitable += trade_profit > 0
        tp_exits += tp_first
        sl_exits += sl_first
        settlement_exits += settle

    return BacktestResult(
        net_profit=net_profit,
        profitable_trades=profitable,
        take_profit_exits=tp_exits,
        stop_loss_exits=sl_exits,
        settlement_exits=settlement_exits,
        eligible_markets=len(markets),
        entered_markets=entered,
        no_entry_markets=no_entry,
        tied_entry_markets=tied_entry,
    )


def parameter_grid(entry_price: float) -> tuple[np.ndarray, np.ndarray]:
    max_tp_distance = min(
        MAX_TAKE_PROFIT_DISTANCE, MAX_TAKE_PROFIT_PRICE - entry_price
    )
    max_sl_distance = min(MAX_STOP_LOSS_DISTANCE, entry_price - MIN_STOP_PRICE)
    tp = np.round(
        np.arange(GRID_STEP, max_tp_distance + GRID_STEP / 2, GRID_STEP), 2
    )
    sl = np.round(
        np.arange(GRID_STEP, max_sl_distance + GRID_STEP / 2, GRID_STEP), 2
    )
    return tp, sl


def heatmap_figure(
    result: BacktestResult,
    entry_price: float,
    tp_distances: np.ndarray,
    sl_distances: np.ndarray,
    metric: str,
):
    import plotly.graph_objects as go

    trades = result.entered_markets
    average_profit = (
        result.net_profit / trades if trades else np.zeros_like(result.net_profit)
    )
    win_rate = (
        100.0 * result.profitable_trades / trades
        if trades
        else np.zeros_like(result.net_profit)
    )
    metrics = {
        "Total net profit": (result.net_profit, "Net profit (USDC)"),
        "Average profit per trade": (average_profit, "Profit / trade (USDC)"),
        "Profitable-trade rate": (win_rate, "Profitable trades (%)"),
    }
    values, colorbar_title = metrics[metric]

    tp_grid, sl_grid = np.meshgrid(tp_distances, sl_distances)
    customdata = np.stack(
        [
            entry_price + tp_grid,
            entry_price - sl_grid,
            result.net_profit,
            average_profit,
            win_rate,
            result.take_profit_exits,
            result.stop_loss_exits,
            result.settlement_exits,
        ],
        axis=-1,
    )
    heatmap = go.Heatmap(
        x=tp_distances,
        y=sl_distances,
        z=values,
        customdata=customdata,
        colorscale="RdYlGn",
        zmid=0 if metric != "Profitable-trade rate" else None,
        colorbar={"title": colorbar_title},
        hovertemplate=(
            "TP distance: %{x:.2f}<br>"
            "SL distance: %{y:.2f}<br>"
            "TP price: %{customdata[0]:.2f}<br>"
            "SL price: %{customdata[1]:.2f}<br>"
            "Net profit: %{customdata[2]:.3f} USDC<br>"
            "Profit/trade: %{customdata[3]:.4f} USDC<br>"
            "Profitable trades: %{customdata[4]:.1f}%<br>"
            "TP / SL / settlement exits: %{customdata[5]:.0f} / "
            "%{customdata[6]:.0f} / %{customdata[7]:.0f}"
            "<extra></extra>"
        ),
    )
    figure = go.Figure(heatmap)
    figure.update_layout(
        title=f"{metric} at an entry threshold of {entry_price:.2f}",
        height=680,
        xaxis={"title": "Take-profit distance (TP − entry)", "tickformat": ".2f"},
        yaxis={"title": "Stop-loss distance (entry − SL)", "tickformat": ".2f"},
        template="plotly_white",
        margin={"l": 80, "r": 100, "t": 70, "b": 80},
    )
    return figure


def best_parameters_table(
    result: BacktestResult,
    entry_price: float,
    tp_distances: np.ndarray,
    sl_distances: np.ndarray,
) -> pd.DataFrame:
    rows = []
    trades = result.entered_markets
    for sl_index, sl_distance in enumerate(sl_distances):
        for tp_index, tp_distance in enumerate(tp_distances):
            profit = float(result.net_profit[sl_index, tp_index])
            rows.append(
                {
                    "Entry": entry_price,
                    "TP": entry_price + float(tp_distance),
                    "SL": entry_price - float(sl_distance),
                    "TP − entry": float(tp_distance),
                    "Entry − SL": float(sl_distance),
                    "Net profit": profit,
                    "Profit / trade": profit / trades if trades else 0.0,
                    "Profitable trades": (
                        float(result.profitable_trades[sl_index, tp_index]) / trades
                        if trades
                        else 0.0
                    ),
                }
            )
    return pd.DataFrame(rows).nlargest(10, "Net profit")


def run_app() -> None:
    import streamlit as st

    st.set_page_config(page_title="BTC 5m Strategy Explorer", page_icon="₿", layout="wide")
    st.title("BTC 5-minute one-contract strategy explorer")

    try:
        paths = discover_databases()
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    if not paths:
        st.error("No databases were found in the configured date range.")
        st.stop()

    signature = database_signature(paths)

    @st.cache_data(show_spinner="Loading qualifying order books…")
    def cached_market_series(
        path_texts: tuple[str, ...],
        database_state: tuple[tuple[str, int, int], ...],
    ) -> list[MarketSeries]:
        del database_state
        return load_market_series([Path(value) for value in path_texts])

    markets = cached_market_series(tuple(str(path) for path in paths), signature)
    if not markets:
        st.warning("No markets meet the minimum good-book requirement.")
        st.stop()

    control_one, control_two = st.columns(2)
    with control_one:
        entry_price = st.slider(
            "Entry contract price",
            min_value=ENTRY_PRICE_MIN,
            max_value=ENTRY_PRICE_MAX,
            value=DEFAULT_ENTRY_PRICE,
            step=GRID_STEP,
            format="%.2f",
        )
    with control_two:
        metric = st.selectbox(
            "Heatmap metric",
            ["Total net profit", "Average profit per trade", "Profitable-trade rate"],
        )

    tp_distances, sl_distances = parameter_grid(entry_price)
    result = evaluate_strategy(markets, entry_price, tp_distances, sl_distances)

    summary_columns = st.columns(4)
    summary_columns[0].metric("Eligible markets", f"{result.eligible_markets:,}")
    summary_columns[1].metric("One-contract trades", f"{result.entered_markets:,}")
    summary_columns[2].metric("No entry crossing", f"{result.no_entry_markets:,}")
    summary_columns[3].metric("Same-sample ties skipped", f"{result.tied_entry_markets:,}")

    figure = heatmap_figure(
        result, entry_price, tp_distances, sl_distances, metric
    )
    st.plotly_chart(figure, width="stretch", theme=None)

    st.caption(
        f"Each market can buy exactly one contract. TP is capped at "
        f"{MAX_TAKE_PROFIT_PRICE:.2f}; SL is never below {MIN_STOP_PRICE:.2f}; "
        f"TP/SL distances are capped at {MAX_TAKE_PROFIT_DISTANCE:.2f}/"
        f"{MAX_STOP_LOSS_DISTANCE:.2f}. Entry uses "
        "the first observed upward ask crossing, exits use the following bid "
        "samples, ties are skipped, and taker fees are included. Settlement "
        "outcomes are inferred from the final recorded TWAP versus price-to-beat."
    )

    st.subheader("Top parameter combinations by total net profit")
    table = best_parameters_table(
        result, entry_price, tp_distances, sl_distances
    ).copy()
    st.dataframe(
        table.style.format(
            {
                "Entry": "{:.2f}",
                "TP": "{:.2f}",
                "SL": "{:.2f}",
                "TP − entry": "{:.2f}",
                "Entry − SL": "{:.2f}",
                "Net profit": "{:.3f}",
                "Profit / trade": "{:.4f}",
                "Profitable trades": "{:.1%}",
            }
        ),
        width="stretch",
        hide_index=True,
    )


if __name__ == "__main__":
    run_app()
