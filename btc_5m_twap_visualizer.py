#!/usr/bin/env python3
"""Interactive MATLAB-style plotter for the BTC 5-minute TWAP datasets.

Run with:
    streamlit run btc_5m_twap_visualizer.py
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pandas as pd


DATA_DIR = Path(__file__).resolve().parent / "btc_5m_twap_statistics_data"
DATABASE_PATTERN = "btc_5m_twap_statistics_*.sqlite3"
MPL_CONFIG_DIR = Path(__file__).resolve().parent / ".btc_5m_twap_visualizer_cache"
MPL_CONFIG_DIR.mkdir(exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CONFIG_DIR))


def discover_databases() -> list[Path]:
    return sorted(
        path for path in DATA_DIR.glob(DATABASE_PATTERN) if path.is_file()
    )


def database_signature(path: Path) -> tuple[tuple[str, int, int], ...]:
    """Include SQLite sidecars so a currently-running collector is noticed."""
    result = []
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            stat = candidate.stat()
        except FileNotFoundError:
            continue
        result.append((candidate.name, stat.st_mtime_ns, stat.st_size))
    return tuple(result)


def read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro",
        uri=True,
        timeout=5,
    )
    connection.execute("PRAGMA query_only = ON")
    return connection


def load_markets(path: Path) -> pd.DataFrame:
    connection = read_only_connection(path)
    try:
        return pd.read_sql_query(
            """
            SELECT id, slug, window_datetime_utc, price_to_beat
            FROM markets
            ORDER BY window_datetime_utc, id
            """,
            connection,
        )
    finally:
        connection.close()


def load_market_samples(path: Path, market_id: int) -> pd.DataFrame:
    connection = read_only_connection(path)
    try:
        return pd.read_sql_query(
            """
            SELECT sample_index, up_bid, up_ask, down_bid, down_ask,
                   twap_price, sample_valid
            FROM samples
            WHERE market_id = ?
            ORDER BY sample_index
            """,
            connection,
            params=(market_id,),
        )
    finally:
        connection.close()


def market_start_et(market: pd.Series) -> pd.Timestamp:
    return pd.to_datetime(market["window_datetime_utc"], utc=True).tz_convert(
        "America/New_York"
    )


def market_name(market: pd.Series) -> str:
    """Create the human-friendly five-minute title used by Polymarket."""
    start = market_start_et(market)
    end = start + pd.Timedelta(minutes=5)
    start_text = start.strftime("%B %d, %Y, %I:%M %p").replace(" 0", " ")
    end_text = end.strftime("%I:%M %p").lstrip("0")
    return f"Bitcoin Up or Down - {start_text}-{end_text} ET"


def plot_market(samples: pd.DataFrame, market: pd.Series, interval_seconds: float):
    import plotly.graph_objects as go

    samples = samples.copy()
    samples["elapsed_seconds"] = samples["sample_index"] * interval_seconds
    valid = samples.loc[samples["sample_valid"].astype(bool)].copy()

    start_et = market_start_et(market)
    valid["time_et"] = start_et + pd.to_timedelta(
        valid["elapsed_seconds"], unit="s"
    )
    customdata = valid[
        [
            "sample_index", "time_et", "elapsed_seconds", "up_bid", "up_ask",
            "down_bid", "down_ask", "twap_price",
        ]
    ].to_numpy()

    book_styles = {
        # Blue/orange is substantially easier to distinguish than red/green
        # for common red-green color-vision deficiencies. Bid/ask also use
        # both line style and marker shape, so color is not the only cue.
        "up_bid": ("#0072B2", "Up bid", "solid", "circle"),
        "up_ask": ("#56B4E9", "Up ask", "dash", "diamond"),
        "down_bid": ("#D55E00", "Down bid", "solid", "square"),
        "down_ask": ("#E69F00", "Down ask", "dash", "x"),
    }
    figure = go.Figure()
    for column, (color, label, linestyle, marker_symbol) in book_styles.items():
        figure.add_trace(
            go.Scatter(
                x=valid["elapsed_seconds"],
                y=valid[column],
                customdata=customdata,
                mode="lines+markers",
                name=label,
                line={"color": color, "width": 2.8, "dash": linestyle},
                marker={"size": 4.5, "color": color, "symbol": marker_symbol},
                hovertemplate=(
                    f"{label}<br>ET: %{{customdata[1]}}<br>"
                    "Elapsed: %{customdata[2]:.2f}s<br>"
                    "Value: %{y:.4f}<extra></extra>"
                ),
                yaxis="y",
            )
        )

    figure.add_trace(
        go.Scatter(
            x=valid["elapsed_seconds"],
            y=valid["twap_price"],
            customdata=customdata,
            mode="lines+markers",
            name="BTC TWAP",
            line={"color": "#111827", "width": 3.0},
            marker={"size": 3.5, "color": "#111827"},
            hovertemplate=(
                "BTC TWAP<br>ET: %{customdata[1]}<br>"
                "Elapsed: %{customdata[2]:.2f}s<br>"
                "BTC: $%{y:,.2f}<extra></extra>"
            ),
            yaxis="y2",
        )
    )
    figure.add_hline(
        y=market["price_to_beat"],
        line={"color": "#6b7280", "width": 2, "dash": "dot"},
        annotation_text=f"Price to beat: ${market['price_to_beat']:,.2f}",
        annotation_position="top right",
        yref="y2",
    )

    figure.update_layout(
        title={"text": market_name(market)},
        height=650,
        hovermode="closest",
        clickmode="event+select",
        margin={"l": 70, "r": 90, "t": 70, "b": 70},
        legend={"orientation": "h", "y": 1.08, "x": 0},
        xaxis={"title": "Elapsed time in market (seconds)", "range": [0, 300], "showspikes": True, "spikemode": "across", "spikethickness": 1},
        yaxis={"title": "Book price / probability", "range": [0, 1]},
        yaxis2={"title": "BTC price (USD)", "overlaying": "y", "side": "right", "showgrid": False},
        template="plotly_white",
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font={"color": "#111827"},
    )
    return figure, len(valid), len(samples)


def run_app() -> None:
    import streamlit as st

    st.set_page_config(page_title="BTC 5m TWAP Plotter", page_icon="₿", layout="wide")
    st.title("BTC 5-minute TWAP plotter")

    paths = discover_databases()
    if not paths:
        st.error(f"No SQLite datasets found in {DATA_DIR}")
        st.stop()

    ordered_paths = list(reversed(paths))
    labels = {
        path: path.stem.removeprefix("btc_5m_twap_statistics_")
        for path in paths
    }

    @st.cache_data(ttl=15, show_spinner=False)
    def cached_markets(path_text: str, signature: tuple[tuple[str, int, int], ...]):
        del signature
        return load_markets(Path(path_text))

    @st.cache_data(ttl=15, show_spinner="Loading market samples…")
    def cached_samples(
        path_text: str,
        market_id: int,
        signature: tuple[tuple[str, int, int], ...],
    ):
        del signature
        return load_market_samples(Path(path_text), market_id)

    day_column, market_column = st.columns(2)
    with day_column:
        selected_path = st.selectbox(
            "Dataset day",
            ordered_paths,
            format_func=lambda path: labels[path],
        )

    try:
        signature = database_signature(selected_path)
        markets = cached_markets(str(selected_path), signature)
    except (OSError, sqlite3.Error, ValueError) as exc:
        st.error(f"Could not read {selected_path.name}: {exc}")
        st.stop()

    if markets.empty:
        st.warning("This dataset has no markets yet.")
        st.stop()

    market_options = markets["id"].tolist()
    market_labels = {
        int(row.id): market_name(pd.Series(row._asdict()))
        for row in markets.itertuples(index=False)
    }
    with market_column:
        selected_market_id = st.selectbox(
            "Market",
            market_options,
            format_func=lambda market_id: market_labels[int(market_id)],
        )

    market = markets.loc[markets["id"] == selected_market_id].iloc[0]
    try:
        samples = cached_samples(str(selected_path), int(selected_market_id), signature)
        connection = read_only_connection(selected_path)
        try:
            metadata = dict(connection.execute("SELECT key, value FROM dataset_metadata"))
        finally:
            connection.close()
        interval_seconds = float(metadata.get("sample_interval_seconds", 0.25))
    except (OSError, sqlite3.Error, ValueError) as exc:
        st.error(f"Could not read market samples: {exc}")
        st.stop()

    figure, valid_count, total_count = plot_market(samples, market, interval_seconds)
    selection = st.plotly_chart(
        figure,
        width="stretch",
        theme=None,
        on_select="rerun",
        selection_mode=["points"],
        key="btc_twap_chart",
    )
    selected_points = getattr(getattr(selection, "selection", None), "points", [])
    if selected_points:
        point = selected_points[0]
        values = point.get("customdata", [])
        if len(values) >= 8:
            st.info(
                f"Selected {point.get('curve_name', 'point')} — "
                f"{values[1]} ET — elapsed {float(values[2]):.2f}s — "
                f"value {float(point.get('y', 0)):.6f}"
            )
    st.caption(
        f"{valid_count:,} valid samples plotted out of {total_count:,} "
        f"at {interval_seconds:g}-second intervals. Book prices use the left axis; "
        "BTC TWAP and price-to-beat use the right axis. Hover for a tooltip, or click a point to keep its value visible."
    )


if __name__ == "__main__":
    run_app()
