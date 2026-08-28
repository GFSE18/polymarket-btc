#!/usr/bin/env python3
"""Table-and-Matplotlib report for BTC five-minute bi-leg opportunities.

Run with:
    streamlit run btc_5m_arbitrage_visualizer.py
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


DATA_DIR = Path(__file__).resolve().parent / "btc_5m_statistics_data"
MPL_CONFIG_DIR = Path(__file__).resolve().parent / ".btc_5m_visualizer_cache"
MPL_CONFIG_DIR.mkdir(exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CONFIG_DIR))
DATABASE_PATTERN = "btc_5m_statistics_*.sqlite3"
STRATEGIES = {
    "45/45": 0.45,
    "40/40": 0.40,
    "35/35": 0.35,
}
OUTCOMES = ("Up", "Down")

WINDOW_COLUMNS = [
    "slug",
    "outcome",
    "window_number",
    "start_ms",
    "end_ms",
    "duration_ms",
]


@dataclass
class DailyData:
    path: Path
    metadata: dict[str, str]
    markets: pd.DataFrame
    quotes: pd.DataFrame
    ask_segments: pd.DataFrame
    price_scale: int

    @property
    def local_date(self) -> str:
        return self.metadata.get("local_date", self.path.stem.rsplit("_", 1)[-1])

    @property
    def market_count(self) -> int:
        return int(self.markets["slug"].nunique())


@dataclass
class StrategyAnalysis:
    strategy: str
    threshold: float
    successful_slugs: set[str]
    windows: pd.DataFrame
    window_statistics: pd.DataFrame
    timing: pd.DataFrame


def discover_databases(data_dir: Path = DATA_DIR) -> list[Path]:
    if not data_dir.is_dir():
        return []
    return sorted(path for path in data_dir.glob(DATABASE_PATTERN) if path.is_file())


def database_signature(path: Path) -> tuple[tuple[str, int, int], ...]:
    """Include SQLite sidecars so Streamlit notices a running collector."""
    signature: list[tuple[str, int, int]] = []
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            stat = candidate.stat()
        except FileNotFoundError:
            continue
        signature.append((candidate.name, stat.st_mtime_ns, stat.st_size))
    return tuple(signature)


def database_market_count(path: Path) -> int:
    connection = _read_only_connection(path)
    try:
        row = connection.execute("SELECT COUNT(DISTINCT slug) FROM markets").fetchone()
    finally:
        connection.close()
    return int(row[0]) if row is not None else 0


def _read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro",
        uri=True,
        timeout=5,
    )
    connection.execute("PRAGMA query_only = ON")
    return connection


def _utc_text_to_epoch_ms(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, utc=True, errors="coerce")
    result = pd.Series(np.nan, index=values.index, dtype="float64")
    valid = parsed.notna()
    if valid.any():
        result.loc[valid] = parsed.loc[valid].map(lambda value: value.timestamp() * 1000)
    return result


def _local_datetime(epoch_ms: int | float) -> datetime:
    return datetime.fromtimestamp(float(epoch_ms) / 1000).astimezone()


def _build_ask_segments(
    markets: pd.DataFrame,
    quotes: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_id",
        "slug",
        "outcome",
        "start_ms",
        "end_ms",
        "ask_micros",
    ]
    if markets.empty or quotes.empty:
        return pd.DataFrame(columns=columns)

    market_info = markets[
        ["id", "slug", "market_duration_ms", "coverage_end_ms"]
    ].rename(columns={"id": "market_id"})
    segments = quotes.merge(market_info, on="market_id", how="inner")
    segments = segments.sort_values(
        ["market_id", "outcome", "observed_elapsed_ms", "id"]
    ).copy()
    segments["start_ms"] = segments["observed_elapsed_ms"].clip(lower=0)
    segments["end_ms"] = segments.groupby(
        ["market_id", "outcome"], sort=False
    )["observed_elapsed_ms"].shift(-1)
    segments["end_ms"] = segments["end_ms"].fillna(segments["coverage_end_ms"])
    segments["start_ms"] = segments[
        ["start_ms", "market_duration_ms"]
    ].min(axis=1)
    segments["end_ms"] = segments[
        ["end_ms", "coverage_end_ms", "market_duration_ms"]
    ].min(axis=1)
    segments = segments.loc[segments["end_ms"] >= segments["start_ms"]].copy()
    segments["start_ms"] = segments["start_ms"].round().astype("int64")
    segments["end_ms"] = segments["end_ms"].round().astype("int64")
    segments["ask_micros"] = segments["best_ask_micros"]
    return segments[columns].reset_index(drop=True)


def load_daily_data(path: Path | str) -> DailyData:
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)

    connection = _read_only_connection(path)
    try:
        metadata = dict(connection.execute("SELECT key, value FROM dataset_metadata"))
        markets = pd.read_sql_query(
            "SELECT * FROM markets ORDER BY window_start_epoch_ms, id",
            connection,
        )
        quotes = pd.read_sql_query(
            """
            SELECT id, market_id, outcome, observed_elapsed_ms,
                   observed_epoch_ms, source_epoch_ms,
                   best_bid_micros, best_ask_micros
            FROM book_quotes
            ORDER BY market_id, outcome, observed_elapsed_ms, id
            """,
            connection,
        )
    finally:
        connection.close()

    price_scale = int(metadata.get("price_scale", "1000000"))

    if not markets.empty:
        markets["market_duration_ms"] = (
            markets["end_epoch_ms"] - markets["window_start_epoch_ms"]
        ).clip(lower=0)
        observed_end_ms = _utc_text_to_epoch_ms(markets["observed_end_utc"])
        markets["coverage_end_ms"] = (
            observed_end_ms - markets["window_start_epoch_ms"]
        )
        if not quotes.empty:
            last_quote = quotes.groupby("market_id")["observed_elapsed_ms"].max()
            quote_coverage = markets["id"].map(last_quote)
            markets["coverage_end_ms"] = pd.concat(
                [markets["coverage_end_ms"], quote_coverage], axis=1
            ).max(axis=1, skipna=True)
        markets["coverage_end_ms"] = markets["coverage_end_ms"].fillna(0).clip(
            lower=0,
            upper=markets["market_duration_ms"],
        )
        markets["window_start_local"] = markets["window_start_epoch_ms"].map(
            _local_datetime
        )

    ask_segments = _build_ask_segments(markets, quotes)
    return DailyData(
        path=path,
        metadata=metadata,
        markets=markets,
        quotes=quotes,
        ask_segments=ask_segments,
        price_scale=price_scale,
    )


def qualifying_windows(data: DailyData, threshold: float) -> pd.DataFrame:
    """Return merged continuous intervals with best ask at or below threshold."""
    threshold_micros = int(round(threshold * data.price_scale))
    segments = data.ask_segments.loc[
        data.ask_segments["ask_micros"].notna()
        & (data.ask_segments["ask_micros"] <= threshold_micros)
    ].copy()
    if segments.empty:
        return pd.DataFrame(columns=WINDOW_COLUMNS)

    keys = ["slug", "outcome"]
    segments = segments.sort_values(keys + ["start_ms", "end_ms", "market_id"])
    segments["previous_running_end"] = segments.groupby(keys, sort=False)[
        "end_ms"
    ].transform(lambda values: values.cummax().shift(1))
    segments["new_window"] = (
        segments["previous_running_end"].isna()
        | (segments["start_ms"] > segments["previous_running_end"])
    )
    segments["window_number"] = segments.groupby(keys, sort=False)[
        "new_window"
    ].cumsum()
    windows = (
        segments.groupby(keys + ["window_number"], as_index=False)
        .agg(start_ms=("start_ms", "min"), end_ms=("end_ms", "max"))
        .sort_values(keys + ["start_ms"])
    )
    windows["duration_ms"] = windows["end_ms"] - windows["start_ms"]
    return windows[WINDOW_COLUMNS].reset_index(drop=True)


def successful_market_slugs(windows: pd.DataFrame) -> set[str]:
    if windows.empty:
        return set()
    up = set(windows.loc[windows["outcome"] == "Up", "slug"])
    down = set(windows.loc[windows["outcome"] == "Down", "slug"])
    return up & down


def opportunity_table(
    data: DailyData,
    windows_by_strategy: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, int | float | str]] = []
    for strategy, threshold in STRATEGIES.items():
        successful = successful_market_slugs(windows_by_strategy[strategy])
        rows.append(
            {
                "Strategy": strategy,
                "Threshold per leg": threshold,
                "Successful markets": len(successful),
                "Observed markets": data.market_count,
                "Opportunity rate": (
                    len(successful) / data.market_count if data.market_count else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def _window_statistics(windows: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, int | float | str]] = []
    for outcome in OUTCOMES:
        group = windows.loc[windows["outcome"] == outcome]
        durations = group["duration_ms"] / 1000
        rows.append(
            {
                "Outcome": outcome,
                "Markets": int(group["slug"].nunique()),
                "Windows": len(group),
                "Total seconds": durations.sum() if not durations.empty else 0.0,
                "Mean seconds": durations.mean() if not durations.empty else np.nan,
                "Median seconds": durations.median() if not durations.empty else np.nan,
                "P25 seconds": durations.quantile(0.25) if not durations.empty else np.nan,
                "P75 seconds": durations.quantile(0.75) if not durations.empty else np.nan,
                "P90 seconds": durations.quantile(0.90) if not durations.empty else np.nan,
                "Maximum seconds": durations.max() if not durations.empty else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _timing_table(
    data: DailyData,
    windows: pd.DataFrame,
    successful_slugs: set[str],
) -> pd.DataFrame:
    columns = [
        "slug",
        "market_start_local",
        "first_leg",
        "finish_leg",
        "earliest_first_seconds",
        "last_finish_seconds",
        "available_span_seconds",
    ]
    if not successful_slugs:
        return pd.DataFrame(columns=columns)

    market_starts = (
        data.markets.groupby("slug")["window_start_local"].min().to_dict()
    )
    rows: list[dict[str, float | str | datetime]] = []
    for slug in sorted(successful_slugs, key=lambda value: market_starts[value]):
        market_windows = windows.loc[windows["slug"] == slug]
        up = market_windows.loc[market_windows["outcome"] == "Up"]
        down = market_windows.loc[market_windows["outcome"] == "Down"]
        earliest_up = int(up["start_ms"].min())
        earliest_down = int(down["start_ms"].min())

        if earliest_up < earliest_down:
            first_leg = "Up"
            finish_leg = "Down"
            earliest_first = earliest_up
            last_finish = int(down["end_ms"].max())
        elif earliest_down < earliest_up:
            first_leg = "Down"
            finish_leg = "Up"
            earliest_first = earliest_down
            last_finish = int(up["end_ms"].max())
        else:
            first_leg = "Simultaneous"
            finish_leg = "Either"
            earliest_first = earliest_up
            last_finish = int(max(up["end_ms"].max(), down["end_ms"].max()))

        rows.append(
            {
                "slug": slug,
                "market_start_local": market_starts[slug],
                "first_leg": first_leg,
                "finish_leg": finish_leg,
                "earliest_first_seconds": earliest_first / 1000,
                "last_finish_seconds": last_finish / 1000,
                "available_span_seconds": max(0, last_finish - earliest_first) / 1000,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def analyze_strategy(
    data: DailyData,
    strategy: str,
    windows: pd.DataFrame | None = None,
) -> StrategyAnalysis:
    if strategy not in STRATEGIES:
        raise ValueError(f"unsupported strategy: {strategy}")
    threshold = STRATEGIES[strategy]
    all_windows = qualifying_windows(data, threshold) if windows is None else windows
    successful = successful_market_slugs(all_windows)
    filtered = all_windows.loc[all_windows["slug"].isin(successful)].copy()
    return StrategyAnalysis(
        strategy=strategy,
        threshold=threshold,
        successful_slugs=successful,
        windows=filtered,
        window_statistics=_window_statistics(filtered),
        timing=_timing_table(data, filtered, successful),
    )


def analyze_day(
    path: Path | str,
) -> tuple[DailyData, dict[str, pd.DataFrame], pd.DataFrame]:
    data = load_daily_data(path)
    windows = {
        strategy: qualifying_windows(data, threshold)
        for strategy, threshold in STRATEGIES.items()
    }
    return data, windows, opportunity_table(data, windows)


def expected_market_count(dataset_date: str) -> int:
    parsed = date.fromisoformat(dataset_date)
    start = datetime.combine(parsed, datetime_time.min).astimezone()
    end = datetime.combine(parsed + timedelta(days=1), datetime_time.min).astimezone()
    return int(round((end.timestamp() - start.timestamp()) / 300))


def _style_axis(axis, dark: bool) -> None:
    foreground = "#f3f4f6" if dark else "#111827"
    grid = "#4b5563" if dark else "#d1d5db"
    background = "#0e1117" if dark else "#ffffff"
    axis.set_facecolor(background)
    axis.tick_params(colors=foreground)
    axis.xaxis.label.set_color(foreground)
    axis.yaxis.label.set_color(foreground)
    axis.title.set_color(foreground)
    for spine in axis.spines.values():
        spine.set_color(grid)
    axis.grid(True, color=grid, linewidth=0.6, alpha=0.55)


def plot_window_lengths(analysis: StrategyAnalysis, dark: bool = False):
    import matplotlib.pyplot as plt

    background = "#0e1117" if dark else "#ffffff"
    figure, axes = plt.subplots(1, 2, figsize=(12, 4.2), sharex=True, sharey=True)
    figure.patch.set_facecolor(background)
    colors = {"Up": "#3b82f6", "Down": "#f97316"}
    bins = np.linspace(0, 300, 31)

    for axis, outcome in zip(axes, OUTCOMES):
        durations = (
            analysis.windows.loc[analysis.windows["outcome"] == outcome, "duration_ms"]
            / 1000
        )
        if durations.empty:
            axis.text(0.5, 0.5, "No qualifying windows", ha="center", va="center", transform=axis.transAxes)
        else:
            axis.hist(durations, bins=bins, color=colors[outcome], alpha=0.82, edgecolor=background)
        axis.set_title(f"{outcome} windows (n={len(durations):,})")
        axis.set_xlabel("Continuous window length (seconds)")
        axis.set_ylabel("Window count")
        axis.set_xlim(0, 300)
        _style_axis(axis, dark)

    figure.suptitle(
        f"{analysis.strategy} window lengths — successful bi-leg markets only",
        color="#f3f4f6" if dark else "#111827",
    )
    figure.tight_layout()
    return figure


def plot_success_timeline(
    data: DailyData,
    analysis: StrategyAnalysis,
    dark: bool = False,
):
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    background = "#0e1117" if dark else "#ffffff"
    foreground = "#f3f4f6" if dark else "#111827"
    strategy_colors = {
        "45/45": "#3b82f6",
        "40/40": "#f97316",
        "35/35": "#22c55e",
    }
    figure, axis = plt.subplots(figsize=(13, 2.25))
    figure.patch.set_facecolor(background)
    _style_axis(axis, dark)

    selected_markets = (
        data.markets.loc[
            data.markets["slug"].isin(analysis.successful_slugs),
            ["slug", "window_start_local"],
        ]
        .drop_duplicates("slug")
        .sort_values("window_start_local")
    )
    local_starts = [
        value.replace(tzinfo=None)
        for value in selected_markets["window_start_local"].tolist()
    ]
    if local_starts:
        axis.scatter(
            local_starts,
            np.zeros(len(local_starts)),
            s=24,
            color=strategy_colors[analysis.strategy],
            alpha=0.82,
            edgecolors="none",
            zorder=3,
        )
    else:
        axis.text(
            0.5,
            0.62,
            "No successful markets",
            color=foreground,
            ha="center",
            va="center",
            transform=axis.transAxes,
        )

    local_day = date.fromisoformat(data.local_date)
    day_start = datetime.combine(local_day, datetime_time.min)
    day_end = day_start + timedelta(days=1)
    axis.axhline(0, color=foreground, linewidth=0.8, alpha=0.55, zorder=1)
    axis.set_xlim(day_start, day_end)
    axis.set_ylim(-0.75, 0.75)
    axis.set_yticks([])
    axis.xaxis.set_major_locator(mdates.HourLocator(interval=3))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    axis.set_xlabel("Market start (local time)")
    axis.set_title(
        f"{analysis.strategy} successful-market timing "
        f"(n={len(analysis.successful_slugs):,})"
    )
    axis.grid(False)
    axis.xaxis.grid(True, color="#4b5563" if dark else "#d1d5db", linewidth=0.6, alpha=0.55)
    figure.tight_layout()
    return figure


def plot_opportunity_timing(analysis: StrategyAnalysis, dark: bool = False):
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    background = "#0e1117" if dark else "#ffffff"
    foreground = "#f3f4f6" if dark else "#111827"
    figure, axis = plt.subplots(figsize=(13, 6))
    figure.patch.set_facecolor(background)
    _style_axis(axis, dark)

    timing = analysis.timing
    colors = {
        "Up": "#3b82f6",
        "Down": "#f97316",
        "Simultaneous": "#a855f7",
    }
    if timing.empty:
        axis.text(0.5, 0.5, "No successful markets", ha="center", va="center", transform=axis.transAxes)
    else:
        for row in timing.itertuples(index=False):
            color = colors[row.first_leg]
            # Matplotlib otherwise normalizes aware datetimes to UTC before
            # applying the formatter, even though the report is labeled local.
            market_start = row.market_start_local.replace(tzinfo=None)
            axis.plot(
                [market_start, market_start],
                [row.earliest_first_seconds, row.last_finish_seconds],
                color=color,
                alpha=0.22,
                linewidth=0.8,
                zorder=1,
            )
            axis.scatter(
                market_start,
                row.earliest_first_seconds,
                color=color,
                s=20,
                marker="o",
                alpha=0.8,
                zorder=2,
            )
            axis.scatter(
                market_start,
                row.last_finish_seconds,
                color=color,
                s=28,
                marker="x",
                linewidths=1.1,
                alpha=0.9,
                zorder=3,
            )

        axis.xaxis.set_major_locator(mdates.HourLocator(interval=3))
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        handles = [
            Line2D([0], [0], marker="o", color="none", markerfacecolor=foreground, markeredgecolor=foreground, label="Earliest first-leg opportunity"),
            Line2D([0], [0], marker="x", color=foreground, linestyle="none", label="Last opportunity to buy other leg"),
            Line2D([0], [0], marker="o", color="none", markerfacecolor=colors["Up"], label="Up first"),
            Line2D([0], [0], marker="o", color="none", markerfacecolor=colors["Down"], label="Down first"),
            Line2D([0], [0], marker="o", color="none", markerfacecolor=colors["Simultaneous"], label="Simultaneous"),
        ]
        legend = axis.legend(handles=handles, ncol=3, loc="upper center", bbox_to_anchor=(0.5, 1.20), frameon=False)
        for text in legend.get_texts():
            text.set_color(foreground)

    axis.set_title(f"{analysis.strategy} first-leg and last-finish opportunities")
    axis.set_xlabel("Market start (local time)")
    axis.set_ylabel("Elapsed within five-minute market (seconds)")
    axis.set_ylim(0, 300)
    axis.set_yticks(np.arange(0, 301, 60))
    figure.autofmt_xdate(rotation=0)
    figure.tight_layout()
    return figure


def run_app() -> None:
    import matplotlib.pyplot as plt
    import streamlit as st

    st.set_page_config(
        page_title="BTC 5m Bi-leg Opportunities",
        page_icon="₿",
        layout="wide",
    )

    @st.cache_data(ttl=15, show_spinner="Analyzing daily quotes…")
    def cached_analysis(path_text: str, signature: tuple):
        del signature
        return analyze_day(path_text)

    paths = discover_databases()
    st.title("BTC five-minute bi-leg opportunities")
    if not paths:
        st.error(f"No daily databases found in {DATA_DIR}")
        st.stop()

    date_labels = {
        path: path.stem.removeprefix("btc_5m_statistics_") for path in paths
    }
    ordered_paths = list(reversed(paths))
    default_path = next(
        (path for path in ordered_paths if database_market_count(path) >= 287),
        ordered_paths[0],
    )
    date_column, strategy_column = st.columns(2)
    with date_column:
        selected_path = st.selectbox(
            "Daily dataset",
            options=ordered_paths,
            index=ordered_paths.index(default_path),
            format_func=lambda path: date_labels[path],
        )
    with strategy_column:
        selected_strategy = st.selectbox("Strategy statistics", options=list(STRATEGIES))

    try:
        data, windows_by_strategy, opportunities = cached_analysis(
            str(selected_path), database_signature(selected_path)
        )
    except (OSError, sqlite3.Error, ValueError) as exc:
        st.error(f"Could not analyze {selected_path.name}: {exc}")
        st.stop()

    expected = expected_market_count(data.local_date)
    st.subheader("Markets with both legs available")
    st.caption(
        f"{data.local_date}: {data.market_count:,} distinct markets observed; "
        f"{expected:,} expected for the local day. A leg qualifies whenever its best ask "
        "is observed at or below the strategy threshold."
    )
    display_opportunities = opportunities.copy()
    display_opportunities["Opportunity rate"] = display_opportunities[
        "Opportunity rate"
    ].map(lambda value: "N/A" if pd.isna(value) else f"{value:.1%}")
    st.dataframe(
        display_opportunities,
        hide_index=True,
        width="stretch",
        column_config={
            "Threshold per leg": st.column_config.NumberColumn(format="$%.2f"),
        },
    )

    analysis = analyze_strategy(
        data,
        selected_strategy,
        windows_by_strategy[selected_strategy],
    )
    dark = getattr(getattr(st, "context", None), "theme", None)
    dark_mode = getattr(dark, "type", "light") == "dark"

    success_timeline = plot_success_timeline(data, analysis, dark=dark_mode)
    st.pyplot(success_timeline, width="stretch")
    plt.close(success_timeline)

    st.subheader(f"{selected_strategy} qualifying-window statistics")
    st.caption(
        "Only markets where both Up and Down legs were available are included. "
        "A missing book ends the current continuous window."
    )
    st.dataframe(
        analysis.window_statistics,
        hide_index=True,
        width="stretch",
        column_config={
            column: st.column_config.NumberColumn(format="%.3f")
            for column in analysis.window_statistics.columns
            if column.endswith("seconds")
        },
    )
    window_figure = plot_window_lengths(analysis, dark=dark_mode)
    st.pyplot(window_figure, width="stretch")
    plt.close(window_figure)

    st.subheader(f"{selected_strategy} first-leg and last-finish timing")
    st.caption(
        "Circles are the earliest time either leg became buyable. X markers are the end "
        "of the final qualifying window for the other leg."
    )
    timing_figure = plot_opportunity_timing(analysis, dark=dark_mode)
    st.pyplot(timing_figure, width="stretch")
    plt.close(timing_figure)

    display_timing = analysis.timing.copy()
    if display_timing.empty:
        st.info("No successful markets for this strategy.")
    else:
        display_timing["Market start"] = display_timing["market_start_local"].map(
            lambda value: value.strftime("%H:%M")
        )
        display_timing["Earliest first opportunity"] = display_timing[
            "earliest_first_seconds"
        ].map(lambda value: f"{value:.3f} / 300s")
        display_timing["Last finish opportunity"] = display_timing[
            "last_finish_seconds"
        ].map(lambda value: f"{value:.3f} / 300s")
        display_timing["Available span (s)"] = display_timing[
            "available_span_seconds"
        ]
        display_timing = display_timing[
            [
                "Market start",
                "slug",
                "first_leg",
                "finish_leg",
                "Earliest first opportunity",
                "Last finish opportunity",
                "Available span (s)",
            ]
        ]
        display_timing.columns = [
            "Market start",
            "Market",
            "First leg",
            "Other leg",
            "Earliest first opportunity",
            "Last finish opportunity",
            "Available span (s)",
        ]
        st.dataframe(
            display_timing,
            hide_index=True,
            width="stretch",
            column_config={
                "Available span (s)": st.column_config.NumberColumn(format="%.3f")
            },
        )


if __name__ == "__main__":
    run_app()
