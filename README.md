# Polymarket BTC 5-Minute Research Toolkit

Python tools for collecting and analyzing Polymarket's Bitcoin five-minute Up/Down markets. Record synchronized order-book quotes and BTC/USD 60-second TWAP data, inspect individual markets, and explore historical entry/exit strategies through interactive Streamlit dashboards.

This project collects public market data and runs local analysis. It does not place trades.

## Features

- Discover the current BTC five-minute market automatically.
- Sample Up/Down best bids and asks alongside the Chainlink 60-second TWAP at 4 Hz.
- Fetch each market's price-to-beat directly from Polymarket.
- Store daily datasets in SQLite, with indexed sample slots and validity flags.
- Plot market quotes, TWAP, and price-to-beat in a browser.
- Explore one-contract strategies with take-profit/stop-loss heatmaps and modeled taker fees.
- Analyze historical two-leg opportunity windows using the older dataset format.
- Summarize data quality and optionally remove flat order-book records after backing up the database.

## Setup

Use Python 3.10 or newer. From the project directory, create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

On macOS or Linux, activate it with `source .venv/bin/activate`.

Install the dashboard dependencies and the collector's WebSocket dependency:

```sh
python -m pip install -r requirements-arbitrage-visualizer.txt
python -m pip install websockets
```

The requirements file covers the visualizers; `websockets` is installed separately for the collector. Public market discovery and collection do not require a wallet, private key, or API credentials. Collection requires internet access; the dashboards read local datasets.

## Collect market data

Check which market is currently active:

```sh
python fetch_current_btc_5m.py
```

Start continuous collection:

```sh
python btc_5m_twap_statistics.py
```

The collector follows successive five-minute markets, reconnects feeds when needed, and rotates databases by UTC date. Press **Ctrl+C** to commit pending data and close the database.

Datasets are saved beside the scripts:

```text
btc_5m_twap_statistics_data/
  btc_5m_twap_statistics_YYYY-MM-DD.sqlite3
```

Each market has 1,200 indexed sample slots at 250 ms intervals. Missing or unusable observations are represented through nullable values and the `sample_valid` flag; 1,200 slots do not imply 1,200 valid observations.

## Explore the data

Run one dashboard at a time and open the local URL printed by Streamlit.

### Market and TWAP viewer

```sh
python -m streamlit run btc_5m_twap_visualizer.py
```

Select a dataset day and market to inspect quotes and the relationship between TWAP and price-to-beat. This viewer reads the current collector's `btc_5m_twap_statistics_data/` directory.

### Entry/exit strategy explorer

```sh
python -m streamlit run btc_5m_twap_strategy_visualizer.py
```

Choose an entry price and compare take-profit/stop-loss combinations by total net profit, average profit per trade, or profitable-trade rate.

Before running, set `START_DATASET_DATE` and `END_DATASET_DATE` near the top of the script to match your datasets. The checked-in range is **2026-08-28 through 2026-09-04**; missing dates are skipped. Markets must meet the configured minimum of 1,101 good order-book readings.

The model buys at the first observed upward ask crossing and checks subsequent bids for exits. It skips simultaneous entry ties, includes its configured taker-fee model, and infers settlement from the final recorded TWAP versus price-to-beat. These are historical simulations; sampled quotes do not establish executable fills or account for available depth, latency, or slippage. Review the fee constant and settlement assumptions before interpreting results.

### Historical arbitrage explorer

```sh
python -m streamlit run btc_5m_arbitrage_visualizer.py
```

Inspect per-leg ask thresholds of 0.45, 0.40, and 0.35, opportunity durations, and timing across markets.

This dashboard reads **the older dataset format** in `btc_5m_statistics_data/btc_5m_statistics_*.sqlite3`. It does not read the current TWAP collector's databases. The earlier collector is preserved in `old/btc_5m_statistics.py`.

## Data quality and maintenance

Summarize good order-book record counts for specific UTC dataset dates:

```sh
python btc_5m_twap_valid_record_summary.py 2026-09-01 2026-09-02
```

Without arguments, the summary uses the date range configured in that script.

Preview markets whose order books remain flat:

```sh
python cleanup_flat_twap_books.py
```

To back up the affected databases and remove matching markets and their samples:

```sh
python cleanup_flat_twap_books.py --apply
```

Backups are written to `btc_5m_twap_statistics_data/pre_flat_book_cleanup/`. The apply operation modifies datasets and verifies database integrity afterward.

## SQLite structure

| Table | Contents |
| --- | --- |
| `dataset_metadata` | UTC date, sampling interval, sample count, and TWAP topic |
| `runs` | Collector session start and end timestamps |
| `markets` | Market slug, UTC window timestamp, and price-to-beat |
| `samples` | Market ID, sample index, Up/Down bids and asks, TWAP, and validity flag |

Samples use `(market_id, sample_index)` as their primary key. Databases use SQLite write-ahead logging; the visualizers open them in read-only mode.

## Project files

| File | Purpose |
| --- | --- |
| `fetch_current_btc_5m.py` | Discover and validate the active market through the Gamma API |
| `btc_5m_twap_statistics.py` | Collect synchronized order-book and TWAP samples |
| `btc_5m_twap_visualizer.py` | View individual markets |
| `btc_5m_twap_strategy_visualizer.py` | Compare historical entry/exit parameters |
| `btc_5m_arbitrage_visualizer.py` | Analyze opportunity windows in older datasets |
| `btc_5m_twap_valid_record_summary.py` | Report order-book coverage |
| `cleanup_flat_twap_books.py` | Preview or remove flat-book markets with backups |
| `test_*.py` | Unit tests for collection, storage, and analysis logic |
| `old/` | Earlier collectors, feed experiments, and log analysis tools |

Collector timing, feed URLs, output paths, and analysis parameters are configured as constants near the top of their respective scripts.

## Tests

After installing the dependencies, run the current test suite:

```sh
python -m unittest test_btc_5m_twap_statistics test_btc_5m_arbitrage_visualizer test_btc_5m_twap_strategy_visualizer
```

The explicit module list excludes the historical experiments under `old/`.
