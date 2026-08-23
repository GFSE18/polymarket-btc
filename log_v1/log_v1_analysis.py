import sys
import json
import requests
import pandas as pd
from tabulate import tabulate

print("SCRIPT STARTED")

GAMMA_API = "https://gamma-api.polymarket.com"


def get_market_result(slug):
    """
    Fetch the resolved outcome from Polymarket using the market/event slug.
    Returns 'Up', 'Down', or 'Unresolved'.
    """
    url = f"{GAMMA_API}/events/slug/{slug}"

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        event = r.json()

        markets = event.get("markets", [])
        if not markets:
            return "Unresolved"

        market = markets[0]

        outcomes = market.get("outcomes", [])
        prices = market.get("outcomePrices", [])

        # Gamma sometimes returns these as JSON strings
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)

        if isinstance(prices, str):
            prices = json.loads(prices)

        prices = [float(x) for x in prices]

        if len(outcomes) != len(prices):
            return "Unresolved"

        # A resolved winning outcome should be priced at 1
        winner_index = max(range(len(prices)), key=lambda i: prices[i])

        if prices[winner_index] < 0.99:
            return "Unresolved"

        return outcomes[winner_index]

    except Exception as e:
        print(f"WARNING: Could not resolve {slug}: {e}")
        return "Unresolved"


def short_market_name(row):
    """
    Turn:
        Bitcoin Up or Down - August 23, 4:00AM-4:05AM ET
    into:
        4:00-4:05 5m
    """

    market = row["market"]
    duration = row["duration"]

    # Keep only the time portion
    if ", " in market:
        time_part = market.split(", ")[-1]
    else:
        time_part = market

    time_part = time_part.replace(" ET", "")

    # Remove AM/PM for a cleaner table
    time_part = (
        time_part.replace("AM", "")
        .replace("PM", "")
    )

    # Normalize duration
    try:
        duration = int(float(duration))
        duration_text = f"{duration}m"
    except:
        duration_text = str(duration)

    return f"{time_part} {duration_text}"


def money(x):
    return f"${x:,.2f}"


def pnl_money(x):
    if x > 0:
        return f"+${x:,.2f}"
    elif x < 0:
        return f"-${abs(x):,.2f}"
    else:
        return "$0.00"


def analyze_csv(filename):
    df = pd.read_csv(filename)

    required_columns = [
        "market",
        "market_slug",
        "duration",
        "side",
        "outcome",
        "shares",
        "cash",
        "fee",
    ]

    missing = [c for c in required_columns if c not in df.columns]

    if missing:
        raise ValueError(
            "CSV is missing required columns: "
            + ", ".join(missing)
        )

    # Convert numeric fields
    for col in ["shares", "cash", "fee"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # This version assumes the logs contain BUY fills like your current exports.
    non_buys = df[df["side"].str.upper() != "BUY"]

    if len(non_buys) > 0:
        print(
            "\nWARNING: This CSV contains SELL transactions.\n"
            "This script currently calculates BUY-only position cost basis.\n"
        )

    buys = df[df["side"].str.upper() == "BUY"].copy()

    rows = []

    # Preserve chronological market order
    grouped = buys.groupby(
        ["market", "market_slug", "duration"],
        sort=False
    )

    print("\nFetching market resolutions from Polymarket...\n")

    for (market, slug, duration), group in grouped:

        up = group[group["outcome"].str.lower() == "up"]
        down = group[group["outcome"].str.lower() == "down"]

        up_shares = up["shares"].sum()
        down_shares = down["shares"].sum()

        up_cost = up["cash"].sum()
        down_cost = down["cash"].sum()

        fees = group["fee"].sum()

        total_cost = up_cost + down_cost + fees

        result = get_market_result(slug)

        if result.lower() == "up":
            payout = up_shares

        elif result.lower() == "down":
            payout = down_shares

        else:
            payout = 0

        if result == "Unresolved":
            pnl = None
        else:
            pnl = payout - total_cost

        # Construct short market name
        fake_row = {
            "market": market,
            "duration": duration,
        }

        name = short_market_name(fake_row)

        rows.append({
            "Market": name,
            "Up shares": up_shares,
            "Up share cost": up_cost,
            "Down shares": down_shares,
            "Down share cost": down_cost,
            "Result": result,
            "Fees": fees,
            "Total cost": total_cost,
            "Payout": payout,
            "P/L": pnl,
        })

    result_df = pd.DataFrame(rows)

    # Totals
    total_up_shares = result_df["Up shares"].sum()
    total_up_cost = result_df["Up share cost"].sum()

    total_down_shares = result_df["Down shares"].sum()
    total_down_cost = result_df["Down share cost"].sum()

    total_fees = result_df["Fees"].sum()
    total_cost = result_df["Total cost"].sum()
    total_payout = result_df["Payout"].sum()

    resolved = result_df["P/L"].notna()

    if resolved.all():
        total_pnl = result_df["P/L"].sum()
    else:
        total_pnl = None

    # Format for console
    table = []

    for _, row in result_df.iterrows():

        if pd.isna(row["P/L"]):
            pnl_text = "UNRESOLVED"
        else:
            pnl_text = pnl_money(row["P/L"])

        table.append([
            row["Market"],
            f'{row["Up shares"]:,.3f}',
            money(row["Up share cost"]),
            f'{row["Down shares"]:,.3f}',
            money(row["Down share cost"]),
            row["Result"],
            money(row["Fees"]),
            money(row["Total cost"]),
            money(row["Payout"]),
            pnl_text,
        ])

    # Total row
    table.append([
        "TOTAL",
        f"{total_up_shares:,.3f}",
        money(total_up_cost),
        f"{total_down_shares:,.3f}",
        money(total_down_cost),
        "",
        money(total_fees),
        money(total_cost),
        money(total_payout),
        pnl_money(total_pnl) if total_pnl is not None else "PARTIAL",
    ])

    headers = [
        "Market",
        "Up shares",
        "Up share cost",
        "Down shares",
        "Down share cost",
        "Result",
        "Fees",
        "Total cost",
        "Payout",
        "P/L",
    ]

    print(
        tabulate(
            table,
            headers=headers,
            tablefmt="rounded_grid",
            stralign="right",
            numalign="right",
        )
    )

    print("\nSUMMARY")
    print("-" * 40)
    print(f"Up share cost:     {money(total_up_cost)}")
    print(f"Down share cost:   {money(total_down_cost)}")
    print(f"Raw share cost:    {money(total_up_cost + total_down_cost)}")
    print(f"Fees:              {money(total_fees)}")
    print(f"Total cost:        {money(total_cost)}")
    print(f"Payout:            {money(total_payout)}")

    if total_pnl is not None:
        print(f"NET P/L:           {pnl_money(total_pnl)}")

        if total_cost > 0:
            roi = total_pnl / total_cost * 100
            print(f"ROI:               {roi:+.2f}%")

    else:
        print("NET P/L:           Cannot calculate until all markets resolve")


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python analyze_trades.py trades.csv")
        sys.exit(1)

    filename = sys.argv[1]

    analyze_csv(filename)