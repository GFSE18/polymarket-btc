import requests
import json


TOKEN_ID = "58349957919154596092441152547485267647573480809590643433972117272263278122069"

GAMMA_API = "https://gamma-api.polymarket.com"


def parse_json_field(value):

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        return json.loads(value)

    return []


def resolve_token(token_id):

    r = requests.get(
        f"{GAMMA_API}/markets",
        params={
            "clob_token_ids": token_id
        },
        timeout=5,
    )

    r.raise_for_status()

    markets = r.json()

    if not markets:
        return None

    market = markets[0]

    token_ids = parse_json_field(
        market.get("clobTokenIds")
    )

    outcomes = parse_json_field(
        market.get("outcomes")
    )

    token_ids = [
        str(x)
        for x in token_ids
    ]

    token_id = str(token_id)

    if token_id not in token_ids:
        return None

    index = token_ids.index(
        token_id
    )

    outcome = (
        outcomes[index]
        if index < len(outcomes)
        else "UNKNOWN"
    )

    crypto_config = (
        market.get("cryptoMarketConfig")
        or {}
    )

    events = (
        market.get("events")
        or []
    )

    price_to_beat = None

    if events:

        event_metadata = (
            events[0].get("eventMetadata")
            or {}
        )

        price_to_beat = (
            event_metadata.get(
                "priceToBeat"
            )
        )

    return {
        "title":
            market.get("question"),

        "outcome":
            outcome,

        "slug":
            market.get("slug"),

        "condition_id":
            market.get("conditionId"),

        "asset":
            crypto_config.get("asset"),

        "duration":
            crypto_config.get("duration"),

        "price_to_beat":
            price_to_beat,

        "all_token_ids":
            token_ids,

        "all_outcomes":
            outcomes,
    }


info = resolve_token(
    TOKEN_ID
)

if info is None:

    print(
        "TOKEN NOT FOUND"
    )

else:

    print()
    print("=" * 100)

    print(
        "MARKET:       ",
        info["title"]
    )

    print(
        "OUTCOME:      ",
        info["outcome"]
    )

    print(
        "ASSET:        ",
        info["asset"]
    )

    print(
        "DURATION:     ",
        info["duration"]
    )

    print(
        "PRICE TO BEAT:",
        info["price_to_beat"]
    )

    print(
        "SLUG:         ",
        info["slug"]
    )

    print(
        "CONDITION ID: ",
        info["condition_id"]
    )

    print("=" * 100)