import requests

BASE_URL = "https://api.binance.com"


def fetch_agg_trades(symbol, from_id=None, limit=1000):

    params = {
        "symbol": symbol.upper(),
        "limit": limit
    }

    if from_id is not None:
        params["fromId"] = from_id

    response = requests.get(
        f"{BASE_URL}/api/v3/aggTrades",
        params=params,
        timeout=10
    )

    response.raise_for_status()

    return response.json()