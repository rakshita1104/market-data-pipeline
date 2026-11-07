import requests, json

BASE = "https://api.binance.com"

r = requests.get(
    f"{BASE}/api/v3/aggTrades",
    params={"symbol": "BTCUSDT", "limit": 5},
    timeout=10,
)
r.raise_for_status()
print(json.dumps(r.json(), indent=2))