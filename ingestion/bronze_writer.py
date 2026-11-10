import json
import uuid

from datetime import datetime, timezone
from pathlib import Path


BRONZE_ROOT = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "bronze"
    / "trades"
)

def new_batch_id():

    return (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        + "_"
        + uuid.uuid4().hex[:6]
    )


def write_batch(symbol, trades, batch_id):

    now = datetime.now(timezone.utc)

    folder = (
        BRONZE_ROOT
        / f"year={now:%Y}"
        / f"month={now:%m}"
        / f"day={now:%d}"
        / symbol.lower()
    )

    folder.mkdir(parents=True, exist_ok=True)

    path = folder / f"batch_{batch_id}.jsonl"

    with open(path, "x", encoding="utf-8") as f:

        for trade in trades:

            record = {
                "source": "binance_spot_rest",
                "symbol": symbol.upper(),
                "batch_id": batch_id,
                "ingestion_ts": now.isoformat(),
                "payload": trade
            }

            f.write(json.dumps(record) + "\n")

    return path