import time
import uuid
from datetime import datetime, timezone

from processing.db import get_connection, init_db
from processing.incremental import load_symbol, SYMBOLS
from processing.silver import run_silver
from processing.gold import run_gold


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def record_run(con, run_id, start, t0, status, stats, error):
    con.execute(
        "INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            run_id, "market_pipeline", start, utcnow(), round(time.time() - t0, 2),
            status, stats["received"], stats["inserted"], stats["skipped"],
            stats["quarantined"], stats["published"], 0, error,
        ],
    )


def run_pipeline():
    con = get_connection()
    init_db(con)

    run_id = uuid.uuid4().hex[:12]
    start, t0 = utcnow(), time.time()
    stats = {"received": 0, "inserted": 0, "skipped": 0, "quarantined": 0, "published": 0}
    status, error = "SUCCESS", None

    try:
        # 1. Ingest: API -> Bronze files -> raw_trades (+ checkpoints)
        for symbol in SYMBOLS:
            received, inserted, skipped = load_symbol(con, symbol)
            stats["received"] += received
            stats["inserted"] += inserted
            stats["skipped"] += skipped

        # 2. Silver: clean + quality rules + quarantine
        valid, invalid = run_silver(con)
        stats["quarantined"] = invalid

        # 3. Gold: publish validated data + metrics
        published, _ = run_gold(con)
        stats["published"] = published

    except Exception as e:
        status = "FAILED"
        error = f"{type(e).__name__}: {e}"
        print(f"PIPELINE FAILED: {error}")

    finally:
        record_run(con, run_id, start, t0, status, stats, error)
        con.close()

    print(f"Run {run_id}: {status} {stats}")
    return status


if __name__ == "__main__":
    run_pipeline()