import sys
import time
import uuid

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import task_run

from processing.db import get_connection, init_db
from processing.incremental import load_symbol, SYMBOLS
from processing.silver import run_silver
from processing.gold import run_gold
from processing.pipeline import record_run, utcnow


@task(retries=3, retry_delay_seconds=[5, 20, 60], cache_policy=NO_CACHE)
def ingest_task(symbol):
    con = get_connection()
    try:
        init_db(con)
        received, inserted, skipped = load_symbol(con, symbol)
    finally:
        con.close()
    return received, inserted, skipped, task_run.run_count - 1   # retries used


@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def silver_task():
    con = get_connection()
    try:
        return run_silver(con)
    finally:
        con.close()


@task(retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def gold_task():
    con = get_connection()
    try:
        return run_gold(con)
    finally:
        con.close()


@flow(name="market_pipeline")
def market_pipeline():
    run_id = uuid.uuid4().hex[:12]
    start, t0 = utcnow(), time.time()
    stats = {"received": 0, "inserted": 0, "skipped": 0, "quarantined": 0, "published": 0}
    retries = 0
    status, error = "RUNNING", None

    try:
        for symbol in SYMBOLS:
            received, inserted, skipped, used = ingest_task(symbol)
            stats["received"] += received
            stats["inserted"] += inserted
            stats["skipped"] += skipped
            retries += used

        valid, invalid = silver_task()
        stats["quarantined"] = invalid

        published, _ = gold_task()
        stats["published"] = published
        status = "SUCCESS"

    except Exception as e:
        status = "FAILED"
        error = f"{type(e).__name__}: {e}"

    finally:
        if status == "RUNNING":
            status = "INTERRUPTED"
        con = get_connection()
        init_db(con)
        record_run(con, run_id, start, t0, status, stats, error, retries)
        con.close()

    print(f"Run {run_id}: {status} {stats} retries={retries}")
    if status == "FAILED":
        raise RuntimeError(error)   # so Prefect also marks the flow run as Failed


if __name__ == "__main__":
    if "--serve" in sys.argv:
        market_pipeline.serve(name="market-pipeline-every-5-min", interval=300)
    else:
        market_pipeline()