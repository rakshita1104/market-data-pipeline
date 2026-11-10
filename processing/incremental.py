import json
from datetime import datetime, timezone

from ingestion.rest_client import fetch_agg_trades
from ingestion.bronze_writer import new_batch_id, write_batch
from processing.db import get_connection, init_db

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
PAGE_SIZE = 1000


def get_checkpoint(con, symbol):
    row = con.execute(
        "SELECT last_agg_trade_id FROM checkpoints WHERE symbol = ?", [symbol]
    ).fetchone()
    return row[0] if row else None


def load_page(con, symbol, trades, batch_id):
    """Insert one page of trades and move the checkpoint, all-or-nothing."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        (symbol, t["a"], t["p"], t["q"], t["T"], t["m"], json.dumps(t), batch_id, now)
        for t in trades
    ]
    new_last_id = max(t["a"] for t in trades)

    before = con.execute(
        "SELECT COUNT(*) FROM raw_trades WHERE symbol = ?", [symbol]
    ).fetchone()[0]

    con.execute("BEGIN")
    try:
        con.executemany(
            "INSERT OR IGNORE INTO raw_trades VALUES (?,?,?,?,?,?,?,?,?)", rows
        )
        con.execute(
            "INSERT OR REPLACE INTO checkpoints VALUES (?, ?, ?)",
            [symbol, new_last_id, now],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    after = con.execute(
        "SELECT COUNT(*) FROM raw_trades WHERE symbol = ?", [symbol]
    ).fetchone()[0]
    inserted = after - before
    return inserted, len(trades) - inserted


def load_symbol(con, symbol):
    checkpoint = get_checkpoint(con, symbol)
    from_id = checkpoint + 1 if checkpoint is not None else None
    mode = "incremental" if checkpoint is not None else "initial"

    total_received = total_inserted = total_skipped = 0

    while True:
        trades = fetch_agg_trades(symbol, from_id=from_id, limit=PAGE_SIZE)
        if not trades:
            break

        batch_id = new_batch_id()
        write_batch(symbol, trades, batch_id)          # 1. Bronze file first
        inserted, skipped = load_page(con, symbol, trades, batch_id)  # 2. DB + checkpoint

        total_received += len(trades)
        total_inserted += inserted
        total_skipped += skipped

        if len(trades) < PAGE_SIZE:   # last page, nothing more to fetch
            break
        from_id = max(t["a"] for t in trades) + 1

    print(f"{symbol} [{mode}] received={total_received} "
          f"inserted={total_inserted} skipped={total_skipped}")


def main():
    con = get_connection()
    try:
        init_db(con)
        for symbol in SYMBOLS:
            load_symbol(con, symbol)
    finally:
        con.close()


if __name__ == "__main__":
    main()