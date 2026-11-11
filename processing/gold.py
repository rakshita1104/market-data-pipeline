from processing.db import get_connection, init_db


def run_gold(con):
    con.execute("BEGIN")
    try:
        # 1. Silver trades that are not in Gold yet (incremental again)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE new_trades AS
            SELECT s.*
            FROM silver_trades s
            LEFT JOIN fact_market_trades f
                   ON f.symbol = s.symbol AND f.agg_trade_id = s.agg_trade_id
            WHERE f.agg_trade_id IS NULL
        """)

        # 2. Publish them
        con.execute("""
            INSERT INTO fact_market_trades
            SELECT symbol, agg_trade_id, price, quantity,
                   CAST(CAST(price AS DECIMAL(19,8)) * CAST(quantity AS DECIMAL(19,8)) AS DECIMAL(28,8)),
                   trade_ts, ingestion_ts
            FROM new_trades
        """)

        # 3. Which 1-minute windows did these new trades touch?
        con.execute("""
            CREATE OR REPLACE TEMP TABLE affected AS
            SELECT DISTINCT symbol, date_trunc('minute', trade_ts) AS window_start
            FROM new_trades
        """)

        # 4. Delete those windows' old summaries...
        con.execute("""
            DELETE FROM fact_market_metrics m
            WHERE EXISTS (
                SELECT 1 FROM affected a
                WHERE a.symbol = m.symbol AND a.window_start = m.window_start
            )
        """)

        # 5. ...and recompute them from the full trade table
        con.execute("""
            INSERT INTO fact_market_metrics
            SELECT f.symbol,
                   date_trunc('minute', f.trade_ts) AS window_start,
                   COUNT(*),
                   arg_min(f.price, f.agg_trade_id),
                   MAX(f.price),
                   MIN(f.price),
                   arg_max(f.price, f.agg_trade_id),
                   CAST(SUM(f.quantity) AS DECIMAL(28,8)),
                   CAST(SUM(f.trade_value) AS DECIMAL(28,8))
            FROM fact_market_trades f
            JOIN affected a
              ON a.symbol = f.symbol
             AND a.window_start = date_trunc('minute', f.trade_ts)
            GROUP BY f.symbol, date_trunc('minute', f.trade_ts)
        """)

        published = con.execute("SELECT COUNT(*) FROM new_trades").fetchone()[0]
        windows = con.execute("SELECT COUNT(*) FROM affected").fetchone()[0]
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    print(f"Gold: published={published} windows_recomputed={windows}")
    return published, windows


def main():
    con = get_connection()
    try:
        init_db(con)
        run_gold(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()