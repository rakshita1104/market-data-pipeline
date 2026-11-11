from datetime import datetime, timezone
from processing.db import get_connection, init_db


def run_silver(con):
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    con.execute("BEGIN")
    try:
        # 1. Pick only raw rows not yet handled, and TRY_CAST the text to numbers.
        #    TRY_CAST gives NULL for junk like 'abc' instead of crashing.
        con.execute("""
            CREATE OR REPLACE TEMP TABLE stage AS
            SELECT
                r.symbol, r.agg_trade_id, r.raw_payload, r.batch_id,
                r.ingestion_ts, r.is_buyer_maker, r.trade_ts_ms,
                r.price AS raw_price, r.quantity AS raw_quantity,
                TRY_CAST(r.price AS DECIMAL(18,8))    AS price,
                TRY_CAST(r.quantity AS DECIMAL(18,8)) AS quantity
            FROM raw_trades r
            LEFT JOIN silver_trades s
                   ON s.symbol = r.symbol AND s.agg_trade_id = r.agg_trade_id
            LEFT JOIN quarantine_records q
                   ON q.symbol = r.symbol AND q.agg_trade_id = r.agg_trade_id
            WHERE s.agg_trade_id IS NULL AND q.agg_trade_id IS NULL
        """)

        # 2. Quality rules: first failing rule gives the error category.
        con.execute("""
            CREATE OR REPLACE TEMP TABLE checked AS
            SELECT *,
                CASE
                    WHEN symbol IS NULL OR NOT regexp_matches(symbol, '^[A-Z0-9]{5,20}$')
                        THEN 'INVALID_SYMBOL'
                    WHEN price IS NULL OR price <= 0
                        THEN 'INVALID_PRICE'
                    WHEN quantity IS NULL OR quantity <= 0
                        THEN 'INVALID_QUANTITY'
                    WHEN trade_ts_ms IS NULL OR trade_ts_ms <= 0
                        THEN 'INVALID_TIMESTAMP'
                END AS error_category
            FROM stage
        """)

        # 3. Good rows -> silver
        con.execute("""
            INSERT INTO silver_trades
            SELECT symbol, agg_trade_id, price, quantity,
                   epoch_ms(trade_ts_ms), is_buyer_maker, batch_id, ingestion_ts
            FROM checked
            WHERE error_category IS NULL
        """)

        # 4. Bad rows -> quarantine, with a readable reason
        con.execute("""
            INSERT INTO quarantine_records
            SELECT symbol, agg_trade_id, raw_payload, error_category,
                CASE error_category
                    WHEN 'INVALID_SYMBOL'    THEN 'symbol has unexpected format: ' || COALESCE(symbol, 'NULL')
                    WHEN 'INVALID_PRICE'     THEN 'price is not a positive number: ' || COALESCE(raw_price, 'NULL')
                    WHEN 'INVALID_QUANTITY'  THEN 'quantity is not a positive number: ' || COALESCE(raw_quantity, 'NULL')
                    WHEN 'INVALID_TIMESTAMP' THEN 'trade timestamp is missing or invalid'
                END,
                batch_id, ?, 'PENDING', 0
            FROM checked
            WHERE error_category IS NOT NULL
        """, [now])

        valid, invalid = con.execute(
            "SELECT COUNT(*) FILTER (WHERE error_category IS NULL), "
            "COUNT(*) FILTER (WHERE error_category IS NOT NULL) FROM checked"
        ).fetchone()
        by_cat = con.execute(
            "SELECT error_category, COUNT(*) FROM checked "
            "WHERE error_category IS NOT NULL GROUP BY 1"
        ).fetchall()

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    print(f"Silver: processed={valid + invalid} valid={valid} quarantined={invalid} {by_cat}")
    return valid, invalid


def main():
    con = get_connection()
    try:
        init_db(con)
        run_silver(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()