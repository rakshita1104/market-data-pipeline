import duckdb
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "warehouse.duckdb"

def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))

def init_db(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_trades (
            symbol          VARCHAR,
            agg_trade_id    BIGINT,
            price           VARCHAR,
            quantity        VARCHAR,
            trade_ts_ms     BIGINT,
            is_buyer_maker  BOOLEAN,
            raw_payload     VARCHAR,
            batch_id        VARCHAR,
            ingestion_ts    TIMESTAMP,
            PRIMARY KEY (symbol, agg_trade_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS checkpoints (
            symbol             VARCHAR PRIMARY KEY,
            last_agg_trade_id  BIGINT,
            updated_at         TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_trades (
            symbol          VARCHAR,
            agg_trade_id    BIGINT,
            price           DECIMAL(18,8),
            quantity        DECIMAL(18,8),
            trade_ts        TIMESTAMP,
            is_buyer_maker  BOOLEAN,
            batch_id        VARCHAR,
            ingestion_ts    TIMESTAMP,
            PRIMARY KEY (symbol, agg_trade_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_records (
            symbol             VARCHAR,
            agg_trade_id       BIGINT,
            raw_payload        VARCHAR,
            error_category     VARCHAR,
            error_message      VARCHAR,
            batch_id           VARCHAR,
            detected_at        TIMESTAMP,
            reprocess_status   VARCHAR,
            retry_count        INTEGER,
            PRIMARY KEY (symbol, agg_trade_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_market_trades (
            symbol         VARCHAR,
            agg_trade_id   BIGINT,
            price          DECIMAL(18,8),
            quantity       DECIMAL(18,8),
            trade_value    DECIMAL(28,8),
            trade_ts       TIMESTAMP,
            ingestion_ts   TIMESTAMP,
            PRIMARY KEY (symbol, agg_trade_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_market_metrics (
            symbol          VARCHAR,
            window_start    TIMESTAMP,
            trade_count     BIGINT,
            open_price      DECIMAL(18,8),
            high_price      DECIMAL(18,8),
            low_price       DECIMAL(18,8),
            close_price     DECIMAL(18,8),
            total_quantity  DECIMAL(28,8),
            total_value     DECIMAL(28,8),
            PRIMARY KEY (symbol, window_start)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id               VARCHAR,
            flow_name            VARCHAR,
            start_time           TIMESTAMP,
            end_time             TIMESTAMP,
            duration_seconds     DOUBLE,
            run_status           VARCHAR,
            records_received     BIGINT,
            records_inserted     BIGINT,
            records_skipped      BIGINT,
            records_quarantined  BIGINT,
            records_published    BIGINT,
            retry_count          INTEGER,
            error_message        VARCHAR
        )
    """)