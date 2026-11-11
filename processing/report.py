from processing.db import get_connection

con = get_connection()

def show(title, query):
    print(f"\n{title}")
    for row in con.execute(query).fetchall():
        print(row)

show("Last 5 pipeline runs (run, status, received, inserted, skipped, quarantined, published, secs):",
     "SELECT run_id, run_status, records_received, records_inserted, records_skipped, "
     "records_quarantined, records_published, duration_seconds "
     "FROM pipeline_runs ORDER BY start_time DESC LIMIT 5")

show("Quarantine by category:",
     "SELECT error_category, COUNT(*) FROM quarantine_records GROUP BY 1")

show("Row counts raw / silver / gold:",
     "SELECT (SELECT COUNT(*) FROM raw_trades), (SELECT COUNT(*) FROM silver_trades), "
     "(SELECT COUNT(*) FROM fact_market_trades)")

show("Latest 1-min metrics BTCUSDT (window, trades, open, high, low, close, qty):",
     "SELECT window_start, trade_count, open_price, high_price, low_price, close_price, total_quantity "
     "FROM fact_market_metrics WHERE symbol = 'BTCUSDT' ORDER BY window_start DESC LIMIT 3")

show("Reconciliation (metrics trade_count total vs fact_market_trades rows). Should match:",
     "SELECT (SELECT SUM(trade_count) FROM fact_market_metrics), "
     "(SELECT COUNT(*) FROM fact_market_trades)")

con.close()