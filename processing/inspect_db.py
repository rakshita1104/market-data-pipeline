from processing.db import get_connection

con = get_connection()

print("\nRows per symbol:")
print(con.execute(
    "SELECT symbol, COUNT(*) AS n, MIN(agg_trade_id) AS min_id, MAX(agg_trade_id) AS max_id "
    "FROM raw_trades GROUP BY symbol"
).fetchall())

print("\nDuplicate check (should be empty):")
print(con.execute(
    "SELECT symbol, agg_trade_id, COUNT(*) FROM raw_trades "
    "GROUP BY symbol, agg_trade_id HAVING COUNT(*) > 1"
).fetchall())

print("\nCheckpoints:")
print(con.execute("SELECT * FROM checkpoints").fetchall())

con.close()