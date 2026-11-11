from datetime import datetime, timezone
from processing.db import get_connection, init_db

now = datetime.now(timezone.utc).replace(tzinfo=None)
T = 1793949499232

bad_rows = [
    ("BTCUSDT", 1, "0.00000000", "0.01000000", T,    False, "{}", "test_batch", now),  # price zero
    ("BTCUSDT", 2, "abc",        "0.01000000", T,    False, "{}", "test_batch", now),  # price not a number
    ("BTCUSDT", 3, "68000.50",   "-1",         T,    False, "{}", "test_batch", now),  # negative quantity
    ("BTCUSDT", 4, "68000.50",   "0.01000000", None, False, "{}", "test_batch", now),  # missing timestamp
]

con = get_connection()
init_db(con)
con.executemany("INSERT OR IGNORE INTO raw_trades VALUES (?,?,?,?,?,?,?,?,?)", bad_rows)
con.close()
print("Injected 4 bad rows")