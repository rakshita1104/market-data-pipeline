import pytest
from processing.incremental import load_page, get_checkpoint
from processing.silver import run_silver
from processing.gold import run_gold
from tests.conftest import make_trades


def count(con, table, where=""):
    return con.execute(f"SELECT COUNT(*) FROM {table} {where}").fetchone()[0]


# ---------- incremental loading + idempotency ----------

def test_no_checkpoint_at_start(con):
    assert get_checkpoint(con, "BTCUSDT") is None


def test_load_inserts_rows_and_moves_checkpoint(con):
    trades = make_trades(100, ["68000"] * 5)
    inserted, skipped = load_page(con, "BTCUSDT", trades, "b1")
    assert (inserted, skipped) == (5, 0)
    assert get_checkpoint(con, "BTCUSDT") == 104


def test_reloading_same_data_creates_no_duplicates(con):
    trades = make_trades(100, ["68000"] * 5)
    load_page(con, "BTCUSDT", trades, "b1")
    inserted, skipped = load_page(con, "BTCUSDT", trades, "b2")   # replay
    assert (inserted, skipped) == (0, 5)
    assert count(con, "raw_trades") == 5


def test_only_new_trades_are_inserted_when_pages_overlap(con):
    load_page(con, "BTCUSDT", make_trades(1, ["1"] * 5), "b1")      # ids 1..5
    inserted, skipped = load_page(con, "BTCUSDT", make_trades(4, ["1"] * 5), "b2")  # ids 4..8
    assert (inserted, skipped) == (3, 2)


def test_same_id_for_different_symbols_is_allowed(con):
    load_page(con, "BTCUSDT", make_trades(1, ["68000"]), "b1")
    load_page(con, "ETHUSDT", make_trades(1, ["3500"]), "b2")
    assert count(con, "raw_trades") == 2


def test_checkpoint_does_not_move_when_insert_fails(con):
    bad = make_trades(1, ["68000"])
    bad[0]["a"] = "not-a-number"          # cannot be stored in a BIGINT column
    with pytest.raises(Exception):
        load_page(con, "BTCUSDT", bad, "b1")
    assert get_checkpoint(con, "BTCUSDT") is None
    assert count(con, "raw_trades") == 0


# ---------- silver: data quality + quarantine ----------

def load_mixed_batch(con):
    trades = make_trades(1, ["68000.50", "0", "abc", "68001", "68002"])
    trades[3]["q"] = "-1"       # negative quantity
    trades[4]["T"] = None       # missing timestamp
    load_page(con, "BTCUSDT", trades, "b1")
    # ids: 1 valid, 2 zero price, 3 text price, 4 negative qty, 5 missing timestamp


def test_silver_separates_valid_and_invalid_rows(con):
    load_mixed_batch(con)
    valid, invalid = run_silver(con)
    assert (valid, invalid) == (1, 4)
    assert count(con, "silver_trades") == 1
    assert count(con, "quarantine_records") == 4


def test_quarantine_stores_the_reason(con):
    load_mixed_batch(con)
    run_silver(con)
    cats = dict(con.execute(
        "SELECT error_category, COUNT(*) FROM quarantine_records GROUP BY 1"
    ).fetchall())
    assert cats == {"INVALID_PRICE": 2, "INVALID_QUANTITY": 1, "INVALID_TIMESTAMP": 1}


def test_silver_second_run_does_nothing(con):
    load_mixed_batch(con)
    run_silver(con)
    assert run_silver(con) == (0, 0)


# ---------- gold: publishing + metrics ----------

def test_gold_only_publishes_valid_rows_and_is_idempotent(con):
    load_mixed_batch(con)
    run_silver(con)
    published, _ = run_gold(con)
    assert published == 1
    assert count(con, "fact_market_trades") == 1
    assert run_gold(con) == (0, 0)          # running again publishes nothing


def test_minute_metrics_open_high_low_close(con):
    # all within one minute, ids increasing: 100, 102, 99, 101
    load_page(con, "BTCUSDT", make_trades(1, ["100", "102", "99", "101"]), "b1")
    run_silver(con)
    run_gold(con)
    row = con.execute(
        "SELECT trade_count, open_price, high_price, low_price, close_price "
        "FROM fact_market_metrics"
    ).fetchone()
    assert row == (4, 100, 102, 99, 101)


def test_late_trade_updates_an_already_published_minute(con):
    load_page(con, "BTCUSDT", make_trades(1, ["100", "101"]), "b1")
    run_silver(con); run_gold(con)
    load_page(con, "BTCUSDT", make_trades(3, ["105"]), "b2")   # same minute, arrives later
    run_silver(con); run_gold(con)
    trade_count, high = con.execute(
        "SELECT trade_count, high_price FROM fact_market_metrics"
    ).fetchone()
    assert (trade_count, high) == (3, 105)
    assert count(con, "fact_market_metrics") == 1          # rebuilt, not duplicated


def test_metrics_reconcile_with_trades(con):
    load_page(con, "BTCUSDT", make_trades(1, ["100"] * 50), "b1")
    run_silver(con); run_gold(con)
    total = con.execute("SELECT SUM(trade_count) FROM fact_market_metrics").fetchone()[0]
    assert total == count(con, "fact_market_trades") == 50