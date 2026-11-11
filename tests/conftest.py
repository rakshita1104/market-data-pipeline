import duckdb
import pytest
from processing.db import init_db


@pytest.fixture
def con():
    """A fresh, empty in-memory database for every test."""
    connection = duckdb.connect(":memory:")
    init_db(connection)
    yield connection
    connection.close()


BASE_TS = 1793949480000  # an exact minute boundary, in milliseconds


def make_trades(start_id, prices, qty="0.01", base_ts=BASE_TS):
    """Build fake Binance-style trades with consecutive IDs."""
    return [
        {"a": start_id + i, "p": p, "q": qty, "f": start_id + i, "l": start_id + i,
         "T": base_ts + i * 1000, "m": False, "M": True}
        for i, p in enumerate(prices)
    ]