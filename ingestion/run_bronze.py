from ingestion.rest_client import fetch_agg_trades

from ingestion.bronze_writer import (
    new_batch_id,
    write_batch
)


SYMBOLS = ["BTCUSDT", "ETHUSDT"]


def main():

    for symbol in SYMBOLS:

        batch_id = new_batch_id()

        trades = fetch_agg_trades(
            symbol,
            limit=1000
        )

        if not trades:

            print(f"{symbol}: no trades returned")
            continue

        path = write_batch(
            symbol,
            trades,
            batch_id
        )

        print(
            f"{symbol}: saved {len(trades)} trades -> {path}"
        )


if __name__ == "__main__":

    main()