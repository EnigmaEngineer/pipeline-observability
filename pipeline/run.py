"""Driver for the orders pipeline.

Runs one date or a range, one partition at a time, and prints a per-day line. Nothing
here writes to the metadata tables yet. That is day 2, and the reason it is not here is
that a collector guessing at row counts before it has been written is worse than an
honest gap.
"""

import argparse
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb

from . import orders


def dates(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def main():
    ap = argparse.ArgumentParser(description="run the orders pipeline over a date range")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end")
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--db", default="warehouse/orders.duckdb")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else start

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db)
    orders.create_tables(con)

    total_rows = 0
    started = time.perf_counter()
    for day in dates(start, end):
        t0 = time.perf_counter()
        rows = orders.load_raw(con, day, args.raw)
        summary = orders.build_daily(con, day)
        took = (time.perf_counter() - t0) * 1000
        total_rows += rows
        if not args.quiet:
            print(f"{day}  rows={rows:>6}  gross={summary[1]:>12,.2f}  {took:6.0f} ms")

    wall = time.perf_counter() - started
    days = (end - start).days + 1
    print(f"\n{total_rows} rows over {days} partitions in {wall:.1f}s "
          f"({total_rows / wall:,.0f} rows/s)")
    con.close()


if __name__ == "__main__":
    main()
