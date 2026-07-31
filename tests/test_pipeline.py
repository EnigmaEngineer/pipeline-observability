"""The load has to survive being run twice.

A retry is the normal case. If load_raw appends, a retry doubles the partition, and the
volume monitor this whole project is building would then fire on damage the pipeline did
to itself. So rerun-safety is pinned here rather than trusted to the DELETE staying put.
"""

import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

import duckdb

from pipeline import generate, orders
from tests.tiny import Checks

START = date(2026, 1, 5)
DAY = date(2026, 1, 7)
NEXT = date(2026, 1, 8)


def build(tmp):
    raw = Path(tmp) / "raw"
    generate.write_day(raw, DAY, START)
    generate.write_day(raw, NEXT, START)
    con = duckdb.connect(":memory:")
    orders.create_tables(con)
    return con, raw


def run():
    c = Checks("test_pipeline")

    with tempfile.TemporaryDirectory() as tmp:
        con, raw = build(tmp)

        first = orders.load_raw(con, DAY, raw)
        orders.build_daily(con, DAY)
        c.ok(first > 0, f"the partition loaded, {first} rows")

        second = orders.load_raw(con, DAY, raw)
        orders.build_daily(con, DAY)
        c.eq(second, first, "loading the same partition twice does not double it")
        c.eq(con.execute("SELECT count(*) FROM raw_orders").fetchone()[0], first,
             "and nothing leaked into the rest of the table")
        c.eq(con.execute("SELECT count(*) FROM daily_orders").fetchone()[0], 1,
             "daily_orders keeps one row per date across reruns")

        orders.load_raw(con, NEXT, raw)
        orders.build_daily(con, NEXT)
        orders.load_raw(con, DAY, raw)
        orders.build_daily(con, DAY)
        c.eq(con.execute("SELECT count(*) FROM raw_orders WHERE dt = ?",
                         [NEXT]).fetchone()[0],
             con.execute("SELECT orders FROM daily_orders WHERE dt = ?",
                         [NEXT]).fetchone()[0],
             "rebuilding one partition leaves its neighbour alone")

        # the aggregate has to agree with the rows it came from, or every monitor
        # downstream is watching a number nobody can trace back.
        agg = con.execute(
            "SELECT orders, items, coupon_orders, cancelled FROM daily_orders "
            "WHERE dt = ?", [DAY]).fetchone()
        raw_side = con.execute(
            """
            SELECT count(*), sum(item_count), count(coupon_code),
                   count(*) FILTER (WHERE status = 'cancelled')
              FROM raw_orders WHERE dt = ?
            """, [DAY]).fetchone()
        c.eq(agg, raw_side, "daily_orders reconciles against raw_orders")

        c.ok(con.execute(
            "SELECT count(*) FROM raw_orders WHERE coupon_code IS NULL"
        ).fetchone()[0] > 0, "nulls survive the load rather than becoming empty strings")

        typed = dict(con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'raw_orders'").fetchall())
        c.eq(typed["order_amount_usd"], "DOUBLE", "declared types are not re-inferred")
        c.eq(typed["ordered_at"], "TIMESTAMP", "ordered_at loads as a timestamp")

        c.raises(FileNotFoundError,
                 lambda: orders.load_raw(con, date(2026, 3, 3), raw),
                 "a missing partition is an error, not an empty load")

        # a load that dies partway must not leave the partition deleted. without the
        # transaction the day reads as zero orders and the volume monitor fires on
        # damage the loader did.
        before = con.execute("SELECT count(*) FROM raw_orders WHERE dt = ?",
                             [DAY]).fetchone()[0]
        broken = Path(tmp) / "raw" / f"dt={DAY.isoformat()}" / "orders.jsonl"
        good = broken.read_text()
        broken.write_text(good.splitlines()[0] + "\n{not json at all\n")
        try:
            orders.load_raw(con, DAY, raw)
        except Exception:
            pass
        broken.write_text(good)
        c.eq(con.execute("SELECT count(*) FROM raw_orders WHERE dt = ?",
                         [DAY]).fetchone()[0], before,
             "a failed load rolls back and leaves the old partition in place")

        stamped = datetime(2026, 7, 31, 9, 0, 0)
        orders.load_raw(con, DAY, raw, now=stamped)
        c.eq(con.execute("SELECT DISTINCT loaded_at FROM raw_orders WHERE dt = ?",
                         [DAY]).fetchone()[0], stamped,
             "loaded_at is set from the caller so a run can stamp its own clock")

    return c


if __name__ == "__main__":
    sys.exit(run().report())
