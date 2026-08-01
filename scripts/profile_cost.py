"""Measure the two choices baked into obs/collect.py.

One. Is the single pass actually worth it, against the obvious loop of one query per
column? The single pass is harder to read and it only earns that if it wins.

Two. Should distinct counts be exact or approximate? Speed is one half of the answer and
the error is the other, because a drift check cannot tell an estimator wobble from a real
change.

    python scripts/profile_cost.py --db warehouse/orders.duckdb --table raw_orders

Every timing here is a median of three, after a warm up query. The first query against a
table pays for opening it and that cost belongs to nobody.
"""

import argparse
import statistics
import sys
from pathlib import Path
from time import perf_counter

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from obs import collect, store  # noqa: E402

REPS = 3


def timed(fn, reps=REPS):
    fn()  # warm up, not counted
    samples = []
    for _ in range(reps):
        t0 = perf_counter()
        result = fn()
        samples.append((perf_counter() - t0) * 1000)
    return statistics.median(samples), result


def single_pass(con, table, columns, exact):
    sql = collect.profile_sql(table, columns, exact_distinct=exact)
    return lambda: con.execute(sql).fetchone()


def per_column(con, table, columns, exact):
    """One query for the row count and one per column, which is the shape you get if you
    write the collector column first.

    The expressions come from `collect.column_aggs` one column at a time, so the two
    versions compute exactly the same things. The first draft of this hand wrote a
    shorter aggregate list that left the quantiles out, and it won by 1.8x on work it was
    not doing.
    """
    def go():
        out = [con.execute(f"SELECT count(*) FROM {collect.quote(table)}").fetchone()[0]]
        for column in columns:
            aggs = collect.column_aggs([column], exact)
            out.append(con.execute(
                f"SELECT {', '.join(aggs)} FROM {collect.quote(table)}").fetchone())
        return out

    return go


def no_quantiles(con, table, columns, exact):
    """The single pass with the quantiles taken out, to find where its time goes."""
    plain = [(n, "VARCHAR") for n, _ in columns]
    sql = collect.profile_sql(table, plain, exact_distinct=exact)
    return lambda: con.execute(sql).fetchone()


def main():
    ap = argparse.ArgumentParser(description="cost of the collector's two choices")
    ap.add_argument("--db", default="warehouse/orders.duckdb")
    ap.add_argument("--table", default="raw_orders")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    columns = store.columns_of(con, args.table)
    if not columns:
        raise SystemExit(f"no table {args.table} in {args.db}")
    rows = con.execute(f"SELECT count(*) FROM {collect.quote(args.table)}").fetchone()[0]
    print(f"{args.table}: {rows:,} rows, {len(columns)} columns, duckdb {duckdb.__version__}")

    one, _ = timed(single_pass(con, args.table, columns, True))
    many, _ = timed(per_column(con, args.table, columns, True))
    flat, _ = timed(no_quantiles(con, args.table, columns, True))
    print(f"\nexact distinct, same aggregates both ways")
    print(f"  single pass     1 query    {one:8.1f} ms")
    print(f"  per column     {len(columns) + 1:2d} queries   {many:8.1f} ms"
          f"   ({many / one:.2f}x the single pass)")
    print(f"  single pass, no quantiles  {flat:8.1f} ms"
          f"   (quantiles are {one - flat:.1f} ms of the {one:.1f})")

    approx_one, _ = timed(single_pass(con, args.table, columns, False))
    print(f"\napprox distinct")
    print(f"  single pass     1 query    {approx_one:8.1f} ms"
          f"   ({one / approx_one:.2f}x faster than exact)")

    # the half of the question that speed does not answer
    exact_row = con.execute(
        collect.profile_sql(args.table, columns, exact_distinct=True)).fetchone()
    approx_row = con.execute(
        collect.profile_sql(args.table, columns, exact_distinct=False)).fetchone()
    print(f"\ndistinct count error, approximate against exact")
    worst = 0.0
    for i, (name, _dtype) in enumerate(columns):
        base = 1 + i * collect.AGGS_PER_COLUMN + 1
        e, a = exact_row[base], approx_row[base]
        err = 0.0 if e == 0 else abs(a - e) / e * 100
        worst = max(worst, err)
        print(f"  {name:<18} exact {e:>8,}   approx {a:>8,}   {err:5.2f}%")
    print(f"\nworst column error {worst:.2f}%")
    con.close()


if __name__ == "__main__":
    main()
