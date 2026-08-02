"""Run the orders pipeline with the metadata collector wrapped around it.

Why this is a script and not a flag on `pipeline/run.py`. The pipeline must not import
the thing watching it. Once `pipeline/orders.py` knows about `obs`, the dependency points
the wrong way and the watcher stops being something you could put around a pipeline you
did not write. So the two packages never import each other and this driver imports both.
That is also how it works in a real deployment, where the collector is a scheduler
listener rather than lines inside the task.

The collection happens outside the tracked block on purpose. Inside it, every profile
query would land in the duration of the run it was profiling, and the duration baseline for
run duration would be learning the cost of its own observability. That is not a rounding
error here. Recorded median duration is 11 ms for load_raw and 7 ms for build_daily,
against roughly 38 ms to collect each dataset. Folding it in would have made an 11 ms task
report 49 ms and left the baseline measuring mostly itself.

    python scripts/run_observed.py --start 2026-03-01 --end 2026-03-14
"""

import argparse
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from obs import collect, store  # noqa: E402
from obs.tracker import track  # noqa: E402
from pipeline import orders  # noqa: E402

PIPELINE = "orders"


def code_version():
    """Which commit ran. An incident timeline that cannot say what code produced a
    partition is missing the first question anyone asks."""
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or None
    except Exception:
        return None


def dates(start, end):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def observe_day(con, obs_con, day, raw_root, version):
    partition = f"dt={day.isoformat()}"

    with track(obs_con, PIPELINE, "load_raw", partition, code_version=version) as run:
        rows = orders.load_raw(con, day, raw_root)
    src = orders.partition_path(raw_root, day)
    collect.collect_into(
        obs_con, con, run.run_id, "raw_orders",
        where="dt = ?", params=[day],
        event_time_column="ordered_at",
        # bytes of the file that produced this partition, not bytes of the partition
        # inside duckdb. The two are different numbers and the useful one is this. A
        # source file that arrives at a fifth of its usual size is an incident, and the
        # storage a columnar engine ends up using for it is not.
        byte_size=src.stat().st_size,
    )

    with track(obs_con, PIPELINE, "build_daily", partition, code_version=version) as run:
        summary = orders.build_daily(con, day)
    # daily_orders has no byte_size. It is one row inside a table, and a per row size
    # would be an invented number. The column stays null rather than getting filled with
    # something that looks like a measurement.
    collect.collect_into(
        obs_con, con, run.run_id, "daily_orders",
        where="dt = ?", params=[day],
    )
    return rows, summary


def main():
    ap = argparse.ArgumentParser(description="run the pipeline and collect run metadata")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end")
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--db", default="warehouse/orders.duckdb")
    ap.add_argument("--obs-db", default="warehouse/obs.duckdb")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else start

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    Path(args.obs_db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db)
    orders.create_tables(con)
    # a second file, not a second schema in the same one. the collector holding a
    # connection it could write the pipeline's tables through would make the separation
    # a convention instead of a fact.
    obs_con = store.connect(args.obs_db)

    version = code_version()
    total = 0
    began = time.perf_counter()
    for day in dates(start, end):
        t0 = time.perf_counter()
        rows, summary = observe_day(con, obs_con, day, args.raw, version)
        total += rows
        if not args.quiet:
            print(f"{day}  rows={rows:>6}  gross={summary[1]:>12,.2f}  "
                  f"{(time.perf_counter() - t0) * 1000:6.0f} ms")

    wall = time.perf_counter() - began
    runs = obs_con.execute("SELECT count(*) FROM obs_run").fetchone()[0]
    dm = obs_con.execute("SELECT count(*) FROM obs_dataset_metric").fetchone()[0]
    cm = obs_con.execute("SELECT count(*) FROM obs_column_metric").fetchone()[0]
    sv = obs_con.execute("SELECT count(*) FROM obs_schema_version").fetchone()[0]
    print(f"\n{total} rows over {(end - start).days + 1} partitions in {wall:.1f}s")
    print(f"metadata now holds {runs} runs, {sv} schema versions, "
          f"{dm} dataset metrics, {cm} column metrics")
    con.close()
    obs_con.close()


if __name__ == "__main__":
    main()
