"""End to end check over the real CLI path.

`test_pipeline` already pins rerun safety at the function level. This exists because the
unit test calls `load_raw` directly and CI needs to know the command line path works too.
It generates two weeks and runs the range. Then it snapshots the counts, runs the same
range again and compares.

The first version of this was two `python -m pipeline.run` steps in the workflow with a
comment claiming they proved rerun safety. They proved nothing. Running a thing twice is
not checking it.

The second half runs the observed path and checks that the metadata agrees with the
warehouse it was watching. Every unit test for the collector points it at a table built
inside the test. This is the only place the numbers it records are checked against a
pipeline that ran for real.
"""

import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from obs import alerting, baseline, drift, history  # noqa: E402

START = "2026-03-02"
END = "2026-03-15"


def sh(*args):
    proc = subprocess.run([sys.executable, "-m", *args], capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(f"command failed: {' '.join(args)}")
    return proc.stdout


def counts(db):
    con = duckdb.connect(db, read_only=True)
    rows = con.execute(
        "SELECT dt, orders, gross_usd FROM daily_orders ORDER BY dt"
    ).fetchall()
    total = con.execute("SELECT count(*) FROM raw_orders").fetchone()[0]
    con.close()
    return total, rows


def observed(raw, tmp):
    """Run the observed path and hold the metadata to the warehouse.

    The invariant that matters: the row counts in obs_dataset_metric have to add up to
    the rows actually in raw_orders. A collector that quietly profiled the whole table
    instead of the partition, or profiled it before the load, passes every unit test and
    fails this.
    """
    db = str(Path(tmp) / "observed.duckdb")
    obs_db = str(Path(tmp) / "obs.duckdb")
    proc = subprocess.run(
        [sys.executable, "scripts/run_observed.py", "--start", START, "--end", END,
         "--raw", raw, "--db", db, "--obs-db", obs_db, "--quiet"],
        capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        return 1, "run_observed failed"

    days = (date.fromisoformat(END) - date.fromisoformat(START)).days + 1
    wh = duckdb.connect(db, read_only=True)
    loaded = wh.execute("SELECT count(*) FROM raw_orders").fetchone()[0]
    wh.close()

    obs = duckdb.connect(obs_db, read_only=True)
    runs = obs.execute("SELECT count(*) FROM obs_run WHERE status = 'success'").fetchone()[0]
    recorded = obs.execute(
        "SELECT sum(row_count) FROM obs_dataset_metric WHERE dataset = 'raw_orders'"
    ).fetchone()[0]
    unfinished = obs.execute(
        "SELECT count(*) FROM obs_run WHERE status = 'running'").fetchone()[0]
    columns = obs.execute("SELECT count(*) FROM obs_column_metric").fetchone()[0]
    obs.close()

    if recorded != loaded:
        return 1, f"metadata says {recorded} raw rows, the warehouse holds {loaded}"
    if runs != days * 2:
        return 1, f"{runs} successful runs recorded for {days} days of two tasks"
    if unfinished:
        return 1, f"{unfinished} runs left at status running after a clean finish"
    if columns != days * (11 + 8):
        return 1, f"{columns} column metrics, expected {days * (11 + 8)}"

    # Fourteen days is two observations per weekday. A baseline that produced seven bands
    # from that would be inventing confidence, so the minimum has to bite here. This is
    # the only place the model meets metadata a real pipeline wrote rather than a list
    # built inside a test.
    obs = duckdb.connect(obs_db, read_only=True)
    volume, skipped = history.volume_history(obs)
    obs.close()
    if len(volume) != days:
        return 1, f"volume history has {len(volume)} observations for {days} partitions"
    if skipped:
        return 1, f"{skipped} partition keys were unreadable"
    keyed = baseline.fit_bands(history.keyed(volume))
    if keyed:
        return 1, (f"{len(keyed)} weekday bands built from {days} days, which is "
                   f"{days / 7:.0f} observations each")
    pooled = baseline.fit_bands(history.unkeyed(volume))
    if None not in pooled or pooled[None].degenerate:
        return 1, "no usable pooled band from a clean fortnight"

    # The drift monitor against metadata a real pipeline wrote. The unit tests build the
    # column history by hand, so this is the only place the reader and the monitor meet
    # rows the collector actually produced. Two properties are checked rather than any
    # number, because a fortnight is too short for a fire rate to mean anything.
    obs = duckdb.connect(obs_db, read_only=True)
    columns_seen, dropped = history.column_history(obs, "raw_orders", "customer_id")
    monitor = drift.Monitor.fit("customer_id", columns_seen)
    status_seen, _ = history.column_history(obs, "raw_orders", "status")
    status_monitor = drift.Monitor.fit("status", status_seen)
    cover = history.coverage(obs, "raw_orders")
    obs.close()
    if len(columns_seen) != days or dropped:
        return 1, (f"column history has {len(columns_seen)} observations and dropped "
                   f"{dropped} for {days} partitions")
    if "distinct_count" not in monitor.refused:
        return 1, ("customer_id distinct_count was accepted as a drift signal against "
                   "real metadata, and it tracks the row count")
    verdict = status_monitor.check("distinct_count", 0, 99)
    if verdict is None or verdict.status == "ok":
        return 1, "a new status category did not register against real metadata"

    # day 5. the same verdict has to come out of the alerting layer as a ticket rather
    # than a page, because a new category is not an emergency, and a lost one is.
    gained = alerting.raise_alert("drift", "distinct_count", verdict, fire_rate=0.0)
    lost = alerting.raise_alert(
        "drift", "distinct_count",
        status_monitor.check("distinct_count", 0, 1), fire_rate=0.0)
    if gained is None or gained.severity != "ticket":
        return 1, f"a new category routed to {gained and gained.severity}, not a ticket"
    if lost is None or lost.severity != "page":
        return 1, f"a lost category routed to {lost and lost.severity}, not a page"

    # and the coverage check has to come back clean on a pipeline that just ran properly.
    # this is the ot-016 check running against metadata something really wrote, which is
    # where the 119 false positives showed up rather than in any unit test.
    if cover["no_dataset_metric"] or cover["no_column_metric"]:
        return 1, (f"coverage found {len(cover['no_dataset_metric'])} runs with no "
                   f"dataset metric on a healthy pipeline")

    return 0, (f"observed ok: {runs} runs, {recorded} rows agreed with the warehouse, "
               f"{columns} column metrics. {days} days build no weekday band and one "
               f"pooled band of {pooled[None].lo:.0f} to {pooled[None].hi:.0f}. "
               f"customer_id distinct_count refused at r="
               f"{monitor.refused['distinct_count']['coupling']:+.4f}")


def main():
    with tempfile.TemporaryDirectory() as tmp:
        raw = str(Path(tmp) / "raw")
        db = str(Path(tmp) / "orders.duckdb")

        sh("pipeline.generate", "--start", START, "--end", END, "--out", raw)
        sh("pipeline.run", "--start", START, "--end", END,
           "--raw", raw, "--db", db, "--quiet")
        first_total, first_rows = counts(db)

        sh("pipeline.run", "--start", START, "--end", END,
           "--raw", raw, "--db", db, "--quiet")
        second_total, second_rows = counts(db)

        code, message = observed(raw, tmp)

    if first_total == 0:
        print("FAIL: the first run loaded nothing")
        return 1
    if (first_total, first_rows) != (second_total, second_rows):
        print(f"FAIL: rerun changed the data. {first_total} rows then {second_total}")
        for a, b in zip(first_rows, second_rows):
            if a != b:
                print(f"  {a} -> {b}")
        return 1

    print(f"smoke ok: {first_total} rows over {len(first_rows)} partitions, "
          f"unchanged after a full rerun")
    print(message)
    return code


if __name__ == "__main__":
    sys.exit(main())
