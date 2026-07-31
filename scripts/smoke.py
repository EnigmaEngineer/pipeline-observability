"""End to end check over the real CLI path.

`test_pipeline` already pins rerun safety at the function level. This exists because the
unit test calls `load_raw` directly and CI needs to know the command line path works too.
It generates two weeks and runs the range. Then it snapshots the counts, runs the same
range again and compares.

The first version of this was two `python -m pipeline.run` steps in the workflow with a
comment claiming they proved rerun safety. They proved nothing. Running a thing twice is
not checking it.
"""

import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

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
    return 0


if __name__ == "__main__":
    sys.exit(main())
