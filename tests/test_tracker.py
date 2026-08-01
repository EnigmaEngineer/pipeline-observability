"""The tracker is judged on what it leaves behind when things go wrong.

A run that succeeded is easy to record. The cases that matter are the failed one, the
retry, and the one that never came back at all, because those are the three a monitor is
looking for and all three are invisible if the row is only written at the end.
"""

import sys
from datetime import datetime, timedelta

from obs import store, tracker
from obs.model import RunRecord
from tests.tiny import Checks

START = datetime(2026, 8, 1, 10, 0, 0)


def fixed_clock(*offsets_seconds):
    """A clock that returns known instants, so duration_ms is checked against arithmetic
    rather than against however long the test happened to take."""
    times = iter([START + timedelta(seconds=s) for s in offsets_seconds])
    return lambda: next(times)


def one(con, sql, params=None):
    return con.execute(sql, params or []).fetchone()


def run():
    c = Checks("test_tracker")
    con = store.connect()

    # the whole point of the ordering: the row is visible while the work is still going.
    with tracker.track(con, "orders", "load_raw", "dt=2026-05-01",
                       run_id="a1", clock=fixed_clock(0, 1.5)) as record:
        row = one(con, "SELECT status, ended_at, duration_ms FROM obs_run "
                       "WHERE run_id = 'a1'")
        c.eq(row, ("running", None, None),
             "the run is on the table as running before the work finishes")
        c.eq(record.attempt, 1, "the first attempt at a partition is attempt 1")

    c.eq(one(con, "SELECT status, duration_ms FROM obs_run WHERE run_id = 'a1'"),
         ("success", 1500), "and is closed out with a duration when it returns")
    c.eq(one(con, "SELECT started_at, ended_at FROM obs_run WHERE run_id = 'a1'"),
         (START, START + timedelta(seconds=1.5)), "both timestamps come from the clock")

    def failing():
        with tracker.track(con, "orders", "load_raw", "dt=2026-05-01",
                           run_id="a2", clock=fixed_clock(10, 12)):
            raise ValueError("bad partition file")

    c.raises(ValueError, failing, "the failure is re-raised, not swallowed")
    status, error, duration = one(
        con, "SELECT status, error, duration_ms FROM obs_run WHERE run_id = 'a2'")
    c.eq(status, "failed", "a raised exception marks the run failed")
    c.eq(error, "ValueError: bad partition file", "the error text says what happened")
    c.eq(duration, 2000, "a failed run still gets a duration")

    c.eq(one(con, "SELECT max(attempt) FROM obs_run WHERE partition_key = 'dt=2026-05-01'")[0],
         2, "the retry is attempt 2, so the first attempt is still on the table")
    c.eq(one(con, "SELECT count(*) FROM obs_run WHERE partition_key = 'dt=2026-05-01'")[0],
         2, "and the retry did not overwrite it")

    with tracker.track(con, "orders", "load_raw", "dt=2026-05-02", run_id="a3"):
        pass
    c.eq(one(con, "SELECT attempt FROM obs_run WHERE run_id = 'a3'")[0], 1,
         "a different partition starts its own attempt count")

    with tracker.track(con, "orders", "vacuum", None, run_id="a4"):
        pass
    with tracker.track(con, "orders", "vacuum", None, run_id="a5"):
        pass
    c.eq(one(con, "SELECT attempt FROM obs_run WHERE run_id = 'a5'")[0], 2,
         "a task with no partition still counts its attempts, so null is not a new group")

    # a Ctrl-C is a real way for a run to end. it should leave a failed row rather than
    # one stuck at running, which is the state reserved for a process that died.
    def interrupted():
        with tracker.track(con, "orders", "build_daily", "dt=2026-05-03", run_id="a6"):
            raise KeyboardInterrupt()

    c.raises(KeyboardInterrupt, interrupted, "an interrupt propagates like any other")
    c.eq(one(con, "SELECT status FROM obs_run WHERE run_id = 'a6'")[0], "failed",
         "and the row is closed out rather than left running")

    # the case nothing can catch. simulate the process disappearing by writing the start
    # row and never updating it.
    store.insert_run(con, RunRecord(
        run_id="dead", pipeline="orders", task="load_raw",
        partition_key="dt=2026-05-04", started_at=START - timedelta(hours=3)))
    stale = tracker.stale_runs(con, older_than_minutes=60, as_of=START)
    c.eq([r[0] for r in stale], ["dead"],
         "a run that started three hours ago and never finished is stale")
    c.eq(tracker.stale_runs(con, older_than_minutes=60 * 24, as_of=START), [],
         "and a wider cutoff correctly finds nothing")

    c.eq(len(tracker.short_error(ValueError("x" * 900))), tracker.ERROR_CHARS,
         "a long error is truncated rather than filling the column")
    c.ok(tracker.new_run_id() != tracker.new_run_id(), "run ids are not repeated")

    con.close()
    return c


if __name__ == "__main__":
    sys.exit(run().report())
