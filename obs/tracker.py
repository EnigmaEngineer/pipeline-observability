"""The run row is written before the work, not after.

The obvious way to record a pipeline run is to time it and insert a row when it finishes.
That records every run except the ones worth knowing about. A process the scheduler kills,
an out of memory kill, a machine that reboots halfway through a load: none of those reach
the insert. The metadata then says the run never happened, and a freshness monitor reads
a run that died the same way it reads a pipeline nobody scheduled.

So the row goes in at the start, status 'running' and no ended_at, and gets updated on the
way out. A crash leaves a row saying a run started and never finished, which is a fact a
monitor can act on.

Two costs, both real. It is two writes per run instead of one. And a row can sit at
'running' forever, because the one failure mode that cannot be caught here is the one
where the process stops existing. `stale_runs` is how those get found, and choosing the
cutoff is a day-5 problem since it depends on what the pipeline's normal duration is.
"""

import uuid
from contextlib import contextmanager
from datetime import timedelta

from . import store
from .model import RunRecord, now_utc

ERROR_CHARS = 400


def new_run_id():
    return uuid.uuid4().hex


def short_error(exc):
    """Type plus message, truncated. A full traceback belongs in the process log. What
    this column is for is telling two failures apart at a glance."""
    text = f"{type(exc).__name__}: {exc}"
    return text[:ERROR_CHARS]


def next_attempt(con, pipeline, task, partition_key):
    """Attempt numbers count retries of the same partition, so the run history keeps
    them all instead of the last one overwriting the rest.

    Read then insert is not atomic. Under DuckDB in one process that does not matter, and
    the UNIQUE key would reject a real collision anyway. Under Snowflake it does matter,
    because Snowflake accepts a UNIQUE constraint and never enforces it, so two schedulers
    starting the same partition at once would both write the same attempt and neither
    would be told. That is a problem for whenever the Snowflake path becomes real.
    """
    row = con.execute(
        """
        SELECT max(attempt) FROM obs_run
         WHERE pipeline = ? AND task = ?
           AND partition_key IS NOT DISTINCT FROM ?
        """,
        [pipeline, task, partition_key],
    ).fetchone()
    return 1 if row[0] is None else int(row[0]) + 1


@contextmanager
def track(con, pipeline, task, partition_key=None, code_version=None,
          triggered_by="schedule", run_id=None, clock=now_utc):
    """Record one attempt at one task. Yields the RunRecord so the body can use run_id.

    The exception is re-raised after the row is updated. A tracker that swallowed the
    failure would make the pipeline look healthier than it is, which is the opposite of
    the job.
    """
    started = clock()
    record = RunRecord(
        run_id=run_id or new_run_id(),
        pipeline=pipeline,
        task=task,
        partition_key=partition_key,
        started_at=started,
        status="running",
        attempt=next_attempt(con, pipeline, task, partition_key),
        code_version=code_version,
        triggered_by=triggered_by,
    )
    store.insert_run(con, record)
    try:
        yield record
    except BaseException as exc:
        # BaseException on purpose. A Ctrl-C is a real way for a run to end and it should
        # leave a failed row, not a row stuck at 'running'. SIGKILL is the case nothing
        # here can reach.
        record.finish(clock(), error=short_error(exc))
        store.update_run(con, record)
        raise
    record.finish(clock())
    store.update_run(con, record)


def stale_runs(con, older_than_minutes=60, as_of=None):
    """Runs that started, never finished, and are past the cutoff.

    This is the query that the insert-first ordering exists to make possible. The cutoff
    is an argument rather than a constant because it depends on what normal looks like
    for a given task, and that is not known until the day-3 duration baseline exists.
    """
    as_of = as_of or now_utc()
    cutoff = as_of - timedelta(minutes=older_than_minutes)
    return con.execute(
        """
        SELECT run_id, pipeline, task, partition_key, started_at
          FROM obs_run
         WHERE status = 'running' AND started_at < ?
         ORDER BY started_at
        """,
        [cutoff],
    ).fetchall()
