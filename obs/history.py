"""Pull observations out of the metadata tables for the baseline to fit on.

This is the only file in the baseline path that knows SQL exists. `obs/baseline.py` takes
lists of pairs and nothing else, which is what makes the whole model testable without a
database.

Two decisions live here rather than in the model, because they are about what counts as an
observation rather than about how to band one.

**One observation per partition, from the last successful attempt.** A partition that
failed twice and succeeded on the third try produces three rows in `obs_run`. Letting all
three into the history would give a bad day three votes and would mix failed durations,
which measure how long it took to break, into a baseline of how long it takes to work. The
last attempt is the one whose output is actually in the warehouse.

**A row whose partition key cannot be read is counted, not dropped.** The key is written as
`dt=YYYY-MM-DD` by `scripts/run_observed.py` and this parses that shape. Anything else is
returned in a skipped count so a caller can see it. A monitoring project that silently
loses rows from its own training set has no business telling anyone else about data
quality.
"""

import json
from datetime import date

PARTITION_PREFIX = "dt="


def partition_date(partition_key):
    """`dt=2026-03-02` becomes a date. Anything else becomes None."""
    if not partition_key or not partition_key.startswith(PARTITION_PREFIX):
        return None
    try:
        return date.fromisoformat(partition_key[len(PARTITION_PREFIX):])
    except ValueError:
        return None


# Monday is 0, matching date.weekday() and DOW_FACTOR in pipeline/generate.py. DuckDB's
# dayofweek starts on Sunday, so the conversion is done in Python where there is one
# convention rather than in SQL where there are two.
WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
                 "Saturday", "Sunday"]


def _latest_attempts(rows):
    """Keep the highest attempt per partition key.

    Sorted here rather than trusting the caller's ORDER BY. Both queries below do order by
    attempt, and a monitor whose answer changes because someone edited an ORDER BY in a
    query three functions away is the kind of thing that is found six months later.
    """
    best = {}
    for row in sorted(rows, key=lambda r: r[2]):
        best[row[0]] = row
    return list(best.values())


def recent(observations, days):
    """The last `days` partitions. A trailing window, not a trailing calendar period.

    The difference matters when partitions are missing. Ten calendar days with three gaps
    is seven observations, and a window that counted calendar days would quietly fit on
    seven while believing it had ten.
    """
    return observations[-days:] if days else observations


def volume_history(con, dataset="raw_orders", pipeline="orders"):
    """Row count per partition, from the last successful run that produced it.

    Returns `(observations, skipped)` where an observation is
    `(weekday_index, row_count, partition_date)`.
    """
    rows = con.execute(
        """
        SELECT r.partition_key, m.row_count, r.attempt
          FROM obs_dataset_metric m
          JOIN obs_run r ON r.run_id = m.run_id
         WHERE m.dataset = ? AND r.pipeline = ? AND r.status = 'success'
         ORDER BY r.partition_key, r.attempt
        """,
        [dataset, pipeline],
    ).fetchall()
    return _shape(_latest_attempts(rows))


def duration_history(con, pipeline="orders", task="load_raw"):
    """Duration per partition, from the last successful attempt at the task.

    Failed runs are excluded on purpose. A failure's duration measures how long the
    pipeline took to break, and mixing that into a baseline of how long it takes to work
    is how a monitor learns that broken is normal.

    Returns `(observations, skipped)` where an observation is
    `(weekday_index, duration_ms, partition_date)`.
    """
    rows = con.execute(
        """
        SELECT partition_key, duration_ms, attempt
          FROM obs_run
         WHERE pipeline = ? AND task = ? AND status = 'success'
               AND duration_ms IS NOT NULL
         ORDER BY partition_key, attempt
        """,
        [pipeline, task],
    ).fetchall()
    return _shape(_latest_attempts(rows))


def run_order(con, pipeline="orders", task="load_raw"):
    """Partition keys in the order the runs actually started.

    Needed because the ordinal position of a run in its process is not recoverable from
    the partition date. A backfill runs 119 partitions inside one process and the first
    one pays for opening the database, and that ordering is the only place that shows.
    """
    rows = con.execute(
        """
        SELECT partition_key, duration_ms
          FROM obs_run
         WHERE pipeline = ? AND task = ? AND status = 'success'
         ORDER BY started_at, attempt
        """,
        [pipeline, task],
    ).fetchall()
    return rows


def column_history(con, dataset="raw_orders", column="order_amount_usd",
                   pipeline="orders"):
    """Per partition column metrics, from the last successful run that produced them.

    Same rule as the other two readers. One observation per partition, taken from the
    highest attempt, failed runs excluded. The row count rides along because every
    proportion in `obs/drift.py` is a share of the partition and because the coupling
    check needs it.

    Returns `(observations, skipped)` where an observation is a dict carrying the
    weekday, the partition date and the raw stored values. A dict rather than a tuple
    because there are six fields and a positional row of six is where the day-2 slicing
    bug came from.
    """
    rows = con.execute(
        """
        SELECT r.partition_key, r.attempt, m.quantiles_json, m.null_count,
               m.distinct_count, m.top_values_json, d.row_count
          FROM obs_column_metric m
          JOIN obs_run r ON r.run_id = m.run_id
          JOIN obs_dataset_metric d
            ON d.run_id = m.run_id AND d.dataset = m.dataset
         WHERE m.dataset = ? AND m.column_name = ? AND r.pipeline = ?
               AND r.status = 'success'
         ORDER BY r.partition_key, r.attempt
        """,
        [dataset, column, pipeline],
    ).fetchall()

    best = {}
    for row in sorted(rows, key=lambda r: r[1]):
        best[row[0]] = row

    observations = []
    skipped = 0
    for partition_key, _attempt, qj, nulls, distinct, tvj, row_count in best.values():
        day = partition_date(partition_key)
        if day is None:
            skipped += 1
            continue
        observations.append({
            "weekday": day.weekday(),
            "date": day,
            "quantiles": None if qj is None else json.loads(qj),
            "null_count": nulls,
            "distinct_count": distinct,
            "top_values": None if tvj is None else json.loads(tvj),
            "row_count": row_count,
        })
    observations.sort(key=lambda o: o["date"])
    return observations, skipped


def _shape(rows):
    observations = []
    skipped = 0
    for partition_key, value, _attempt in rows:
        day = partition_date(partition_key)
        if day is None or value is None:
            skipped += 1
            continue
        observations.append((day.weekday(), value, day))
    observations.sort(key=lambda o: o[2])
    return observations, skipped


def keyed(observations):
    """Drop the date, leaving the `(key, value)` pairs the model wants."""
    return [(k, v) for k, v, _ in observations]


def unkeyed(observations):
    """Same values under a single key, for the pooled comparison."""
    return [(None, v) for _, v, _ in observations]
