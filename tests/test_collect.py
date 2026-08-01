"""What the collector says about a table has to be true of the table.

Most of these are small because the values are known by construction. The ones that are
not small are the two that would fail silently in production: the positional slicing of
the single pass result, and the promise that a broken collector does not break the
pipeline.
"""

import sys
from datetime import datetime

import duckdb

from obs import collect, store
from obs.model import QUANTILE_PROBS, RunRecord
from tests.tiny import Checks

NOW = datetime(2026, 8, 1, 9, 30, 0)


def source():
    """A table small enough that every expected value can be counted by hand.

    The column order matters. A text column sits between two numeric ones so that a
    slicing bug in the single pass result cannot line up by accident.
    """
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE events (
            part DATE, label VARCHAR, amount DOUBLE, note VARCHAR,
            rare VARCHAR, n INTEGER, seen_at TIMESTAMP
        )
        """
    )
    # `rare` is null in three of the four rows of the first partition on purpose. `note`
    # is null in two of four, and a null count that matched the non null count would let
    # a collector reporting the wrong one of the two pass this file.
    rows = [
        ("2026-05-01", "a", 10.0, None, None, 1, "2026-05-01 01:00:00"),
        ("2026-05-01", "a", 20.0, "x", None, 2, "2026-05-01 05:00:00"),
        ("2026-05-01", "b", 30.0, None, None, 3, "2026-05-01 09:00:00"),
        ("2026-05-01", "b", 40.0, "y", "q", 4, "2026-05-01 23:00:00"),
        ("2026-05-02", "c", 99.0, "z", "q", 9, "2026-05-02 04:00:00"),
    ]
    con.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    return con


def by_name(profile):
    return {m.column_name: m for m in profile.column_metrics}


def run():
    c = Checks("test_collect")
    con = source()
    obs = store.connect()
    # the metric rows carry a foreign key to obs_run, so a run has to exist before its
    # metrics can. That ordering is free in the driver because the tracker writes the run
    # row before the work starts, and it is worth having the schema insist on it.
    for run_id in ("r1", "r2", "r3", "r9"):
        store.insert_run(obs, RunRecord(run_id=run_id, pipeline="t", task="t",
                                        partition_key=run_id, started_at=NOW))

    p = collect.profile(con, "events", "r1", where="part = ?",
                        params=["2026-05-01"], collected_at=NOW,
                        event_time_column="seen_at", byte_size=4096)
    cols = by_name(p)

    c.eq(p.dataset_metric.row_count, 4, "the where clause profiles the partition")
    c.eq(cols["note"].null_count, 2, "null_count counts the nulls, not the rows")
    c.eq(cols["rare"].null_count, 3, "and not the non nulls either")
    c.eq(cols["label"].distinct_count, 2, "distinct_count is exact")
    c.eq(cols["amount"].min_value, "10.0", "min is stored as text")
    c.eq(cols["amount"].max_value, "40.0", "max is stored as text")
    c.eq(cols["amount"].mean_value, 25.0, "mean is the mean of the partition only")
    c.eq(cols["label"].mean_value, None, "a text column has no mean")
    c.eq(cols["n"].mean_value, 2.5, "the numeric column after a text one is not shifted")
    c.eq(cols["n"].min_value, "1", "and neither are its min and max")

    c.eq(sorted(cols["amount"].quantiles), sorted(str(p_) for p_ in QUANTILE_PROBS),
         "quantiles use the fixed probabilities and nothing else")
    c.eq(cols["amount"].quantiles["0.5"], 25.0, "the median of 10 20 30 40")
    c.eq(cols["label"].quantiles, None, "no quantiles on a text column")

    c.eq(cols["label"].top_values, {"a": 2, "b": 2}, "top values for a small text column")
    c.eq(cols["note"].top_values, {"x": 1, "y": 1},
         "top values skip nulls, which null_count already carries")
    c.eq(cols["n"].top_values, None, "no top values on a numeric column")

    c.eq(p.dataset_metric.event_time_min, datetime(2026, 5, 1, 1, 0),
         "event time min comes back typed, not as text")
    c.eq(p.dataset_metric.event_time_max, datetime(2026, 5, 1, 23, 0),
         "and it respects the partition filter")
    c.eq(p.dataset_metric.byte_size, 4096, "byte_size is passed in, never guessed")
    c.eq(p.queries, 1 + 3, "one pass plus one query per column that gets top values")

    whole = collect.profile(con, "events", "r2", collected_at=NOW)
    c.eq(whole.dataset_metric.row_count, 5, "no where clause profiles the whole table")
    c.eq(whole.dataset_metric.schema_hash, p.dataset_metric.schema_hash,
         "same shape, same hash, whatever the filter was")

    # a partition profile that quietly read the whole table would still look plausible,
    # so the two have to disagree somewhere provable.
    c.ok(by_name(whole)["amount"].mean_value != cols["amount"].mean_value,
         "the filtered and unfiltered profiles are genuinely different reads")

    written = collect.write(obs, p)
    c.eq(written, 7, "one column metric row per column")
    stored = obs.execute(
        "SELECT row_count, byte_size FROM obs_dataset_metric WHERE run_id = ?",
        ["r1"]).fetchone()
    c.eq(stored, (4, 4096), "the dataset metric round trips through duckdb")
    c.eq(obs.execute("SELECT count(*) FROM obs_schema_version").fetchone()[0], 1,
         "the schema version is written once")

    quantiles_back = obs.execute(
        "SELECT quantiles_json FROM obs_column_metric "
        "WHERE run_id = ? AND column_name = 'amount'", ["r1"]).fetchone()[0]
    c.ok('"0.5": 25.0' in quantiles_back, "quantiles survive as json text")

    # the contract collect_into exists for. the pipeline does not fall over because the
    # thing watching it fell over, and what is left behind is a gap.
    missing = collect.collect_into(obs, con, "r9", "no_such_table")
    c.eq(missing, None, "a collector failure returns None instead of raising")
    c.eq(obs.execute("SELECT count(*) FROM obs_dataset_metric WHERE run_id = ?",
                     ["r9"]).fetchone()[0], 0,
         "and leaves no dataset metric, which is what day 5 alerts on")

    con.execute('CREATE TABLE odd ("a""b" INTEGER, "select" VARCHAR)')
    con.execute("""INSERT INTO odd VALUES (1, 'k'), (2, 'k')""")
    odd = collect.profile(con, "odd", "r3", collected_at=NOW)
    odd_cols = by_name(odd)
    c.eq(odd_cols['a"b'].mean_value, 1.5, "a quote in a column name does not break the sql")
    c.eq(odd_cols["select"].top_values, {"k": 2}, "and neither does a reserved word")

    c.eq(store.table_names(con), ["events", "odd"],
         "the collector created nothing in the database it was reading")

    con.close()
    obs.close()
    return c


if __name__ == "__main__":
    sys.exit(run().report())
