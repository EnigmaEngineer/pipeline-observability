"""The metadata schema does what its comments claim.

Most of this is about the grain. A comment saying "one row per run and dataset" is a
wish. A primary key that rejects the second insert is the thing that makes it true, and
the only way to know the key is actually there is to try to violate it.
"""

import sys
from datetime import datetime

import duckdb

from obs import schema, store
from obs.model import ColumnMetric, DatasetMetric, RunRecord, SchemaVersion
from tests.tiny import Checks

NOW = datetime(2026, 7, 31, 12, 0, 0)


def seed_run(con, run_id="r1", partition="dt=2026-05-01", attempt=1):
    run = RunRecord(run_id=run_id, pipeline="orders", task="load_raw",
                    partition_key=partition, started_at=NOW, attempt=attempt)
    store.insert_run(con, run)
    return run


def seed_dataset(con, run_id="r1", dataset="raw_orders"):
    version = SchemaVersion.from_columns(
        dataset, [("order_id", "VARCHAR"), ("amount", "DOUBLE")], NOW
    )
    store.upsert_schema_version(con, version)
    m = DatasetMetric(run_id=run_id, dataset=dataset, schema_hash=version.schema_hash,
                      row_count=10, collected_at=NOW)
    store.insert_dataset_metric(con, m)
    return version


def run():
    c = Checks("test_schema")
    con = store.connect()

    c.eq(sorted(store.table_names(con)), sorted(schema.TABLE_NAMES),
         "apply creates exactly the four metadata tables")

    # applying twice is what every scheduled run does
    schema.apply(con)
    c.eq(len(store.table_names(con)), 4, "apply is idempotent")

    seed_run(con)
    c.raises(duckdb.ConstraintException, lambda: seed_run(con),
             "obs_run rejects a duplicate run_id")

    # same pipeline, task and partition, second attempt. this must be allowed, because a
    # retry of a failed partition is a different run and both belong in the history.
    seed_run(con, run_id="r1b", attempt=2)
    c.eq(con.execute("SELECT count(*) FROM obs_run").fetchone()[0], 2,
         "a second attempt at the same partition is a separate row")

    c.raises(
        duckdb.ConstraintException,
        lambda: store.insert_run(con, RunRecord(
            run_id="r1c", pipeline="orders", task="load_raw",
            partition_key="dt=2026-05-01", started_at=NOW, attempt=1)),
        "the natural key rejects a third row with the same attempt",
    )

    version = seed_dataset(con)
    c.raises(duckdb.ConstraintException, lambda: seed_dataset(con),
             "obs_dataset_metric is one row per run and dataset")

    c.raises(
        duckdb.ConstraintException,
        lambda: store.insert_dataset_metric(con, DatasetMetric(
            run_id="ghost", dataset="raw_orders", schema_hash=version.schema_hash,
            row_count=1, collected_at=NOW)),
        "a metric row for a run that does not exist is rejected",
    )

    cols = [
        ColumnMetric(run_id="r1", dataset="raw_orders", column_name="amount",
                     data_type="DOUBLE", null_count=0, mean_value=41.2,
                     quantiles={"0.5": 33.0}),
        ColumnMetric(run_id="r1", dataset="raw_orders", column_name="order_id",
                     data_type="VARCHAR", null_count=0, distinct_count=10),
    ]
    c.eq(store.insert_column_metrics(con, cols), 2, "column metrics insert")
    c.raises(duckdb.ConstraintException,
             lambda: store.insert_column_metrics(con, cols[:1]),
             "obs_column_metric is one row per run, dataset and column")

    stored = con.execute(
        "SELECT quantiles_json FROM obs_column_metric WHERE column_name = 'amount'"
    ).fetchone()[0]
    c.eq(stored, '{"0.5": 33.0}', "quantiles survive the round trip as text")

    # first_seen_at is the whole point of the schema version row. a later run seeing the
    # same shape must not move it forward.
    later = SchemaVersion.from_columns(
        "raw_orders", [("order_id", "VARCHAR"), ("amount", "DOUBLE")],
        datetime(2026, 9, 1, 0, 0, 0))
    c.ok(store.upsert_schema_version(con, later) is False,
         "re-seeing a known schema is not an insert")
    kept = con.execute(
        "SELECT first_seen_at FROM obs_schema_version WHERE schema_hash = ?",
        [later.schema_hash]).fetchone()[0]
    c.eq(kept, NOW, "first_seen_at is not moved by a later sighting")

    # update_run and columns_of both exist for the day-2 collector. an untested function
    # waiting for a caller is how a repo accumulates code nobody has run.
    run = RunRecord(run_id="r9", pipeline="orders", task="build_daily",
                    partition_key="dt=2026-05-02", started_at=NOW)
    store.insert_run(con, run)
    run.finish(datetime(2026, 7, 31, 12, 0, 3), error="read_json blew up")
    store.update_run(con, run)
    got = con.execute(
        "SELECT status, duration_ms, error FROM obs_run WHERE run_id = 'r9'").fetchone()
    c.eq(got, ("failed", 3000, "read_json blew up"), "update_run closes out a failed run")

    cols_seen = store.columns_of(con, "obs_dataset_metric")
    c.eq([n for n, _ in cols_seen][:3], ["run_id", "dataset", "schema_hash"],
         "columns_of returns columns in ordinal position")
    c.eq(store.columns_of(con, "no_such_table"), [],
         "columns_of on a missing table is empty, not an error")

    sf = schema.ddl("snowflake")
    c.ok(all("TIMESTAMP_NTZ" in s or "{" not in s for s in sf),
         "snowflake ddl uses TIMESTAMP_NTZ")
    c.ok(not any("{" in s for s in sf), "no unsubstituted placeholders in snowflake ddl")
    c.raises(ValueError, lambda: schema.ddl("bigquery"), "unknown dialect is rejected")

    # ot-021, decided day 7. the expected column list is parsed back out of the DDL rather
    # than written twice, so the first thing to check is that the parse agrees with what
    # the database actually got. a parser that dropped a column would make check_shape
    # raise on a fresh database, and a parser that kept the constraint lines would make it
    # raise on every database forever.
    wanted = schema.expected_columns()
    c.eq(sorted(wanted), sorted(schema.TABLE_NAMES), "every table is parsed")
    for table in schema.TABLE_NAMES:
        actual = [name for name, _ in store.columns_of(con, table)]
        c.eq(wanted[table], actual, f"{table} parses to exactly the columns created")
    c.ok("PRIMARY" not in " ".join(wanted["obs_column_metric"]),
         "constraint lines are not mistaken for columns")

    c.eq(sorted(schema.check_shape(con)), sorted(schema.TABLE_NAMES),
         "a database built from this DDL passes and says what it checked")

    # the real case. a database created before cold_start was added keeps the old shape,
    # because CREATE TABLE IF NOT EXISTS is a no-op against it. building that state here
    # by stripping the column out of the shipped DDL, so the fixture cannot drift from
    # what the module says.
    old = duckdb.connect(":memory:")
    stripped = "\n".join(line for line in schema.ddl("duckdb")[0].splitlines()
                         if "cold_start" not in line)
    c.ok("cold_start" not in stripped, "the fixture really removed the column")
    old.execute(stripped)
    c.raises_message(RuntimeError, "cold_start", lambda: schema.check_shape(old),
                     "an old shaped table raises and names the missing column")
    c.raises_message(RuntimeError, "ot-021", lambda: schema.apply(old),
                     "apply raises too, so no caller can reach an insert on it")
    old.close()

    # a reorder holds the same columns and is still a different table, and this project has
    # a specific reason to care. the schema hash is order sensitive on purpose, because a
    # column reorder breaks a positional load. a mutant comparing the two lists as sets
    # survived the first version of these tests, because a fixture that only ever removed a
    # column never exercised the ordering rule. same shape as the 08-02 fixture lesson.
    shuffled = duckdb.connect(":memory:")
    shuffled.execute(
        "CREATE TABLE obs_schema_version (dataset VARCHAR, schema_hash VARCHAR, "
        "columns_json VARCHAR, column_count INTEGER, first_seen_at TIMESTAMP)")
    have = [n for n, _ in store.columns_of(shuffled, "obs_schema_version")]
    c.eq(sorted(have), sorted(schema.expected_columns()["obs_schema_version"]),
         "the reordered fixture holds exactly the right columns as a set")
    c.ok(have != schema.expected_columns()["obs_schema_version"],
         "and really is in a different order, so the case is live")
    c.raises_message(RuntimeError, "obs_schema_version",
                     lambda: schema.check_shape(shuffled),
                     "a reordered table is rejected even though no column is missing")
    shuffled.close()

    # a table this schema has never heard of is not this schema's problem, and a database
    # holding none of these tables has nothing to check rather than something wrong.
    empty = duckdb.connect(":memory:")
    c.eq(schema.check_shape(empty), [], "an empty database checks nothing")
    empty.execute("CREATE TABLE unrelated (x INTEGER)")
    c.eq(schema.check_shape(empty), [], "and an unrelated table is ignored")
    empty.close()

    return c


if __name__ == "__main__":
    sys.exit(run().report())
