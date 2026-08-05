"""Connection plus the writes for the metadata tables.

DuckDB only for now. The Snowflake DDL is generated from the same template in schema.py
but nothing here has ever talked to Snowflake. There is no adapter interface and no base
class, because there is one implementation and inventing a second seat for an occupant
that does not exist yet is how you end up with an abstraction shaped around the wrong
thing. When the Snowflake path is real this file grows a branch.
"""

import duckdb

from . import schema
from .model import ColumnMetric, DatasetMetric, RunRecord, SchemaVersion


def connect(path=":memory:", dialect="duckdb"):
    con = duckdb.connect(path)
    schema.apply(con, dialect)
    return con


def insert_run(con, run: RunRecord):
    con.execute(
        """
        INSERT INTO obs_run (run_id, pipeline, task, partition_key, attempt,
                             started_at, ended_at, duration_ms, status, error,
                             code_version, triggered_by, cold_start)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [run.run_id, run.pipeline, run.task, run.partition_key, run.attempt,
         run.started_at, run.ended_at, run.duration_ms, run.status, run.error,
         run.code_version, run.triggered_by, run.cold_start],
    )


def update_run(con, run: RunRecord):
    con.execute(
        """
        UPDATE obs_run
           SET ended_at = ?, duration_ms = ?, status = ?, error = ?
         WHERE run_id = ?
        """,
        [run.ended_at, run.duration_ms, run.status, run.error, run.run_id],
    )


def upsert_schema_version(con, version: SchemaVersion):
    """First writer wins. first_seen_at is the point of the row, so a later run seeing
    the same shape must not move it."""
    existing = con.execute(
        "SELECT 1 FROM obs_schema_version WHERE schema_hash = ?", [version.schema_hash]
    ).fetchone()
    if existing:
        return False
    con.execute(
        """
        INSERT INTO obs_schema_version (schema_hash, dataset, columns_json,
                                        column_count, first_seen_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [version.schema_hash, version.dataset, version.columns_json,
         version.column_count, version.first_seen_at],
    )
    return True


def insert_dataset_metric(con, m: DatasetMetric):
    con.execute(
        """
        INSERT INTO obs_dataset_metric (run_id, dataset, schema_hash, row_count,
                                        byte_size, event_time_min, event_time_max,
                                        collected_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [m.run_id, m.dataset, m.schema_hash, m.row_count, m.byte_size,
         m.event_time_min, m.event_time_max, m.collected_at],
    )


def insert_column_metrics(con, metrics):
    rows = [
        [m.run_id, m.dataset, m.column_name, m.data_type, m.null_count,
         m.distinct_count, m.min_value, m.max_value, m.mean_value,
         m.quantiles_json(), m.top_values_json()]
        for m in metrics
    ]
    con.executemany(
        """
        INSERT INTO obs_column_metric (run_id, dataset, column_name, data_type,
                                       null_count, distinct_count, min_value,
                                       max_value, mean_value, quantiles_json,
                                       top_values_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def table_names(con):
    rows = con.execute(
        """
        SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'main' ORDER BY table_name
        """
    ).fetchall()
    return [r[0] for r in rows]


def columns_of(con, table):
    """Ordered (name, type) pairs for a table. This is what the day-2 collector will
    hash to detect a schema change on the pipeline's own output tables."""
    rows = con.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
         WHERE table_schema = 'main' AND table_name = ?
         ORDER BY ordinal_position
        """,
        [table],
    ).fetchall()
    return [(r[0], r[1]) for r in rows]
