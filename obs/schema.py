"""The run-metadata schema.

Four tables. Everything the monitors read on later days comes out of these, so the
grain of each one is the decision that matters most here.

    obs_run             one row per attempt at a (pipeline, task, partition)
    obs_schema_version  one row per distinct column list a dataset has ever had
    obs_dataset_metric  one row per (run_id, dataset)
    obs_column_metric   one row per (run_id, dataset, column_name)

Two things are deliberate and worth knowing before you extend this.

The schema of a dataset is stored once per distinct shape and referenced by hash from
the metric row. The obvious alternative is a snapshot row per run. That duplicates an
unchanging column list thousands of times to answer a question that gets asked once per
incident. Hashing gives the same answer and makes "did the schema change between these
two runs" a hash comparison instead of a diff.

Column summaries are quantiles at fixed probabilities, not histograms. A histogram needs
bin edges chosen up front and edges chosen from week one are wrong by week five. Fixed
probabilities are comparable across any two days without agreeing on anything. The cost
is real. You cannot recover a distribution from seven quantiles, so a bimodal shift that
leaves the quantiles alone is invisible to this schema. That limitation belongs to day 4
and it is written in the README.
"""

# DuckDB is the daily driver. The Snowflake DDL is generated from the same template so
# the two cannot drift, but it has not been run against a real account yet. See README.
DIALECTS = {
    "duckdb": {"ts": "TIMESTAMP", "dbl": "DOUBLE", "json": "VARCHAR"},
    "snowflake": {"ts": "TIMESTAMP_NTZ", "dbl": "FLOAT", "json": "VARCHAR"},
}

# JSON payloads are stored as text on both. DuckDB's JSON type and Snowflake's VARIANT
# do not take the same insert, and nothing here ever queries into these blobs by path.
# They get read whole and parsed in Python. Text keeps one code path.

TABLES = [
    (
        "obs_run",
        """
        CREATE TABLE IF NOT EXISTS obs_run (
            run_id        VARCHAR PRIMARY KEY,
            pipeline      VARCHAR NOT NULL,
            task          VARCHAR NOT NULL,
            partition_key VARCHAR,
            attempt       INTEGER NOT NULL DEFAULT 1,
            started_at    {ts} NOT NULL,
            ended_at      {ts},
            duration_ms   BIGINT,
            status        VARCHAR NOT NULL,
            error         VARCHAR,
            code_version  VARCHAR,
            triggered_by  VARCHAR NOT NULL DEFAULT 'schedule',
            UNIQUE (pipeline, task, partition_key, attempt)
        )
        """,
    ),
    (
        "obs_schema_version",
        """
        CREATE TABLE IF NOT EXISTS obs_schema_version (
            schema_hash   VARCHAR PRIMARY KEY,
            dataset       VARCHAR NOT NULL,
            columns_json  {json} NOT NULL,
            column_count  INTEGER NOT NULL,
            first_seen_at {ts} NOT NULL
        )
        """,
    ),
    (
        "obs_dataset_metric",
        """
        CREATE TABLE IF NOT EXISTS obs_dataset_metric (
            run_id          VARCHAR NOT NULL,
            dataset         VARCHAR NOT NULL,
            schema_hash     VARCHAR NOT NULL,
            row_count       BIGINT NOT NULL,
            byte_size       BIGINT,
            event_time_min  {ts},
            event_time_max  {ts},
            collected_at    {ts} NOT NULL,
            PRIMARY KEY (run_id, dataset),
            FOREIGN KEY (run_id) REFERENCES obs_run (run_id),
            FOREIGN KEY (schema_hash) REFERENCES obs_schema_version (schema_hash)
        )
        """,
    ),
    (
        "obs_column_metric",
        """
        CREATE TABLE IF NOT EXISTS obs_column_metric (
            run_id          VARCHAR NOT NULL,
            dataset         VARCHAR NOT NULL,
            column_name     VARCHAR NOT NULL,
            data_type       VARCHAR NOT NULL,
            null_count      BIGINT NOT NULL,
            distinct_count  BIGINT,
            min_value       VARCHAR,
            max_value       VARCHAR,
            mean_value      {dbl},
            quantiles_json  {json},
            top_values_json {json},
            PRIMARY KEY (run_id, dataset, column_name),
            FOREIGN KEY (run_id, dataset)
                REFERENCES obs_dataset_metric (run_id, dataset)
        )
        """,
    ),
]

# min_value and max_value are text on purpose. One table holds columns of every type and
# splitting into numeric, text and timestamp variants triples the width to serve a field
# that exists for a human reading an incident. The numeric summary a monitor actually
# reads is mean_value plus quantiles_json. That is the tradeoff and it is not free. You
# cannot write a SQL predicate over min_value without a cast.

TABLE_NAMES = [name for name, _ in TABLES]


def ddl(dialect="duckdb"):
    """Return the CREATE TABLE statements for a dialect, in dependency order."""
    if dialect not in DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}, have {sorted(DIALECTS)}")
    types = DIALECTS[dialect]
    return [sql.format(**types).strip() for _, sql in TABLES]


def apply(con, dialect="duckdb"):
    """Create the metadata tables if they are not there. Safe to call every run."""
    for statement in ddl(dialect):
        con.execute(statement)
    return TABLE_NAMES


# Snowflake accepts PRIMARY KEY and FOREIGN KEY and then does not enforce either one.
# So the constraints above are documentation there and a real guard here. The day-2
# collector has to treat a duplicate grain as its own problem rather than expect the
# warehouse to reject it.
