"""The run-metadata schema.

Four tables. Everything the monitors read on later days comes out of these, so the
grain of each one is the decision that matters most here.

    obs_run             one row per attempt at a (pipeline, task, partition)
                        carries cold_start, see below
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
            cold_start    BOOLEAN NOT NULL DEFAULT FALSE,
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

# cold_start says whether this was the first attempt at its (pipeline, task) inside the
# process that ran it. Added day 5 because the duration monitor fires on every restart and
# nothing in the metadata could tell a restart from a regression. It has to be written by
# the tracker at the moment the run starts. It cannot be recovered later from a gap in
# started_at, because a backfill and a schedule produce the same gaps and mean opposite
# things by them. What it is worth as a suppression rule is a separate question and the
# answer on this feed is uncomfortable. See obs/alerting.py.

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


def expected_columns():
    """Column names per table, parsed back out of the DDL this module ships.

    Parsed rather than listed a second time. A hand written list beside the DDL is two
    declarations of the same thing and they drift, and then the check compares the schema
    against the stale copy instead of against what gets created.
    """
    wanted = {}
    for name, sql in TABLES:
        columns = []
        body = sql.split("(", 1)[1]
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if not line or line.startswith(")"):
                continue
            first = line.split()[0].upper()
            if first in ("PRIMARY", "FOREIGN", "UNIQUE", "REFERENCES", "CHECK"):
                continue
            columns.append(line.split()[0])
        wanted[name] = columns
    return wanted


def check_shape(con):
    """Raise if an existing metadata database does not match the DDL in this file.

    `ot-021`, decided on day 7. `apply` runs CREATE TABLE IF NOT EXISTS, so a database
    created before a column was added keeps the old shape and the create is a no-op. The
    first thing that notices is an INSERT failing on a column count, several layers away
    from the cause, with a message about parameters rather than about migrations.

    A real migration path is a version row and an ALTER ladder. That is a day of work and
    it is not what this project is demonstrating, so it is not here. What is here is the
    difference between failing at the point of the problem and failing four frames later.
    Adding `cold_start` on day 5 would have hit this on any database that already existed,
    and nothing noticed because every run rebuilds from scratch under /tmp.

    Returns the tables it checked, so a caller cannot mistake a skipped check for a pass.
    """
    checked = []
    for table, wanted in expected_columns().items():
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?"
            " ORDER BY ordinal_position",
            [table],
        ).fetchall()
        if not rows:
            continue
        have = [r[0] for r in rows]
        if have != wanted:
            missing = [c for c in wanted if c not in have]
            extra = [c for c in have if c not in wanted]
            raise RuntimeError(
                f"{table} in this database does not match the shipped DDL. "
                f"missing {missing or 'nothing'}, unexpected {extra or 'nothing'}. "
                "this schema is create only and has no migration path, so an existing "
                "database has to be rebuilt or altered by hand. see ot-021 in the README."
            )
        checked.append(table)
    return checked


def apply(con, dialect="duckdb"):
    """Create the metadata tables if they are not there, then check the shape.

    The check runs after the create so a fresh database passes it trivially and an old one
    fails it loudly. Ordering it the other way would make every first run raise.
    """
    for statement in ddl(dialect):
        con.execute(statement)
    check_shape(con)
    return TABLE_NAMES


# Snowflake accepts PRIMARY KEY and FOREIGN KEY and then does not enforce either one.
# So the constraints above are documentation there and a real guard here. The day-2
# collector has to treat a duplicate grain as its own problem rather than expect the
# warehouse to reject it.
