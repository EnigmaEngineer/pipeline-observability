"""Turn a table, or one partition of it, into the metric rows for one run.

Every column summary comes out of a single SELECT with all the aggregates side by side.
The reason is not the one this file was written to prove. The expectation was that one
scan would beat one query per column. Measured on 254,346 rows it does not. The single
pass takes 79.4 ms and the eleven column loop takes 76.9 ms, because DuckDB is columnar
and a query reading one column never touched the other ten.

What the single pass does buy is a consistent read. Eleven queries against a live table
are eleven snapshots, so a row count and a null count can come from different states of
the world and disagree by an amount nobody can explain afterwards. It is also one round
trip instead of twelve, which is worth nothing locally and worth a great deal against a
warehouse across a network. Those are the reasons it stays. `scripts/profile_cost.py`
reproduces the timing.

Two more rules the collector keeps.

It never writes to the connection it reads from. Profiling takes the pipeline connection
and the metadata writes go to a different one. A bug in here can lose a metric row. It
cannot damage the table it was watching.

Nothing about how a summary was computed is a per-call argument. Two runs summarised
different ways cannot be compared, and the only way to guarantee that never happens is to
make it not a parameter. `QUANTILE_PROBS` in model.py made that choice for quantiles.
`DISTINCT_EXACT` here makes it for distinct counts.
"""

import sys
from dataclasses import dataclass, field

from . import store
from .model import (
    QUANTILE_PROBS,
    ColumnMetric,
    DatasetMetric,
    SchemaVersion,
    now_utc,
)

# Exact, not HyperLogLog. approx_count_distinct is 1.14x faster on this table and its
# error is nothing like the small wobble the name suggests. Measured against exact counts
# on raw_orders it was 25 percent wrong on `status`, a column with four values that came
# back as three. A monitor watching for a new status value would have been blind to the
# one thing it exists to catch. 1.14x does not buy that.
DISTINCT_EXACT = True

# Quantiles are the most expensive thing in the single pass, 31.4 ms of 79.4, and
# approx_quantile was 2.57x faster at a worst error of 0.27 percent on the day-2 run.
# It is still not used.
# The estimator is a t-digest and it depends on the order rows arrive in. Reading the same
# 254,346 rows in a different physical order moved p05 by 0.35 percent with the data
# unchanged. A drift check built on that starts with a noise floor it did not choose. At a
# thousand times this volume the answer flips and the tradeoff has to be revisited.

# Above this many distinct values a top-k list stops being a summary of the column and
# starts being a sample of it. Categorical columns here sit at 4 to 6 values, so 50 is
# not a tuned number, it is far enough above them that a column crossing it has changed
# character rather than drifted.
TOP_VALUES_MAX_DISTINCT = 50
TOP_VALUES_K = 5

_NUMERIC = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
    "FLOAT", "REAL", "DOUBLE",
}
_TEXTLIKE = {"VARCHAR", "CHAR", "TEXT", "STRING", "BOOLEAN"}


def is_numeric(data_type):
    t = data_type.upper()
    return t in _NUMERIC or t.startswith("DECIMAL") or t.startswith("NUMERIC")


def is_textlike(data_type):
    return data_type.upper() in _TEXTLIKE


def quote(name):
    """Quote an identifier. Column names come from information_schema here rather than
    from a user, but the collector is the one part of this that runs against whatever
    table it is pointed at, so it does not get to assume they are tidy."""
    return '"' + name.replace('"', '""') + '"'


@dataclass
class Profile:
    version: SchemaVersion
    dataset_metric: DatasetMetric
    column_metrics: list = field(default_factory=list)
    # queries is here because the number of them is a design property worth pinning in a
    # test, not because anything displays it.
    queries: int = 0


def column_aggs(columns, exact_distinct=DISTINCT_EXACT):
    """Six aggregate expressions per column, always in the same order and always the same
    count, so the flat result row can be sliced back apart by position.

    Non numeric columns get typed NULL placeholders for mean and quantiles rather than
    being skipped, because a ragged row would make the slicing depend on the types.
    """
    distinct = "count(DISTINCT {c})" if exact_distinct else "approx_count_distinct({c})"
    probs = "[" + ", ".join(str(p) for p in QUANTILE_PROBS) + "]"
    out = []
    for name, dtype in columns:
        c = quote(name)
        out.append(f"count({c})")
        out.append(distinct.format(c=c))
        out.append(f"CAST(min({c}) AS VARCHAR)")
        out.append(f"CAST(max({c}) AS VARCHAR)")
        if is_numeric(dtype):
            out.append(f"avg({c})")
            out.append(f"quantile_cont({c}, {probs})")
        else:
            out.append("CAST(NULL AS DOUBLE)")
            out.append("CAST(NULL AS DOUBLE[])")
    return out


AGGS_PER_COLUMN = 6


def _where(clause, extra=None):
    parts = [p for p in (clause, extra) if p]
    return " WHERE " + " AND ".join(f"({p})" for p in parts) if parts else ""


def profile_sql(dataset, columns, where=None, event_time_column=None,
                exact_distinct=DISTINCT_EXACT):
    parts = ["count(*)"] + column_aggs(columns, exact_distinct)
    if event_time_column:
        e = quote(event_time_column)
        parts += [f"min({e})", f"max({e})"]
    return f"SELECT {', '.join(parts)} FROM {quote(dataset)}{_where(where)}"


def top_values(con, dataset, column, where=None, params=None, k=TOP_VALUES_K):
    c = quote(column)
    rows = con.execute(
        f"SELECT CAST({c} AS VARCHAR), count(*) FROM {quote(dataset)}"
        f"{_where(where, f'{c} IS NOT NULL')}"
        # the second sort key is not decoration. two values on the same count would
        # otherwise swap places between runs and read as drift.
        f" GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT {int(k)}",
        list(params or []),
    ).fetchall()
    return {value: count for value, count in rows}


def profile(con, dataset, run_id, where=None, params=None, collected_at=None,
            event_time_column=None, byte_size=None):
    """Read `dataset` through `con` and build the rows for one run.

    `where` is SQL text with `?` placeholders and `params` fills them. The same pair is
    reused for every query in here, so a partition profile really is the partition and
    not the table.
    """
    collected_at = collected_at or now_utc()
    params = list(params or [])
    columns = store.columns_of(con, dataset)
    if not columns:
        raise ValueError(f"no such table {dataset!r}, or it has no columns")

    sql = profile_sql(dataset, columns, where, event_time_column)
    # the params list is repeated once per query in the statement. duckdb binds
    # positionally and the WHERE clause appears once, so one copy is right here.
    row = con.execute(sql, params).fetchone()
    queries = 1

    rows = row[0]
    metrics = []
    for i, (name, dtype) in enumerate(columns):
        base = 1 + i * AGGS_PER_COLUMN
        non_null, distinct, lo, hi, mean, quantiles = row[base:base + AGGS_PER_COLUMN]
        metrics.append(ColumnMetric(
            run_id=run_id,
            dataset=dataset,
            column_name=name,
            data_type=dtype,
            null_count=rows - non_null,
            distinct_count=distinct,
            min_value=lo,
            max_value=hi,
            mean_value=None if mean is None else float(mean),
            quantiles=None if quantiles is None else {
                str(p): float(v) for p, v in zip(QUANTILE_PROBS, quantiles)
            },
        ))

    # top values needs a GROUP BY, so it cannot ride along in the single pass. It is
    # asked for only where it is cheap and only where it means something, which is a
    # text column that has already been counted and come back small.
    for m in metrics:
        if (is_textlike(m.data_type) and m.distinct_count is not None
                and 0 < m.distinct_count <= TOP_VALUES_MAX_DISTINCT):
            m.top_values = top_values(con, dataset, m.column_name, where, params)
            queries += 1

    event_min = event_max = None
    if event_time_column:
        event_min, event_max = row[-2], row[-1]

    version = SchemaVersion.from_columns(dataset, columns, collected_at)
    dataset_metric = DatasetMetric(
        run_id=run_id,
        dataset=dataset,
        schema_hash=version.schema_hash,
        row_count=rows,
        byte_size=byte_size,
        event_time_min=event_min,
        event_time_max=event_max,
        collected_at=collected_at,
    )
    return Profile(
        version=version,
        dataset_metric=dataset_metric,
        column_metrics=metrics,
        queries=queries,
    )


def write(obs_con, profile_result):
    """Write a profile to the metadata tables. Separate from profile() so a caller can
    look at what was measured before it lands, which the tests do."""
    store.upsert_schema_version(obs_con, profile_result.version)
    store.insert_dataset_metric(obs_con, profile_result.dataset_metric)
    return store.insert_column_metrics(obs_con, profile_result.column_metrics)


def collect_into(obs_con, con, run_id, dataset, **kwargs):
    """Profile and write, and swallow anything that goes wrong.

    Deliberate. The pipeline does not fail because the thing watching it failed. What is
    left behind is a run row with no dataset metric, and day 5 treats a successful run
    that produced no metrics as its own alert. That gap only exists because the run row
    is written before the work rather than after, which the tracker guarantees and the
    foreign key on obs_dataset_metric enforces. The weakness of that is real and it is in
    the README: a missing row cannot tell you whether the collector broke or was never
    pointed at this dataset in the first place.
    """
    try:
        result = profile(con, dataset, run_id, **kwargs)
        write(obs_con, result)
        return result
    except Exception as exc:
        print(f"collector failed on {dataset} for run {run_id}: "
              f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return None
