"""Records that map one to one onto the metadata tables.

The point of these is that the day-2 collector builds objects and the store writes them,
so a column added to the schema fails at the dataclass instead of silently landing in the
wrong position of a tuple.
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def now_utc():
    """Naive UTC, on purpose.

    DuckDB TIMESTAMP and Snowflake TIMESTAMP_NTZ are both wall clock with no zone, so
    storing an aware datetime means the zone is dropped somewhere you cannot see. Dropping
    it here, in one place, is the version you can reason about. `datetime.utcnow` would do
    the same thing and is deprecated from 3.12.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def columns_json(columns):
    """Canonical text for an ordered list of (name, type) pairs.

    Types are lowercased and stripped because DuckDB and Snowflake disagree on casing for
    the same type and that difference is noise, not drift.
    """
    return json.dumps([{"name": n, "type": str(t).strip().lower()} for n, t in columns])


def schema_hash(columns):
    """Hash the canonical text. Same input as the column list that gets stored.

    Order matters. Two tables with the same columns in a different order are not the same
    table to a consumer doing SELECT *, and a reorder is the kind of change that breaks a
    downstream load quietly. So this is deliberately order sensitive rather than sorted.

    The first version of this joined "name:type" pairs with a newline, which a column
    named `a:varchar\\nb` could forge. Two different schemas hashed to the same value.
    Hashing the JSON instead removes the whole class of problem, because JSON escapes the
    separators, and it has the side benefit that the hash and the stored columns_json can
    never disagree about what was hashed.
    """
    return hashlib.sha256(columns_json(columns).encode("utf-8")).hexdigest()[:16]


@dataclass
class RunRecord:
    run_id: str
    pipeline: str
    task: str
    partition_key: Optional[str]
    started_at: datetime
    status: str = "running"
    attempt: int = 1
    ended_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    code_version: Optional[str] = None
    triggered_by: str = "schedule"
    cold_start: bool = False

    def finish(self, ended_at, error=None, work_began=None):
        """Close the run out.

        `work_began` is when the task itself started, which is not the same moment as
        `started_at`. `started_at` is when the tracker was entered and it is what
        `stale_runs` compares against, so it has to stay where it is. The duration is
        measured from `work_began` because everything between the two is the tracker
        looking up an attempt number and inserting a row.

        Measured on 08-05 at 2.8 ms against a recorded median of 30, so 9 percent of
        every duration this project stored before today was its own observability. Day 2
        moved the profiling queries out of the tracked block for exactly this reason and
        left the run row insert inside it. Durations recorded before this change are not
        comparable with ones recorded after, which is why the history gets rebuilt rather
        than appended to.
        """
        self.ended_at = ended_at
        began = work_began or self.started_at
        self.duration_ms = int((ended_at - began).total_seconds() * 1000)
        self.status = "failed" if error else "success"
        self.error = error
        return self


@dataclass
class SchemaVersion:
    schema_hash: str
    dataset: str
    columns_json: str
    column_count: int
    first_seen_at: datetime

    @classmethod
    def from_columns(cls, dataset, columns, seen_at):
        return cls(
            schema_hash=schema_hash(columns),
            dataset=dataset,
            columns_json=columns_json(columns),
            column_count=len(columns),
            first_seen_at=seen_at,
        )


@dataclass
class DatasetMetric:
    run_id: str
    dataset: str
    schema_hash: str
    row_count: int
    collected_at: datetime
    byte_size: Optional[int] = None
    event_time_min: Optional[datetime] = None
    event_time_max: Optional[datetime] = None


@dataclass
class ColumnMetric:
    run_id: str
    dataset: str
    column_name: str
    data_type: str
    null_count: int
    distinct_count: Optional[int] = None
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    mean_value: Optional[float] = None
    quantiles: Optional[dict] = field(default=None)
    top_values: Optional[dict] = field(default=None)

    def quantiles_json(self):
        return None if self.quantiles is None else json.dumps(self.quantiles)

    def top_values_json(self):
        return None if self.top_values is None else json.dumps(self.top_values)


# The probabilities the day-4 drift check will compare on. Fixed here rather than passed
# in, because two runs summarised at different probabilities cannot be compared and the
# only way to guarantee they are not is to make it not a parameter. Nothing reads this
# yet. It is here so day 2 and day 4 cannot each pick their own.
QUANTILE_PROBS = (0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99)
