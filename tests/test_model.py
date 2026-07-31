"""schema_hash has to notice the changes that break a downstream load.

The interesting case is column order. Sorting the columns before hashing looks tidier
and is wrong, because SELECT * into a positional load cares about order and a reorder is
a real incident. So the hash is order sensitive and this pins it.
"""

import sys
from datetime import datetime

from obs.model import RunRecord, SchemaVersion, schema_hash
from tests.tiny import Checks

BASE = [("order_id", "VARCHAR"), ("amount", "DOUBLE"), ("dt", "DATE")]


def run():
    c = Checks("test_model")

    c.eq(schema_hash(BASE), schema_hash(list(BASE)), "same columns, same hash")

    reordered = [BASE[1], BASE[0], BASE[2]]
    c.ok(schema_hash(reordered) != schema_hash(BASE),
         "reordering columns changes the hash")

    retyped = [("order_id", "VARCHAR"), ("amount", "VARCHAR"), ("dt", "DATE")]
    c.ok(schema_hash(retyped) != schema_hash(BASE), "a type change changes the hash")

    renamed = [("order_ref", "VARCHAR"), ("amount", "DOUBLE"), ("dt", "DATE")]
    c.ok(schema_hash(renamed) != schema_hash(BASE), "a rename changes the hash")

    added = BASE + [("promo_id", "VARCHAR")]
    c.ok(schema_hash(added) != schema_hash(BASE), "a new column changes the hash")

    cased = [("order_id", "varchar"), ("amount", " Double "), ("dt", "DATE")]
    c.eq(schema_hash(cased), schema_hash(BASE),
         "type casing and whitespace are not drift")

    # this one found a real bug. the first hash joined "name:type" with a newline, so a
    # single column named `a:varchar\nb` produced the identical payload to two columns
    # named a and b. two different schemas, one hash, no alert.
    two_columns = schema_hash([("a", "VARCHAR"), ("b", "VARCHAR")])
    forged = schema_hash([("a:varchar\nb", "VARCHAR")])
    c.ok(two_columns != forged, "a column name cannot forge the pair separator")

    version = SchemaVersion.from_columns("raw_orders", BASE, datetime(2026, 7, 31))
    c.eq(version.column_count, 3, "column_count matches")
    c.ok('"name": "order_id"' in version.columns_json, "columns_json keeps names")
    c.ok('"type": "double"' in version.columns_json, "columns_json normalises type case")

    run_rec = RunRecord(run_id="r", pipeline="orders", task="load_raw",
                        partition_key="dt=2026-05-01",
                        started_at=datetime(2026, 7, 31, 10, 0, 0))
    c.eq(run_rec.status, "running", "a fresh run is running")
    run_rec.finish(datetime(2026, 7, 31, 10, 0, 2))
    c.eq(run_rec.duration_ms, 2000, "duration is milliseconds")
    c.eq(run_rec.status, "success", "finishing without an error is a success")

    failed = RunRecord(run_id="r2", pipeline="orders", task="load_raw",
                       partition_key=None,
                       started_at=datetime(2026, 7, 31, 10, 0, 0))
    failed.finish(datetime(2026, 7, 31, 10, 0, 1), error="boom")
    c.eq(failed.status, "failed", "an error makes it a failure")
    c.eq(failed.duration_ms, 1000, "a failed run still records its duration")

    return c


if __name__ == "__main__":
    sys.exit(run().report())
