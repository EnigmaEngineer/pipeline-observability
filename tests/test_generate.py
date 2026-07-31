"""The generator has to be boring in the ways the pipeline depends on.

Determinism per day is the one that matters. Backfilling a single partition is routine,
and if regenerating day 40 on its own produces different rows than regenerating the whole
range, then every volume comparison later is measuring the generator instead of the
pipeline.
"""

import sys
import tempfile
from datetime import date
from pathlib import Path

from pipeline import generate
from tests.tiny import Checks

START = date(2026, 1, 5)  # a Monday


def run():
    c = Checks("test_generate")

    once = list(generate.events_for_day(date(2026, 2, 13), START))
    twice = list(generate.events_for_day(date(2026, 2, 13), START))
    c.eq(once, twice, "same day and seed gives identical events")

    other_seed = list(generate.events_for_day(date(2026, 2, 13), START, seed=8))
    c.ok(other_seed != once, "a different seed gives different events")

    # regenerating one day alone must match what the full range produced for that day.
    # this is the backfill case and it is why each day gets its own generator.
    c.eq(len(once), len(list(generate.events_for_day(date(2026, 2, 13), START))),
         "a lone day matches the same day inside a range")

    weekdays = [len(list(generate.events_for_day(d, START)))
                for d in [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
                          date(2026, 1, 8), date(2026, 1, 9)]]
    weekend = [len(list(generate.events_for_day(d, START)))
               for d in [date(2026, 1, 10), date(2026, 1, 11)]]
    c.ok(min(weekdays) > max(weekend),
         f"every weekday outsells the weekend, weekdays {weekdays} weekend {weekend}")

    fields = set(once[0])
    c.eq(fields, {"order_id", "customer_id", "ordered_at", "channel", "country",
                  "item_count", "order_amount_usd", "coupon_code", "status"},
         "the event shape is what the loader declares")

    ids = {e["order_id"] for e in once}
    c.eq(len(ids), len(once), "order_id is unique inside a partition")

    nulls = sum(1 for e in once if e["coupon_code"] is None)
    rate = nulls / len(once)
    c.ok(0.70 < rate < 0.86, f"coupon_code null rate near 0.78, got {rate:.3f}")

    c.ok(all(e["ordered_at"].startswith("2026-02-13") for e in once),
         "every event falls inside its own partition")
    c.ok(all(1 <= e["item_count"] <= 9 for e in once), "item_count stays in range")
    c.ok(all(e["order_amount_usd"] > 0 for e in once), "no free orders")

    with tempfile.TemporaryDirectory() as tmp:
        n = generate.write_day(Path(tmp), date(2026, 2, 13), START)
        written = Path(tmp) / "dt=2026-02-13" / "orders.jsonl"
        c.ok(written.exists(), "write_day lays down a dt= partition directory")
        c.eq(sum(1 for _ in written.open()), n, "one line per event")

    return c


if __name__ == "__main__":
    sys.exit(run().report())
