"""Source events for the pipeline this project instruments.

There is no real order stream to point at, so one gets generated. That creates a trap
worth naming here rather than in a footnote. Day 3 of this project builds a seasonal
baseline for volume. If the generator lays down a clean weekly pattern and the baseline
then learns that pattern, nothing has been demonstrated. The model recovered a shape it
was handed.

Two things keep it honest.

The generator does not use the form the baseline will use. Volume here is a
multiplicative day-of-week factor times a slow trend times lognormal noise. A baseline
that assumes additive weekday offsets on the raw counts will be wrong in a way that
shows up in the residuals.

The day-3 test is not fit quality. It is whether the baseline stays quiet through the
nuisances a real feed has. Those go in on day 6 as injected failures.

Determinism is per day, not per run. Regenerating a single date gives byte-identical
output whether or not its neighbours were regenerated, which matters because backfilling
one partition is the most common thing anyone does to a pipeline.
"""

import argparse
import hashlib
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# Measured from nothing. These are a plausible retail shape, not observed traffic, and
# the README says so. Friday peaks, the weekend drops off hard.
DOW_FACTOR = [1.05, 1.00, 1.02, 1.06, 1.15, 0.78, 0.72]  # Monday first

# Orders cluster around lunch and again after work. Weights per hour, 0 to 23.
HOUR_WEIGHT = [
    2, 1, 1, 1, 1, 2, 4, 7, 10, 12, 13, 15,
    18, 16, 13, 12, 13, 16, 19, 18, 14, 10, 6, 3,
]

CHANNELS = [("web", 0.46), ("ios", 0.24), ("android", 0.21), ("affiliate", 0.09)]
COUNTRIES = [("US", 0.58), ("GB", 0.13), ("CA", 0.09), ("DE", 0.08),
             ("AU", 0.07), ("IN", 0.05)]
STATUSES = [("placed", 0.86), ("cancelled", 0.06), ("refunded", 0.04),
            ("pending_payment", 0.04)]

COUPON_CODES = ["SAVE10", "WELCOME", "FREESHIP", "BF25", "LOYAL5"]
COUPON_RATE = 0.22  # so roughly four in five rows have a null coupon_code

DAILY_GROWTH = 0.0015


def _rng_for(seed, day):
    """One generator per day so a single partition can be rebuilt on its own."""
    key = f"{seed}:{day.isoformat()}".encode("utf-8")
    return random.Random(int(hashlib.sha256(key).hexdigest()[:16], 16))


def _pick(rng, weighted):
    r = rng.random()
    acc = 0.0
    for value, share in weighted:
        acc += share
        if r <= acc:
            return value
    return weighted[-1][0]


def volume_for(day, start, base, rng):
    trend = (1 + DAILY_GROWTH) ** (day - start).days
    noise = rng.lognormvariate(0, 0.07)
    return max(1, int(round(base * DOW_FACTOR[day.weekday()] * trend * noise)))


def events_for_day(day, start, base=2000, seed=7):
    rng = _rng_for(seed, day)
    n = volume_for(day, start, base, rng)
    hours = list(range(24))
    for i in range(n):
        hour = rng.choices(hours, weights=HOUR_WEIGHT, k=1)[0]
        ordered_at = datetime(day.year, day.month, day.day, hour,
                              rng.randrange(60), rng.randrange(60))
        amount = round(rng.lognormvariate(3.75, 0.62), 2)
        yield {
            "order_id": f"{day.strftime('%Y%m%d')}-{i:06d}",
            "customer_id": f"c{rng.randrange(1, 90000):05d}",
            "ordered_at": ordered_at.isoformat(sep=" "),
            "channel": _pick(rng, CHANNELS),
            "country": _pick(rng, COUNTRIES),
            "item_count": min(9, 1 + int(rng.expovariate(0.9))),
            "order_amount_usd": amount,
            "coupon_code": rng.choice(COUPON_CODES) if rng.random() < COUPON_RATE else None,
            "status": _pick(rng, STATUSES),
        }


def write_day(out_root: Path, day: date, start: date, base=2000, seed=7):
    part = out_root / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    target = part / "orders.jsonl"
    rows = list(events_for_day(day, start, base, seed))
    with target.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    return len(rows)


def daterange(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def main():
    ap = argparse.ArgumentParser(description="write partitioned order events")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--base", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    out_root = Path(args.out)

    total = 0
    days = 0
    for day in daterange(start, end):
        total += write_day(out_root, day, start, args.base, args.seed)
        days += 1
    print(f"wrote {total} events across {days} partitions to {out_root}")


# TODO(day 6): injected failures live here. A partial day where the feed stops at 14:00,
# a double load of one partition, and a new column appearing mid-history. Keep them off
# by default so the baseline in day 3 trains on clean history.

if __name__ == "__main__":
    main()
