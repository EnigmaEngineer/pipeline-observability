"""Whether the events in a partition belong to the day the partition is named after.

This module exists because of a measurement and not because of a plan. The day-6
injection harness put a late arrival fault through the stack and all five monitors that
existed at the time passed it. `event_time_min` and `event_time_max` have been collected
on every run since day 2 and until today nothing read them. Six days of a monitoring
project storing a field no monitor consults.

`build_daily` groups on `dt`, the partition the file landed in. It does not group on
`ordered_at`, which is when the event actually happened. So a row that happened on the 3rd and
arrived in the 4th's file is counted on the 4th, and the 3rd has already been built and
will not be rebuilt. That is ot-015, open since day 1, and this is the half of it that
can be answered from run metadata.

No band here and no history. The rule is known without a spread. A partition named
`dt=2026-05-01` should hold events from 2026-05-01, and any event outside that range is
wrong by definition rather than unusual by degree. Day 4 established that a signal whose
reference point is known should be held as a constant and not banded, and this is the
same argument one step further. There is nothing to fit at all.

What this cannot do is fix the pipeline. Detecting that rows landed on the wrong day is a
different job from restating the day they belong to. A restatement window is a real
design decision with a cost and it is named in the README limitations rather than
guessed at here.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional


@dataclass
class Lag:
    """How far outside its own partition a batch of events reaches.

    `before` and `after` are whole days, both non negative. They are kept apart because
    they mean different things. Events from before the partition are late arrivals and
    the day they belong to is already wrong. Events from after it are a clock problem or
    a partition written under the wrong name, which is rarer and worse.
    """
    partition: date
    event_min: Optional[datetime]
    event_max: Optional[datetime]
    before: int = 0
    after: int = 0

    @property
    def clean(self):
        return self.before == 0 and self.after == 0

    @property
    def status(self):
        if self.event_min is None:
            return "unknown"
        if self.clean:
            return "ok"
        return "late_arrival" if self.before else "ahead_of_partition"


def _as_date(value):
    """Accept a datetime or a date. DuckDB hands back datetimes and the tests hand back
    dates, and a check that only worked on one of those would be tested on the wrong
    type."""
    if value is None:
        return None
    return value.date() if isinstance(value, datetime) else value


def measure(partition, event_min, event_max, tolerance_days=0):
    """How far the event times reach outside the partition they were stored under.

    `tolerance_days` is here for a pipeline whose partitions are not aligned to the event
    clock, for example a UTC partition over local timestamps. It defaults to 0 because
    this pipeline writes both in naive UTC and a tolerance nobody needs is a place for a
    real fault to hide.
    """
    if tolerance_days < 0:
        raise ValueError("tolerance_days cannot be negative, it would reject valid rows")
    lo = _as_date(event_min)
    hi = _as_date(event_max)
    if lo is None:
        return Lag(partition=partition, event_min=None, event_max=None)
    window = timedelta(days=tolerance_days)
    before = (partition - window - lo).days
    after = (hi - (partition + window)).days if hi is not None else 0
    return Lag(partition=partition, event_min=event_min, event_max=event_max,
               before=max(0, before), after=max(0, after))


def check(observation, tolerance_days=0):
    """Measure one observation from `history.event_time_history`."""
    return measure(observation["date"], observation["event_min"],
                   observation["event_max"], tolerance_days)


def scan(observations, tolerance_days=0):
    """Every partition measured. Returns the lags that are not clean, in date order.

    Returns only the failures because this is the one monitor in the project whose clean
    answer carries no information. A band reports a score even when it passes and that
    score is worth plotting. A partition whose events fall inside their own day has
    nothing more to say about itself.
    """
    return [lag for lag in (check(o, tolerance_days) for o in observations)
            if not lag.clean and lag.status != "unknown"]
