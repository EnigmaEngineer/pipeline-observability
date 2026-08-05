"""Faults injected into one partition, so the monitors get something real to fail on.

Days 3 to 5 built monitors and measured every one of them against a feed that never
breaks. That answers half the question. A stack that stays quiet on clean data and also
stays quiet on a broken partition is not quiet. It is off.

Each function here takes the events of one partition and returns a new list. Nothing
here touches the pipeline or the collector. The fault goes into the source file and
everything downstream finds out about it the way it would in production.

The set is chosen so that each fault has a different first responder. One belongs to
volume. One belongs to the null rate. Two belong to the categorical share and two to the
numeric quantiles. The last two are here because nothing obviously owns them, which is
the more useful kind of result.

None of these are on by default. `pipeline/generate.py` writes clean partitions and the
day-3 baseline trains on those. The injected days are appended after the clean history
by `scripts/incident_report.py`, so a monitor judging one of them is judging a partition
it has never seen. That is out of sample by construction rather than by promise.
"""

import random
from datetime import timedelta


def _hour_of(event):
    """Hour out of an ISO timestamp written as `YYYY-MM-DD HH:MM:SS`.

    Parsed by slice rather than by `datetime.fromisoformat`. These events are dicts of
    JSON scalars on the way to a file and have not been through a type yet, and turning
    twenty thousand strings into datetimes to read two characters is work nobody asked
    for.
    """
    return int(event["ordered_at"][11:13])


def truncate_at_hour(events, hour=14, **_):
    """The upstream extractor stopped at 14:00 and the file holds a partial day.

    This is the most common real incident of the set. It is also the one the volume
    monitor was built for, so it is the control on the whole exercise. If this is not
    caught then nothing else here means anything.
    """
    return [dict(e) for e in events if _hour_of(e) < hour]


def double_load(events, **_):
    """The export ran twice and the file holds both copies.

    The order ids get a suffix rather than repeating, because a genuine double export
    comes from two runs of a query and carries two sets of keys. Repeating the id would
    also make the fault visible to a uniqueness check this project does not have, which
    would be testing the wrong thing.
    """
    out = [dict(e) for e in events]
    for event in events:
        copy = dict(event)
        copy["order_id"] = f"{event['order_id']}-r2"
        out.append(copy)
    return out


def null_out(events, column="order_amount_usd", share=0.4, seed=11, **_):
    """A share of rows lose a value they always had.

    A join that started missing on the upstream side looks like this. The column is
    still present and still typed and a fraction of it is empty.
    """
    rng = random.Random(seed)
    out = []
    for event in events:
        copy = dict(event)
        if rng.random() < share:
            copy[column] = None
        out.append(copy)
    return out


def drop_category(events, column="status", value="refunded", replacement="placed", **_):
    """A categorical value stops being produced.

    Worth more than the reverse. A vocabulary gaining a value usually means somebody
    shipped a feature. A vocabulary losing one usually means an upstream branch stopped
    running and nobody noticed, because the rows still arrive and still look fine.
    """
    out = []
    for event in events:
        copy = dict(event)
        if copy.get(column) == value:
            copy[column] = replacement
        out.append(copy)
    return out


def new_category(events, column="status", value="chargeback", share=0.05, seed=13, **_):
    """A value nobody has seen appears on a share of rows."""
    rng = random.Random(seed)
    out = []
    for event in events:
        copy = dict(event)
        if rng.random() < share:
            copy[column] = value
        out.append(copy)
    return out


def scale_column(events, column="order_amount_usd", factor=100.0, **_):
    """A currency unit bug. Cents arrive where dollars were expected.

    The whole distribution moves and its shape does not, which is the case a quantile
    vector should be good at. Day 4 proved a seven point vector cannot see a bimodal
    shift. This is the other end of that.
    """
    out = []
    for event in events:
        copy = dict(event)
        if copy.get(column) is not None:
            copy[column] = round(copy[column] * factor, 2)
        out.append(copy)
    return out


def shift_item_count(events, column="item_count", by=1, share=0.5, seed=19, **_):
    """Half the rows gain an item. This is ot-019 given something to be tested against.

    `item_count` is an integer from 1 to 9, so its seven stored quantiles are the same
    seven integers on every clean partition and `quantile_shift` is exactly zero across
    the whole history. The open thread says a change smaller than one whole integer at a
    stored probability is invisible. Adding a whole one is the largest move the column
    can make. If the monitor cannot see this then it cannot see anything on this column
    and the thread has its answer.
    """
    rng = random.Random(seed)
    out = []
    for event in events:
        copy = dict(event)
        if copy.get(column) is not None and rng.random() < share:
            copy[column] = copy[column] + by
        out.append(copy)
    return out


def late_arrival(events, prior_events=None, share=0.25, seed=17, **_):
    """A share of yesterday's events land in today's file. This is ot-015.

    `build_daily` groups on `dt`, the partition the file arrived in, and not on
    `ordered_at`. So these rows are counted on the wrong day and the day they belong to
    has already been built and will not be rebuilt. The pipeline has been correct for
    five days only because the generator never does this.

    The rows carry an `ordered_at` on the previous date, which means the fault is
    visible in `event_time_min` if anything reads it. Whether anything does is the
    question this injection exists to answer.
    """
    if not prior_events:
        return [dict(e) for e in events]
    rng = random.Random(seed)
    late = [dict(e) for e in prior_events if rng.random() < share]
    return [dict(e) for e in events] + late


def drop_column(events, column="channel", **_):
    """The upstream stops sending a column entirely.

    The key is gone from the JSON, not set to null. Those are different bytes on disk
    and the interesting question is whether they are different anywhere downstream,
    given that `pipeline/orders.py` declares its column list rather than inferring it.
    That declaration was a day-1 decision made to stop the loader retyping a column on a
    null heavy day. What it costs is measured in `scripts/incident_report.py`.
    """
    return [{k: v for k, v in event.items() if k != column} for event in events]


def stall_load(events, **_):
    """No change to the data at all.

    Here on purpose. The duration monitor is the one monitor whose subject is not in the
    file, so no edit to the events can move it. Leaving a no-op in the list means the
    report has to say out loud that one of its six monitors is untestable through this
    harness rather than quietly covering five and reporting six.
    """
    return [dict(e) for e in events]


# Each fault, the monitor that should answer for it, and one line on why. `expect` is
# what the design says should happen and the report prints it beside what did. Writing
# the expectation down before the run is the only thing that makes a miss visible. A
# harness that reports whatever fired and calls it detection can never fail.
SCENARIOS = [
    ("truncate", truncate_at_hour, {},
     "volume", "a partial day is fewer rows than any weekday band allows"),
    ("double_load", double_load, {},
     "volume", "twice the rows is above the band on the other side"),
    ("null_flood", null_out, {},
     "drift null_rate", "a column that was never null is null on 40 percent of rows"),
    ("lost_category", drop_category, {},
     "drift share_tv", "status loses a value it has had on all 119 partitions"),
    ("new_category", new_category, {},
     "drift share_tv", "status gains a value it has never had"),
    ("amount_scale", scale_column, {},
     "drift quantile_shift", "every amount is 100x, so the whole vector moves"),
    ("item_shift", shift_item_count, {},
     "drift quantile_shift", "ot-019, the largest move a 1 to 9 integer column can make"),
    ("late_arrival", late_arrival, {},
     "none declared", "ot-015, rows counted on the wrong day. no monitor owns this"),
    ("dropped_column", drop_column, {},
     "schema", "a column disappears upstream and the loader declares its own columns"),
    ("no_change", stall_load, {},
     "none", "the control on the control. nothing changed so nothing should fire"),
]

SCENARIO_NAMES = [name for name, _, _, _, _ in SCENARIOS]


def apply(name, events, prior_events=None):
    """Run one named scenario. Raises on an unknown name rather than returning the
    events unchanged, because a typo that silently injects nothing would show up in the
    report as a monitor that failed to detect something that was never there."""
    for scenario, fn, kwargs, _, _ in SCENARIOS:
        if scenario == name:
            return fn(events, prior_events=prior_events, **kwargs)
    raise ValueError(f"unknown scenario {name!r}, have {SCENARIO_NAMES}")


def previous_day(day):
    return day - timedelta(days=1)
