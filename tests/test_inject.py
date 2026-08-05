"""Tests for the fault injections.

An injection that does not change the thing it claims to change turns the whole day-6
harness into a report about nothing. A monitor that fails to detect a fault that was
never injected reads exactly like a monitor that missed a real one, and there is no way
to tell them apart from the detection table. So every scenario here is asserted to have
actually done something, and to have done the specific thing its name says.

This is the 07-29 rule about reverting a fix and confirming the test fails, pointed at
the fixture instead of at the code. The fixture is the part that can silently be a no-op.
"""

from datetime import date

from pipeline import inject
from tests.tiny import Checks

DAY = date(2026, 5, 1)
PRIOR = date(2026, 4, 30)


def events(n=40, hour=9, status="placed", amount=10.0, item_count=2, day=DAY):
    out = []
    for i in range(n):
        out.append({
            "order_id": f"{day.strftime('%Y%m%d')}-{i:06d}",
            "customer_id": f"c{i:05d}",
            "ordered_at": f"{day.isoformat()} {hour:02d}:00:00",
            "channel": "web",
            "country": "US",
            "item_count": item_count,
            "order_amount_usd": amount,
            "coupon_code": None,
            "status": status,
        })
    return out


def varied(n=40, day=DAY):
    """A partition holding every condition the scenarios need to bite on.

    The first version of this was forty identical morning rows all marked `placed`,
    which meant `truncate` had no afternoon to cut and `drop_category` had no refunds to
    drop. Both returned the input unchanged and the loop below caught them. That is the
    08-02 fixture lesson landing on a fixture rather than on a rule. A partition with one
    kind of row in it cannot test an injection that selects rows.
    """
    out = []
    for i in range(n):
        hour = 9 if i % 2 else 16
        status = "refunded" if i % 5 == 0 else "placed"
        out.extend(events(1, hour=hour, status=status, day=day))
        out[-1]["order_id"] = f"{day.strftime('%Y%m%d')}-{i:06d}"
    return out


def run():
    c = Checks("inject")

    # every scenario has to change something. a no-op injection and a missed detection
    # look identical in the report, so this is the assertion the report leans on.
    base = varied(40)
    prior = varied(20, day=PRIOR)
    for name, _fn, _kw, _owner, _why in inject.SCENARIOS:
        out = inject.apply(name, base, prior)
        changed = out != base
        if name == "no_change":
            c.ok(not changed, "no_change leaves the events alone")
        else:
            c.ok(changed, f"{name} actually changes the partition")

    c.raises(ValueError, lambda: inject.apply("not_a_scenario", base, prior),
             "an unknown scenario name raises instead of injecting nothing")

    # truncate keeps the morning and drops the afternoon
    mixed = events(10, hour=9) + events(10, hour=16)
    cut = inject.truncate_at_hour(mixed, hour=14)
    c.eq(len(cut), 10, "truncate keeps only the events before the cutoff")
    c.ok(all(int(e["ordered_at"][11:13]) < 14 for e in cut),
         "nothing after the cutoff survives truncate")

    # double_load doubles the rows and keeps the ids unique, because a repeated id would
    # be caught by a uniqueness check this project does not have, which would be a
    # different test passing for a different reason.
    doubled = inject.double_load(base)
    c.eq(len(doubled), 80, "double_load doubles the row count")
    c.eq(len({e["order_id"] for e in doubled}), 80, "double_load keeps order ids unique")

    # null_out nulls some rows and not all of them. a share of 1.0 would make this pass
    # while testing nothing about the share.
    nulled = inject.null_out(base, column="order_amount_usd", share=0.4, seed=11)
    missing = sum(1 for e in nulled if e["order_amount_usd"] is None)
    c.ok(0 < missing < len(base), f"null_out nulls some rows and not all, got {missing}")
    c.eq(len(nulled), len(base), "null_out does not change the row count")

    # drop_category removes a value entirely, gain adds one that was not there
    with_refunds = events(10, status="refunded") + events(10, status="placed")
    dropped = inject.drop_category(with_refunds, column="status", value="refunded",
                                   replacement="placed")
    c.ok("refunded" not in {e["status"] for e in dropped},
         "drop_category removes every instance of the value")
    c.eq(len(dropped), 20, "drop_category keeps the row count")

    gained = inject.new_category(base, column="status", value="chargeback", share=0.5,
                                 seed=13)
    values = {e["status"] for e in gained}
    c.ok("chargeback" in values, "new_category introduces the new value")
    c.ok("placed" in values, "new_category leaves some rows on the old value")

    # scale_column moves every value and leaves nulls alone, because multiplying None
    # would raise and the fault is meant to reach the monitors rather than the loader.
    scaled = inject.scale_column(events(5, amount=3.0) + [dict(events(1)[0],
                                                              order_amount_usd=None)],
                                 column="order_amount_usd", factor=100.0)
    c.eq(scaled[0]["order_amount_usd"], 300.0, "scale_column multiplies the value")
    c.eq(scaled[-1]["order_amount_usd"], None, "scale_column leaves a null alone")

    shifted = inject.shift_item_count(base, by=1, share=1.0, seed=19)
    c.eq({e["item_count"] for e in shifted}, {3}, "shift_item_count adds a whole integer")

    # late arrival carries an ordered_at on the previous day. that is the only thing
    # about it any monitor can see, so it is the thing worth asserting.
    late = inject.late_arrival(base, prior_events=prior, share=1.0, seed=17)
    c.eq(len(late), 60, "late_arrival appends the prior events")
    early = [e for e in late if e["ordered_at"].startswith(PRIOR.isoformat())]
    c.eq(len(early), 20, "the appended rows carry the previous day's event time")

    # with no prior events there is nothing to be late, and the function must not
    # invent any. this is the branch that would otherwise silently inject nothing.
    c.eq(inject.late_arrival(base, prior_events=None), base,
         "late_arrival with no prior events changes nothing")

    without = inject.drop_column(base, column="channel")
    c.ok(all("channel" not in e for e in without), "drop_column removes the key")
    c.ok(all("country" in e for e in without), "drop_column leaves other keys alone")

    # the originals are never mutated. the harness injects into a list it also uses for
    # the control arm, and an in place edit would quietly make both arms identical.
    c.eq(len(base), 40, "the input list is not resized by any injection")
    c.eq(base[0]["item_count"], 2, "the input events are not mutated in place")
    c.eq(base[0]["status"], "refunded", "the input statuses are not mutated in place")

    return c
