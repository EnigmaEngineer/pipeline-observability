"""Tests for the incident timeline and the freshness check.

The rule doing most of the work here is the one from 08-02 about fixtures. A fixture with
one row per group cannot test a rule about choosing between rows. `last_known_good` picks
one partition out of a history under three competing conditions. So the history below holds
a clean partition, an alerting partition and an unchecked partition. They are arranged so
that taking the wrong one gives a different answer.
"""

from datetime import date, datetime, timedelta

from obs import freshness
from obs import timeline as tl
from obs.alerting import Alert, Incident
from tests.tiny import Checks

D = date(2026, 5, 10)


def alert(monitor="volume", subject="row_count", severity="page", day=D):
    return Alert(monitor=monitor, subject=subject, partition=day, status="low",
                 severity=severity, reason="below the band")


def mkrun(task="load_raw", attempt=1, status="success", ms=11, cold=False,
          error=None):
    return tl.Run(run_id="r", task=task, partition_key="dt=2026-05-10", attempt=attempt,
                  started_at=datetime(2026, 5, 10, 1, 0), ended_at=None, duration_ms=ms,
                  status=status, error=error, code_version="abc123", cold_start=cold)


def entry(alerts=(), checks=1, rows=2000, ms=11):
    return {"alerts": list(alerts), "checks": checks, "row_count": rows,
            "duration_ms": ms}


def run_tests():
    c = Checks("timeline")

    # last known good has to skip an alerting partition and an unchecked one, and the
    # three are ordered so that any of the three wrong answers is a different date.
    # two clean partitions, not one. with a single clean candidate the walk direction
    # cannot be tested, because forwards and backwards land on the same row. a mutant
    # that reversed the sort survived the earlier version of this fixture.
    judged = {
        date(2026, 5, 5): entry(rows=1000),                    # clean, oldest
        date(2026, 5, 6): entry(alerts=[alert()]),             # alerting
        date(2026, 5, 7): entry(rows=3000),                    # clean, most recent
        date(2026, 5, 8): entry(checks=0, rows=9999),          # never checked
        date(2026, 5, 9): entry(alerts=[alert()]),             # alerting
    }
    good = tl.last_known_good(D, judged)
    c.eq(good.state, tl.GOOD_CLEAN, "a clean partition is found")
    c.eq(good.partition, date(2026, 5, 7), "the unchecked and alerting ones are skipped")
    c.eq(good.row_count, 3000, "the reference value comes from the clean partition")
    c.eq(good.searched, 3, "searched counts back from the partition, not forward")
    c.ok(good.usable, "a clean result is usable as a reference")

    # every earlier partition alerted. that is not the same as none being checked and
    # the two must not collapse, because one says the incident is older than this
    # partition and the other says the monitor is the problem.
    all_bad = tl.last_known_good(D, {date(2026, 5, 9): entry(alerts=[alert()])})
    c.eq(all_bad.state, tl.GOOD_ALL_ALERTING, "all earlier partitions alerting is its "
                                              "own state")
    c.ok(not all_bad.usable, "an all alerting result is not usable as a reference")

    never = tl.last_known_good(D, {date(2026, 5, 9): entry(checks=0)})
    c.eq(never.state, tl.GOOD_UNJUDGED, "no earlier partition checked is its own state")
    c.ok(never.state != all_bad.state, "the two empty answers are distinguishable")

    c.eq(tl.last_known_good(D, {}).state, tl.GOOD_NONE, "no history at all")
    c.eq(tl.last_known_good(D, {date(2026, 5, 20): entry()}).state, tl.GOOD_NONE,
         "a later partition is not earlier history")

    # schema facts. an unchanged hash, a changed one and a first partition are three
    # different answers and only the middle one is a lead during an incident.
    current = {"raw_orders": ("aaa", 11, "2026-01-01")}
    same = tl.schema_facts(current, {"raw_orders": ("aaa", 11, "2026-01-01")})
    c.ok(not same[0].changed_here, "an unchanged hash does not flag")
    moved = tl.schema_facts(current, {"raw_orders": ("bbb", 10, "2026-01-01")})
    c.ok(moved[0].changed_here, "a changed hash flags")
    c.eq(moved[0].previous_hash, "bbb", "the previous hash is carried for the reader")
    first = tl.schema_facts(current, None)
    c.ok(not first[0].changed_here,
         "the first partition is not a schema change, it is the first schema")

    # upstream comes from the declared graph. build_daily is fed by load_raw and
    # load_raw is fed by nothing, and inferring either from run order is the mistake
    # the module exists to avoid.
    runs = [mkrun(task="load_raw"), mkrun(task="build_daily")]
    up = tl.upstream_runs(runs)
    c.eq([r.task for r in up], ["load_raw"], "the declared feeder is returned")
    c.eq(tl.upstream_runs([mkrun(task="load_raw")]), [],
         "a task with no declared feeder has no upstream")
    c.eq(tl.upstream_runs(runs, task_graph={"build_daily": [], "load_raw": []}), [],
         "an empty graph produces no upstream, so the graph is really consulted")

    incident = Incident(partition=D, alerts=[alert(), alert(monitor="drift",
                                                          subject="status share_tv",
                                                          severity="ticket")])
    built = tl.assemble(incident, runs, current, {"raw_orders": ("bbb", 10, "x")},
                        judged)
    c.eq(built.severity, "page", "an incident is as urgent as its worst alert")
    c.eq(built.monitors, ["drift", "volume"], "the monitors are listed")
    c.ok(any("schema changed" in n for n in built.notes), "the schema change is noted")
    c.eq(built.last_good.partition, date(2026, 5, 7), "the timeline carries last good")

    failed = tl.assemble(
        Incident(partition=D, alerts=[alert()]),
        [mkrun(status="failed", error="OSError: disk"), mkrun(attempt=2, cold=True)],
        current, current, judged)
    c.ok(any("a run failed here" in n for n in failed.notes), "a failed run is noted")
    c.ok(any("retries" in n for n in failed.notes), "a retry is noted")
    c.ok(any("first of its task" in n for n in failed.notes), "a cold start is noted")

    text = tl.render(built)
    c.ok("what fired" in text and "last known good" in text, "render has its sections")
    c.ok("row_count " in text, "a subject is padded rather than run into its reason")

    # freshness. the partition is the reference and there is nothing to fit.
    lag = freshness.measure(D, datetime(2026, 5, 10, 0, 1), datetime(2026, 5, 10, 23, 9))
    c.ok(lag.clean, "events inside their own day are clean")
    c.eq(lag.status, "ok", "a clean partition reports ok")

    late = freshness.measure(D, datetime(2026, 5, 9, 8, 0), datetime(2026, 5, 10, 23, 0))
    c.eq(late.before, 1, "an event from the previous day is one day early")
    c.eq(late.status, "late_arrival", "that is a late arrival")
    c.ok(not late.clean, "a late arrival is not clean")

    ahead = freshness.measure(D, datetime(2026, 5, 10, 1, 0),
                              datetime(2026, 5, 12, 1, 0))
    c.eq(ahead.after, 2, "an event after the partition is measured separately")
    c.eq(ahead.status, "ahead_of_partition", "and it is a different status")

    # a date rather than a datetime has to work, because the tests hand one shape and
    # duckdb hands the other, and a check that only worked on one would be tested on
    # the wrong type.
    c.ok(freshness.measure(D, date(2026, 5, 10), date(2026, 5, 10)).clean,
         "a plain date is accepted")

    c.eq(freshness.measure(D, None, None).status, "unknown",
         "a partition with no event time is unknown rather than ok")

    # tolerance widens the window in both directions and cannot be negative, because a
    # negative tolerance would reject rows that are correct.
    c.ok(freshness.measure(D, datetime(2026, 5, 9, 8, 0), datetime(2026, 5, 10, 1, 0),
                           tolerance_days=1).clean,
         "a one day tolerance accepts the previous day")
    c.raises_message(ValueError, "negative",
                     lambda: freshness.measure(D, datetime(2026, 5, 10, 1, 0), None,
                                               tolerance_days=-1),
                     "a negative tolerance raises and says why")

    scanned = freshness.scan([
        {"date": D, "event_min": datetime(2026, 5, 9, 1, 0),
         "event_max": datetime(2026, 5, 10, 1, 0)},
        {"date": date(2026, 5, 11), "event_min": datetime(2026, 5, 11, 1, 0),
         "event_max": datetime(2026, 5, 11, 2, 0)},
    ])
    c.eq(len(scanned), 1, "scan returns only the partitions that are not clean")
    c.eq(scanned[0].partition, D, "and it returns the right one")

    # the day-7 clean rate annotation. three subjects on purpose. one measured noisy, one
    # measured quiet and one absent entirely. the rule that matters is that an absent count
    # is not treated as a quiet one, and a fixture holding only a noisy subject would pass
    # whether or not that distinction existed.
    noisy = alert(subject="duration_ms", severity="info")
    quiet = alert(subject="channel null_rate", severity="page")
    unknown = alert(subject="brand new signal", severity="ticket")
    rates = {"duration_ms": (10, 10), "channel null_rate": (1, 10),
             "counted but empty": (0, 0)}
    built = tl.assemble(Incident(partition=D, alerts=[noisy, quiet, unknown]),
                        [mkrun()], {}, {}, {D - timedelta(days=1): entry()},
                        clean_rates=rates)
    c.eq(built.noise_note(noisy), (10, 10), "a measured noisy subject reports its count")
    c.eq(built.noise_note(quiet), (1, 10), "a measured quiet subject reports its count")
    c.eq(built.noise_note(unknown), None, "an unmeasured subject reports nothing")
    c.eq(built.noise_note(alert(subject="counted but empty")), None,
         "zero observations is unmeasured rather than a rate of zero")

    text = tl.render(built)
    c.ok("also fires on 10 of 10 clean" in text, "the noisy line is marked in the render")
    c.ok("also fires on 1 of 10 clean" in text, "and so is the quiet one, with its count")
    c.eq(text.count("also fires on"), 2,
         "the unmeasured subject gets no marker, so a reader cannot read silence as quiet")

    bare = tl.assemble(Incident(partition=D, alerts=[noisy]),
                       [mkrun()], {}, {}, {})
    c.eq(bare.clean_rates, {}, "no rates passed leaves the mapping empty")
    c.ok("also fires on" not in tl.render(bare),
         "and nothing is claimed about a subject nobody counted")

    return c


# the module entrypoint run_all looks for. named apart from the `mkrun` fixture above
# on purpose, because the first version called the fixture `run` and the alias then
# made every fixture call recurse into the test body.
run = run_tests
