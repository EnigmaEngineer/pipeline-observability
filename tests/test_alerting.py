"""Tests for the alerting layer.

Two rules from the last week are doing most of the work in here.

From 08-02, a test can pass against unfixed code because the language raises the same
exception the guard would have, so assert the message rather than the type. That is why
the severity tests check which severity came back and not merely that something did.

From 08-02 again, a fixture that exercises one row per group cannot test a rule about
choosing between rows. The window fixture below has a page, a ticket and an info inside
the same window on purpose, because a window that only ever meets one severity cannot
show that it caps rather than deletes.
"""

from datetime import date

from obs import alerting
from obs.baseline import Baseline, Verdict
from tests.tiny import Checks

DAY = date(2026, 4, 10)


def v(status, value=1.0, expected=None, score=None):
    return Verdict(key=None, value=value, status=status, score=score, expected=expected)


def run():
    c = Checks("alerting")

    # rank ordering. the whole file compares severities and "info" < "page" is
    # alphabetically true, which is the bug this exists to make impossible.
    c.ok(alerting.rank("page") < alerting.rank("ticket"), "page outranks ticket")
    c.eq(alerting.cap("page", "ticket"), "ticket", "cap lowers a page to a ticket")
    c.eq(alerting.cap("info", "ticket"), "info", "cap never raises an info")

    # outcome and direction on a constant
    c.eq(alerting.outcome(v("ok")), None, "ok produces no outcome")
    c.eq(alerting.outcome(None), None, "a missing verdict produces no outcome")
    c.eq(alerting.outcome(v("changed", value=5, expected=4)), "changed_up",
         "a constant that rose")
    c.eq(alerting.outcome(v("changed", value=3, expected=4)), "changed_down",
         "a constant that fell")
    c.eq(alerting.outcome(v("changed", value="b", expected=None)), "changed_up",
         "an unorderable constant biases loud")

    # severity routing. the asymmetries are the content, so both directions are asserted
    # rather than one of them.
    c.eq(alerting.severity_for("volume", "row_count", v("low"), fire_rate=0.0), "page",
         "volume falling pages")
    c.eq(alerting.severity_for("volume", "row_count", v("high"), fire_rate=0.0), "ticket",
         "volume rising does not page")
    c.eq(alerting.severity_for("drift", "distinct_count",
                               v("changed", value=3, expected=4), fire_rate=0.0), "page",
         "a lost category pages")
    c.eq(alerting.severity_for("drift", "distinct_count",
                               v("changed", value=5, expected=4), fire_rate=0.0),
         "ticket", "a new category does not page")
    c.eq(alerting.severity_for("drift", "made_up_signal", v("high"), fire_rate=0.0),
         "ticket", "an unknown signal gets the default and not an exception")

    # a monitor that could not judge never wakes anyone, and no policy can promote it
    for status in alerting.CANNOT_JUDGE:
        c.eq(alerting.severity_for("volume", "row_count", v(status), fire_rate=0.0),
             "info", f"{status} cannot be promoted to a page")
        c.eq(alerting.raise_alert("volume", "row_count", v(status), partition=DAY,
                                  fire_rate=0.0), None,
             f"{status} produces no alert at all")

    # the fire rate gate. it can only ever make an alert quieter.
    c.eq(alerting.severity_for("volume", "row_count", v("low"), fire_rate=0.5), "ticket",
         "a noisy signal is held off the pager")
    c.eq(alerting.severity_for("volume", "row_count", v("low"), fire_rate=None), "ticket",
         "an uncounted signal is held off the pager")
    c.eq(alerting.severity_for("drift", "quantile_shift", v("high"), fire_rate=0.0),
         "ticket", "a quiet signal is not promoted by being quiet")
    c.ok(alerting.page_eligible(0.05), "the limit itself is eligible")
    c.ok(not alerting.page_eligible(0.0500001), "just over the limit is not")
    c.ok(not alerting.page_eligible(None), "an unknown rate is not eligible")

    # the demotion has to say so in the reason, otherwise a reader cannot tell a ticket
    # that was always a ticket from a page that was held back
    held = alerting.raise_alert("volume", "row_count", v("low"), partition=DAY,
                                fire_rate=0.9)
    c.eq(held.severity, "ticket", "a held page arrives as a ticket")
    c.ok("held off the pager" in held.reason, "and says it was held")
    clean = alerting.raise_alert("drift", "share_tv", v("high"), partition=DAY,
                                 fire_rate=0.0)
    c.ok("held off the pager" not in clean.reason, "a real ticket does not claim it was")

    # coverage_gaps reports once per signal, not once per partition
    def judge(name):
        return {"a": v("unbanded"), "b": v("ok"), "c": v("unknown_key"),
                "d": None}.get(name)

    gaps = alerting.coverage_gaps(["a", "b", "c", "d"], judge)
    c.eq(sorted(gaps), ["a", "c"], "only the unjudgeable signals are gaps")
    c.eq(gaps["a"], "unbanded", "and the gap carries which kind it was")

    # windows cap rather than delete, and the fixture holds all three severities inside
    # the same window so the difference is visible
    inside = [
        alerting.Alert("volume", "row_count", DAY, "low", "page", "r"),
        alerting.Alert("drift", "share_tv", DAY, "high", "ticket", "r"),
        alerting.Alert("drift", "null_rate", DAY, "high", "info", "r"),
    ]
    outside = [alerting.Alert("volume", "row_count", date(2026, 5, 1), "low", "page", "r")]
    window = alerting.Window(start=date(2026, 4, 1), end=date(2026, 4, 30),
                             reason="migration", ceiling="ticket")
    capped = alerting.apply_windows(inside + outside, [window])
    c.eq(len(capped), 4, "nothing is removed by a window")
    c.eq(capped[0].severity, "ticket", "the page is capped")
    c.eq(capped[0].original_severity, "page", "and remembers what it was")
    c.eq(capped[0].suppressed_by, "migration", "and which window did it")
    c.eq(capped[1].severity, "ticket", "a ticket at the ceiling is untouched")
    c.eq(capped[1].suppressed_by, None, "and is not marked as suppressed")
    c.eq(capped[2].severity, "info", "an info below the ceiling is not raised to it")
    c.eq(capped[3].severity, "page", "an alert outside the window keeps its severity")

    # two overlapping windows. this case was missing and a mutant that made every window
    # apply instead of the first one survived because of it. the quietest ceiling has to
    # win regardless of the order they arrive in, so both orders are asserted.
    loud = alerting.Window(start=date(2026, 4, 1), end=date(2026, 4, 30),
                           reason="migration", ceiling="ticket")
    quiet = alerting.Window(start=date(2026, 4, 5), end=date(2026, 4, 15),
                            reason="full outage", ceiling="info")
    for order, label in (([loud, quiet], "loud first"), ([quiet, loud], "quiet first")):
        both = alerting.apply_windows([inside[0]], order)[0]
        c.eq(both.severity, "info", f"the quietest ceiling wins, {label}")
        c.eq(both.original_severity, "page",
             f"and the true original survives both caps, {label}")
        c.ok("migration" in both.suppressed_by and "full outage" in both.suppressed_by,
             f"both windows are named, {label}")

    # a window scoped to one monitor leaves the others alone
    scoped = alerting.Window(start=date(2026, 4, 1), end=date(2026, 4, 30),
                             reason="drift only", ceiling="info",
                             monitors=frozenset({"drift"}))
    out = alerting.apply_windows(inside, [scoped])
    c.eq(out[0].severity, "page", "a volume page survives a drift scoped window")
    c.eq(out[1].severity, "info", "and the drift ticket is capped")

    # a window with no end on the right day boundary. inclusive, because partitions are
    # whole days and an exclusive end silently leaves the last day of a migration loud.
    edge = alerting.Window(start=DAY, end=DAY, reason="one day", ceiling="info")
    c.ok(edge.covers(inside[0]), "the window covers its own single day")
    c.ok(not edge.covers(outside[0]), "and not the day outside it")
    c.ok(not edge.covers(alerting.Alert("volume", "x", None, "low", "page", "r")),
         "an alert with no partition is never covered")

    # incident grouping
    grouped = alerting.group_incidents(inside + outside)
    c.eq(len(grouped), 2, "two partitions make two incidents")
    c.eq(len(grouped[0]), 3, "and the first holds its three alerts")
    c.eq(grouped[0].severity, "page", "an incident is as loud as its worst alert")
    c.eq(grouped[0].monitors, ["drift", "volume"], "and lists the monitors involved")
    c.eq(alerting.group_incidents([]), [], "no alerts make no incidents")

    counts = alerting.counts_by_severity(inside)
    c.eq(counts["page"], 1, "severity counts are per level")
    c.eq(sum(counts.values()), 3, "and cover everything")

    # the two band scheme. built from one Baseline call each so the losing side cannot be
    # rigged by hand writing it, which is the 08-01 lesson.
    # the first version of this fixture put the two bands 30 apart and asserted that 145
    # fell between them. it did not, it fell inside both, and the test failed rather than
    # passing on a coincidence. the separation has to be large enough that the middle
    # region is unmistakable, so the wide set spreads five times further per step.
    tight = [(None, 100.0 + i * 0.1) for i in range(30)]
    loose = [(None, 100.0 + i * 5.0) for i in range(30)]
    narrow = Baseline.fit(tight, space="raw")
    wide = Baseline.fit(loose, space="raw")
    c.ok(narrow.bands[None].width < wide.bands[None].width,
         "the narrow band really is narrower, checked and not assumed")
    c.eq(alerting.two_band_verdict(wide, narrow, None, 101.0)[0], "inside_both",
         "an ordinary value is inside both")
    c.eq(alerting.two_band_verdict(wide, narrow, None, 5000.0)[0], "outside_both",
         "a wild value is outside both")
    c.eq(alerting.two_band_verdict(wide, narrow, None, 5000.0)[1], "page",
         "and it pages")
    between = alerting.two_band_verdict(wide, narrow, None, 150.0)
    c.eq(between[0], "between", "a value between the two bands has its own status")
    c.eq(between[1], "ticket", "and it is a ticket rather than a page")
    c.eq(alerting.two_band_verdict(wide, narrow, "unseen", 100.0)[0], "unknown_key",
         "an unseen key is passed through rather than guessed at")

    tallies = alerting.two_band_counts(wide, narrow, tight)
    c.eq(sum(tallies.values()), len(tight), "every observation lands somewhere")

    # cold start accounting. the share is the number that decides whether the rule is
    # safe, so it is asserted at both ends rather than only on the realistic one.
    cost = alerting.cold_start_cost([1, 2, 3], [True, False, False])
    c.eq(cost["cold"], 1, "one cold run counted")
    c.eq(round(cost["share"], 4), 0.3333, "and the share reported")
    c.eq(alerting.cold_start_cost([], [])["share"], None,
         "no runs gives no share rather than a zero")
    every = alerting.cold_start_cost([1, 2], [True, True])
    c.eq(every["share"], 1.0, "a schedule where every run is cold reports 1.0")

    return c
