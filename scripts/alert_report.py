"""Measure what the alerting layer actually does, rather than asserting it in a README.

Same job `baseline_report.py` does for day 3 and `drift_report.py` does for day 4. Every
number the README quotes about alerting comes out of here.

    python scripts/alert_report.py --obs-db /tmp/obs.duckdb --chart docs/alerts.png

Seven sections. Four of them exist because an open thread came due today and the answer
was not the one the thread expected.

    coverage    the three ways a partition can be silent, and which of them are checkable
    cold        the cold start label, and why it does not become a suppression rule
    twoband     the wide volume band against the narrow one, which is ot-017
    pager       which signals are quiet enough to page, measured out of sample
    gaps        signals no band could be fitted for, said once instead of daily
    severity    every partition in the history routed, counted by severity
    windows     what a suppression window costs when it caps instead of deleting
    incidents   alerts against incidents, which is the number an on call rota feels
"""

import argparse
import statistics as st
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from obs import alerting, drift, history  # noqa: E402
from obs.baseline import Baseline  # noqa: E402

WATCHED = ["order_amount_usd", "item_count", "coupon_code", "status", "channel",
           "customer_id"]

# The trailing window ot-017 measured the volume band over. 56 partitions is eight weeks,
# which is eight observations per weekday key. Day 3 recorded that eight per key estimates
# a spread badly and that is exactly why this is the narrow band and not the only band.
NARROW_WINDOW = 56


def fmt(v, places=4):
    return "n/a" if v is None else f"{v:.{places}f}"


def coverage_section(con, dataset, expected):
    print("\n== coverage: the three silences, and which of them the metadata can see ==")
    out = history.coverage(con, dataset, expected_partitions=expected)
    print(f"partitions with at least one run row   {out['partitions_seen']}")
    print(f"successful runs with no dataset metric {len(out['no_dataset_metric'])}")
    print(f"dataset metrics with no column metric  {len(out['no_column_metric'])}")
    if out["never_ran_checked"]:
        print(f"expected partitions that never ran     {len(out['never_ran'])}")
        for key in out["never_ran"][:5]:
            print(f"    {key}")
    else:
        print("expected partitions that never ran     NOT CHECKED, no expected set given")
    print()
    print("the first two are real checks. a run row exists and a metric row does not, so")
    print("the absence is visible from inside the metadata. the third is not, because a")
    print("table of runs has no opinion about runs that did not happen. it needs a list")
    print("of partitions that should exist and that list has to come from outside.")
    return out


def cold_section(con, pipeline, task):
    print("\n== cold: the label ot-018 asked for, and the rule it does not justify ==")
    obs, skipped = history.cold_start_history(con, pipeline, task)
    if not obs:
        print("no duration history")
        return None
    flags = [k for k, _, _ in obs]
    values = [v for _, v, _ in obs]
    cost = alerting.cold_start_cost(obs, flags)
    print(f"runs {cost['runs']}   cold {cost['cold']}   warm {cost['warm']}   "
          f"cold share {fmt(cost['share'], 4)}")
    if skipped:
        print(f"{skipped} rows had an unreadable partition key")

    cold_values = [v for f, v in zip(flags, values) if f]
    warm_values = [v for f, v in zip(flags, values) if not f]
    if cold_values and warm_values:
        print(f"cold median {st.median(cold_values):.0f} ms   "
              f"warm median {st.median(warm_values):.0f} ms   "
              f"ratio {st.median(cold_values) / st.median(warm_values):.1f}x")

    # ask of the cold flag exactly what day 3 asked of the weekday. does it earn a band.
    keyed = [(f, v) for f, v, _ in obs]
    baseline = Baseline.fit(keyed, space="log")
    print(f"bands fitted on the cold flag: {sorted(baseline.bands)}")
    for flag in (True, False):
        verdict = baseline.check(flag, st.median(values))
        print(f"  check(cold={flag}) on the median run -> {verdict.status}")
    print()
    print(f"this is the finding and it is not a good one. one run in {cost['runs']} is")
    print("cold because this history is a backfill inside one process. a daily schedule")
    print("runs every partition in its own process, so every run there is cold and the")
    print("same rule silences the monitor entirely. the flag cannot be banded either,")
    print("because one observation is below the seven a band needs, so the honest")
    print("verdict on a cold run is that nothing at all is known about it.")
    return cost


def two_band_section(con, pipeline, dataset, window):
    print("\n== twoband: ot-017, the wide volume band against the narrow one ==")
    obs, _ = history.volume_history(con, dataset, pipeline)
    if len(obs) < window + 7:
        print("not enough volume history")
        return None
    full = history.keyed(obs)
    recent = history.keyed(history.recent(obs, window))
    wide = Baseline.fit(full)
    narrow = Baseline.fit(recent)

    wide_widths = [b.width for b in wide.bands.values() if not b.degenerate]
    narrow_widths = [b.width for b in narrow.bands.values() if not b.degenerate]
    print(f"wide band   fitted on {len(full):>3} partitions   "
          f"mean width {st.mean(wide_widths):8.1f}")
    print(f"narrow band fitted on {len(recent):>3} partitions   "
          f"mean width {st.mean(narrow_widths):8.1f}")
    print(f"the narrow band is {(1 - st.mean(narrow_widths) / st.mean(wide_widths)) * 100:.0f} "
          "percent tighter, which is the trend the wide one is holding as spread")

    _, wide_rate = wide.fire_rate(recent)
    _, narrow_rate = narrow.fire_rate(recent)
    print(f"\nfire rate over the last {window} partitions")
    print(f"  wide band   {wide_rate:.3f}")
    print(f"  narrow band {narrow_rate:.3f}")

    counts = alerting.two_band_counts(wide, narrow, recent)
    print(f"\nwhere the last {window} partitions land across both bands")
    for status in sorted(counts):
        print(f"  {status:<18}{counts[status]:>4}")
    print()
    print("the middle region is the point. a value there is unusual against recent")
    print("traffic and ordinary against the year. that is a real state and it is not an")
    print("emergency, so it gets a ticket. outside the wide band gets the page. the")
    print("trend that made the wide band too wide is what makes it the right page line.")
    return {"wide": wide, "narrow": narrow, "counts": counts,
            "wide_rate": wide_rate, "narrow_rate": narrow_rate,
            "wide_width": st.mean(wide_widths), "narrow_width": st.mean(narrow_widths)}


HOLDOUT_SPLIT = 0.7


def holdout_fire_rates(observations_by_column, split=HOLDOUT_SPLIT):
    """Fire rate per signal, fitted on the first part of the history and counted on the
    rest.

    The in sample version of this number is worthless for the pager gate and the reason is
    specific. A signal whose history never moved is stored as a constant, and a constant
    cannot fire on the partitions that defined it. Its in sample rate is 0 because of how
    it was built. Every signal the first version of this report let page was a constant,
    so the gate approved all of them for a tautology.

    Splitting fixes the direction of the argument without making the number strong. The
    training half is smaller, so a signal can come out constant here that was banded
    before. That is a real difference and it is reported rather than smoothed over.

    **The first version of this leaked and the leak is worth naming.** It took the bands
    from the training half and the signal values from `signal_series(obs)` over the whole
    history. That function derives its reference from whatever list it is handed, so the
    reference for the held out partitions was an elementwise median that had already seen
    them. Half of the split was held out and half was not. Corrected on 08-05 once
    `Monitor.signals` existed, which is the function that scores a partition against the
    reference stored at fit time.
    """
    rates = {}
    for column, obs in observations_by_column.items():
        cut = int(len(obs) * split)
        train, test = obs[:cut], obs[cut:]
        if len(train) < 14 or not test:
            continue
        monitor = drift.Monitor.fit(column, train)
        scored = [monitor.signals(o) for o in obs]
        series = {name: [row.get(name) for row in scored]
                  for name in {n for row in scored for n in row}}
        for name in monitor.watched():
            values = series.get(name)
            if values is None:
                continue
            pairs = [(o["weekday"], v) for o, v in zip(obs[cut:], values[cut:])
                     if v is not None]
            if not pairs:
                continue
            fired = 0
            for weekday, value in pairs:
                verdict = monitor.check(name, weekday, value)
                if verdict is not None and verdict.status in ("high", "low", "changed"):
                    fired += 1
            rates[(column, name)] = fired / len(pairs)
    return rates


def pager_section(monitors, in_sample, out_sample):
    print("\n== pager: which signals are quiet enough to page ==")
    print(f"a signal firing more often than {alerting.MAX_PAGE_FIRE_RATE:.2f} is held off "
          "the pager whatever the")
    print("policy says. the rate has to be measured out of sample. a constant cannot fire")
    print("on the partitions that defined it, so its in sample rate is zero by")
    print("construction and approving a page on that is a tautology.")
    print(f"{'column':<18}{'signal':<17}{'in':>7}{'out':>8}  {'policy':<8}pages")
    eligible, total, binds, disagree = 0, 0, 0, 0
    for column in sorted(monitors):
        monitor = monitors[column]
        for name in monitor.watched():
            rate_in = in_sample.get((column, name))
            rate_out = out_sample.get((column, name))
            policy = alerting.POLICY.get(("drift", name), alerting.DEFAULT_POLICY)
            loudest = min(policy.values(), key=alerting.rank) if policy else "ticket"
            ok = alerting.page_eligible(rate_out)
            total += 1
            if loudest == "page" and not ok:
                binds += 1
            if ok and loudest == "page":
                eligible += 1
            if (rate_in is not None and rate_out is not None
                    and abs(rate_in - rate_out) > 1e-9):
                disagree += 1
            print(f"{column:<18}{name:<17}{fmt(rate_in, 3):>7}{fmt(rate_out, 3):>8}  "
                  f"{loudest:<8}{'yes' if ok and loudest == 'page' else 'no'}")
    print(f"\n{eligible} of {total} watched signals can reach the pager")
    print(f"the gate refuses {binds} of them, and the two rates disagree on {disagree}")
    if binds == 0:
        print("a gate that refuses nothing is not a safety feature, it is a comment. it")
        print("stays because the policy it guards is a table anyone can edit, and it is")
        print("reported here rather than described so the next reader sees it is idle.")
    return eligible, total, binds


def gaps_section(monitors, observations_by_column):
    print("\n== gaps: signals held but not judged, said once instead of every day ==")
    print("routing these to an info alert produced 238 of 255 alerts on the first run,")
    print("two per partition forever, each one the monitor describing itself. whether a")
    print("band could be fitted is a fact about the monitor and not about the partition.")
    total = 0
    for column in sorted(monitors):
        monitor = monitors[column]
        obs = observations_by_column[column]
        sample = obs[-1]
        series = drift.signal_series(obs)

        def judge(name, sample=sample, series=series, monitor=monitor):
            values = series.get(name)
            if not values or values[-1] is None:
                return None
            return monitor.check(name, sample["weekday"], values[-1])

        gaps = alerting.coverage_gaps(monitor.watched(), judge)
        for name, status in sorted(gaps.items()):
            print(f"  {column:<18}{name:<17}{status}")
            total += 1
    print(f"\n{total} signals are watched and cannot be judged")
    return total


def build_alerts(monitors, series_by_column, observations_by_column, fire_rates):
    """Every partition in the history, run through every monitor it has, turned into
    alerts. This is the volume an on call rota would have actually received."""
    alerts = []
    for column, monitor in monitors.items():
        obs = observations_by_column[column]
        series = series_by_column[column]
        for name in monitor.watched():
            values = series.get(name)
            if values is None:
                continue
            rate = fire_rates.get((column, name))
            for o, value in zip(obs, values):
                if value is None:
                    continue
                verdict = monitor.check(name, o["weekday"], value)
                alert = alerting.raise_alert(
                    "drift", name, verdict, partition=o["date"], fire_rate=rate,
                    subject=f"{column} {name}")
                if alert:
                    alerts.append(alert)
    return alerts


def severity_section(alerts, partitions):
    print("\n== severity: every partition in the history routed ==")
    counts = alerting.counts_by_severity(alerts)
    for level in alerting.SEVERITIES:
        per_partition = counts[level] / partitions if partitions else 0
        print(f"  {level:<8}{counts[level]:>6}   {per_partition:.2f} per partition")
    print(f"  {'total':<8}{len(alerts):>6}")
    return counts


def windows_section(alerts, windows):
    print("\n== windows: what a suppression window costs when it caps instead of deletes ==")
    before = alerting.counts_by_severity(alerts)
    after_alerts = alerting.apply_windows(alerts, windows)
    after = alerting.counts_by_severity(after_alerts)
    capped = [a for a in after_alerts if a.suppressed_by]
    for window in windows:
        print(f"  {window.start} to {window.end}  ceiling {window.ceiling:<7}"
              f"{window.reason}")
    print(f"{'':<10}{'before':>8}{'after':>8}")
    for level in alerting.SEVERITIES:
        print(f"  {level:<8}{before[level]:>8}{after[level]:>8}")
    print(f"\n{len(capped)} alerts were capped. none were removed, which is the whole")
    print("point. an incident during a deploy window still has a record and only stops")
    print("ringing the phone.")
    return before, after, capped


def incidents_section(alerts):
    print("\n== incidents: what a person actually receives ==")
    incidents = alerting.group_incidents(alerts)
    if not incidents:
        print("no alerts at all")
        return incidents
    sizes = [len(i) for i in incidents]
    print(f"{len(alerts)} alerts collapse into {len(incidents)} incidents")
    print(f"largest incident {max(sizes)} alerts, median {st.median(sizes):.0f}")
    paging = [i for i in incidents if i.severity == "page"]
    print(f"{len(paging)} incidents would page")
    worst = max(incidents, key=len)
    print(f"\nthe largest, {worst.partition}, {len(worst)} alerts across "
          f"{', '.join(worst.monitors)}")
    for alert in worst.alerts[:6]:
        print(f"    {alert.severity:<7}{alert.subject:<34}{alert.reason}")
    if len(worst) > 6:
        print(f"    and {len(worst) - 6} more")
    print()
    print(f"the grouping saves {len(alerts) - len(incidents)} messages here, which is not "
          "much of a case for it. this")
    print("feed has no injected failures in it, so nothing has yet moved more than two")
    print("signals at once. the argument for grouping is that one upstream change moves")
    print("several, and that argument is untested until day 6 puts a real failure in.")
    return incidents


def chart(path, two_band, alerts, incidents):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(11, 4))

    ax = axes[0]
    counts = two_band["counts"]
    order = ["inside_both", "between", "outside_both"]
    labels = [o for o in order if o in counts]
    ax.bar(labels, [counts[o] for o in labels], color=["#9ecae1", "#fdae6b", "#de2d26"])
    ax.set_title(f"last {NARROW_WINDOW} partitions across two volume bands")
    ax.set_ylabel("partitions")
    for i, o in enumerate(labels):
        ax.text(i, counts[o], str(counts[o]), ha="center", va="bottom")

    ax = axes[1]
    sev = alerting.counts_by_severity(alerts)
    ax.bar(list(sev), [sev[s] for s in sev], color=["#de2d26", "#fdae6b", "#9ecae1"])
    ax.set_title(f"{len(alerts)} alerts, {len(incidents)} incidents")
    ax.set_ylabel("alerts")
    for i, s in enumerate(sev):
        ax.text(i, sev[s], str(sev[s]), ha="center", va="bottom")

    fig.tight_layout()
    fig.savefig(path, dpi=130)
    print(f"\nchart written to {path}")


def main():
    ap = argparse.ArgumentParser(description="measure the alerting layer")
    ap.add_argument("--obs-db", default="warehouse/obs.duckdb")
    ap.add_argument("--dataset", default="raw_orders")
    ap.add_argument("--pipeline", default="orders")
    ap.add_argument("--task", default="load_raw")
    ap.add_argument("--window", type=int, default=NARROW_WINDOW)
    ap.add_argument("--chart", default=None)
    args = ap.parse_args()

    con = duckdb.connect(args.obs_db, read_only=True)

    volume, _ = history.volume_history(con, args.dataset, args.pipeline)
    expected = None
    if volume:
        first, last = volume[0][2], volume[-1][2]
        expected = [f"dt={(first + timedelta(days=i)).isoformat()}"
                    for i in range((last - first).days + 1)]

    coverage_section(con, args.dataset, expected)
    cold_section(con, args.pipeline, args.task)
    two_band = two_band_section(con, args.pipeline, args.dataset, args.window)

    observations_by_column, series_by_column, monitors, fire_rates = {}, {}, {}, {}
    for column in WATCHED:
        obs, _ = history.column_history(con, args.dataset, column)
        if not obs:
            continue
        observations_by_column[column] = obs
        series_by_column[column] = drift.signal_series(obs)
        monitor = drift.Monitor.fit(column, obs)
        monitors[column] = monitor
        for name in monitor.watched():
            values = series_by_column[column].get(name)
            if values is None:
                continue
            pairs = [(o["weekday"], v) for o, v in zip(obs, values) if v is not None]
            fired = sum(1 for w, v in pairs
                        if monitor.check(name, w, v).status in ("high", "low", "changed"))
            fire_rates[(column, name)] = fired / len(pairs) if pairs else None
    con.close()

    if not monitors:
        print("no column history at all. run scripts/run_observed.py first.")
        return 1

    out_sample = holdout_fire_rates(observations_by_column)
    pager_section(monitors, fire_rates, out_sample)
    gaps_section(monitors, observations_by_column)
    alerts = build_alerts(monitors, series_by_column, observations_by_column, out_sample)
    partitions = len(next(iter(observations_by_column.values())))
    severity_section(alerts, partitions)

    # A window over a real stretch of this history, so the cost is measured rather than
    # described. Two weeks in April, the shape a warehouse migration takes. The ceiling is
    # info rather than ticket because a ticket ceiling caps nothing on a history with no
    # pages in it, and a demonstration that moves no numbers demonstrates nothing.
    windows = [alerting.Window(start=date(2026, 4, 6), end=date(2026, 4, 19),
                               reason="warehouse migration", ceiling="info")]
    windows_section(alerts, windows)
    incidents = incidents_section(alerts)

    if args.chart and two_band:
        chart(args.chart, two_band, alerts, incidents)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
