"""Three incidents walked end to end, the way a person on call would meet them.

    python scripts/run_observed.py --start 2026-01-01 --end 2026-04-29 \
        --db /tmp/wh.duckdb --obs-db /tmp/obs.duckdb --quiet
    python scripts/worked_incidents.py --obs-db /tmp/obs.duckdb --db /tmp/wh.duckdb

`incident_report.py` counts detections across all ten faults. That answers "does the stack
catch things". It does not answer the question the README needed for day 7, which is what a
person actually does with one alert at 3am. These three are picked because they fail in
three different ways and the middle one is the uncomfortable one.

    truncate         the volume monitor's home case. it works.
    dropped_column   the monitor named for it cannot see it. another one catches it.
    late_arrival     detected at page severity, and this repo cannot repair the damage.

Two things get applied here that `incident_report.py` does not apply, and both came out of
writing these three up.

**A subject that fires on every clean partition is quarantined.** `duration_ms` fired on 10
of 10 clean out of sample partitions on day 6, at `info`, which put a line carrying no
information on every incident view in the project. `alerting.quarantine` holds it out of the
stream and states it once. That is the day-7 answer to `ot-023` and it is a containment
rather than a fix.

**Every surviving alert carries how often its subject fires on clean data.** Three of the
five lines on the truncate timeline fire on ordinary days too. The control arm has known
that since day 6 and the timeline was never told, so the reader was left to invent a
ranking. `timeline.assemble` now takes those counts.
"""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from obs import alerting, history, store, timeline as tl  # noqa: E402
from pipeline import inject  # noqa: E402

import incident_report as ir  # noqa: E402

# The three, and what each one is here to show. Kept beside the code rather than in the
# module docstring because the report prints it and a reader of the output should not have
# to open the file.
WALKED = [
    ("truncate",
     "the upstream extractor stopped at 14:00 and the file holds a partial day",
     "the case volume was built for. it works, and the incident view still needs help."),
    ("dropped_column",
     "an upstream column stops being sent at all",
     "the monitor named for this cannot see it. the loader normalises it away first."),
    ("late_arrival",
     "a quarter of yesterday's events land in today's file",
     "detected at page severity. nothing in this repo can repair the damage."),
]


def clean_counts(rows):
    """Per subject, how many clean partitions it fired on and how many there were.

    Read off the control arm of the paired run, so these are partitions the fit never saw.
    Every subject that fired at all gets an entry. A subject that never fired gets none,
    and that absence is deliberate. `timeline.noise_note` treats an unmeasured subject
    differently from a quiet one, because this project keeps finding that the unmeasured
    one is the one firing nightly.
    """
    observed = len(rows)
    counts = {}
    for row in rows:
        for alert in row["control_alerts"]:
            fired, _ = counts.get(alert.subject, (0, observed))
            counts[alert.subject] = (fired + 1, observed)
    return counts


def judge_arm_days(fitted, arm, con, days, counts, quarantined, past):
    """Judge every arm partition once, so `last_known_good` has the real past to walk.

    **The first version of this walked one incident at a time and only put that incident's
    own day into `judged`.** So the search for a last known good partition skipped the other
    nine injected days entirely and reported the last clean training partition as one step
    back, when eight broken partitions sat between the two. The number it printed was
    correct about the dict it was handed and wrong about the world, which is the failure mode
    this project has hit in five other places.

    Quarantined subjects are removed before the alert list is stored, because a partition
    whose only alert was quarantined is clean as far as a reference lookup is concerned.
    """
    judged = dict(past)
    for day in sorted(days):
        raw, checks = ir.judge_partition(fitted, arm["injected"], day, True,
                                         con["injected"])
        kept = [a for a in raw if a.subject not in quarantined]
        judged[day] = {
            "alerts": kept, "checks": checks,
            "row_count": arm["injected"]["volume"].get(day, (None, None))[1],
            "duration_ms": arm["injected"]["duration"].get(day, (None, None))[1],
        }
    return judged


def walk(fitted, arm, con, day, scenario, why, lesson, counts, quarantined, past):
    """One incident, printed the way the README quotes it."""
    print()
    print("=" * 78)
    print(f"INCIDENT {scenario}   partition {day}")
    print(f"  fault      {why}")
    print(f"  why it is here  {lesson}")
    print("=" * 78)

    control, _ = ir.judge_partition(fitted, arm["control"], day, True, con["control"])
    raw, checks = ir.judge_partition(fitted, arm["injected"], day, True, con["injected"])

    kept = [a for a in raw if a.subject not in quarantined]
    dropped = [a for a in raw if a.subject in quarantined]
    new = {a.subject for a in kept} - {a.subject for a in control}

    print(f"\n{checks} monitor verdicts on this partition. "
          f"{len(raw)} alerts before quarantine, {len(kept)} after.")
    if dropped:
        for alert in dropped:
            print(f"  quarantined  {alert.subject:<30}{quarantined[alert.subject]}")

    print("\nwhat a reader should trust, and what the clean arm did on the same date:")
    print(f"  {'subject':<32}{'severity':<9}{'clean arm':<11}new")
    for alert in sorted(kept, key=lambda a: (alerting.rank(a.severity), a.subject)):
        fired, total = counts.get(alert.subject, (0, 0))
        seen = f"{fired} of {total}" if total else "unmeasured"
        print(f"  {alert.subject:<32}{alert.severity:<9}{seen:<11}"
              f"{'yes' if alert.subject in new else 'no'}")

    incidents = alerting.group_incidents(kept)
    if not incidents:
        print("\nno incident to assemble, every alert was quarantined")
        return
    judged = past
    runs = [tl.Run(*row) for row in
            history.partition_runs(con["injected"], f"dt={day.isoformat()}", ir.PIPELINE)]
    current = history.partition_schema(con["injected"], f"dt={day.isoformat()}",
                                       ir.PIPELINE)
    previous = history.partition_schema(
        con["injected"], f"dt={(day - timedelta(days=1)).isoformat()}", ir.PIPELINE)
    built = tl.assemble(incidents[0], runs, current, previous, judged,
                        clean_rates=counts)
    print()
    print(tl.render(built))


def main():
    ap = argparse.ArgumentParser(description="three incidents walked end to end")
    ap.add_argument("--obs-db", default="/tmp/obs.duckdb")
    ap.add_argument("--db", default="/tmp/wh.duckdb")
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--scratch", default="/tmp")
    args = ap.parse_args()

    base_con = store.connect(args.obs_db)
    fitted = ir.fit_everything(base_con)
    start = date.fromisoformat(args.start)
    first_day = fitted["last_clean"] + timedelta(days=1)
    base_con.close()

    control_db, days = ir.run_arm("control", args.obs_db, args.db, first_day, start,
                                  False, args.scratch)
    injected_db, _ = ir.run_arm("injected", args.obs_db, args.db, first_day, start, True,
                                args.scratch)
    control_con = store.connect(control_db)
    injected_con = store.connect(injected_db)
    con = {"control": control_con, "injected": injected_con}
    arm = {"control": ir.read_arm(control_con), "injected": ir.read_arm(injected_con)}

    # the control arm first, because everything below is read against it. reusing
    # incident_report's own detect pass rather than recomputing the rows here, so the two
    # scripts cannot disagree about what the clean arm did.
    rows, _caught, _owner, _testable = ir.detect_section(
        fitted, {"arm": arm["control"], "con": control_con},
        {"arm": arm["injected"], "con": injected_con}, days, True, "with freshness")
    counts = clean_counts(rows)
    quarantined = alerting.quarantine(counts)

    print("\n== quarantine: subjects held out of the alert stream entirely ==")
    print("measured on the clean arm, which is ten partitions the fit never saw.")
    for subject, reason in sorted(quarantined.items()):
        print(f"  {subject:<30}{reason}")
    if not quarantined:
        print("  none")
    print("\nclean arm fire counts, every subject that fired at all:")
    for subject, (fired, observed) in sorted(counts.items(), key=lambda kv: -kv[1][0]):
        mark = "  QUARANTINED" if subject in quarantined else ""
        print(f"  {subject:<30}{fired} of {observed}{mark}")

    base_con = store.connect(args.obs_db)
    history_judged = ir.judge_history(fitted, ir.read_arm(base_con), base_con, True)
    base_con.close()
    past = judge_arm_days(fitted, arm, con, days, counts, quarantined, history_judged)

    by_scenario = {scenario: day for day, scenario in days.items()}
    for scenario, why, lesson in WALKED:
        day = by_scenario.get(scenario)
        if day is None:
            print(f"\n{scenario} is not in this run")
            continue
        walk(fitted, arm, con, day, scenario, why, lesson, counts, quarantined, past)

    control_con.close()
    injected_con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
