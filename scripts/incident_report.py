"""Put real faults through the whole stack and count which ones it catches.

    python scripts/run_observed.py --start 2026-01-01 --end 2026-04-29 \
        --db /tmp/wh.duckdb --obs-db /tmp/obs.duckdb --quiet
    python scripts/incident_report.py --obs-db /tmp/obs.duckdb --db /tmp/wh.duckdb \
        --chart docs/incidents.png

Days 3 to 5 measured this project against a feed that never breaks. Every number so far
describes how quiet the monitors are on ordinary data. That is half a result. A stack
that stays silent on clean partitions and also stays silent on broken ones is not quiet.

The design is paired and the pairing is the part that makes the number mean anything.
Ten future partitions are generated past the end of the clean history. Both arms get the
same ten dates from the same generator with the same seed. One arm loads them clean and
the other loads them with one fault each. The monitors are fitted once, on the clean 119
partitions only, and both arms are judged by those same fitted objects. So the arms
differ by the injection and by nothing else, the control arm measures the false positive
rate on partitions the fit never saw, and a detection is a difference between the two
rather than a firing in isolation.

A harness that reported whatever fired and called it detection could never fail. So
`pipeline/inject.py` writes down which monitor should answer for each fault before the
run, and the table below prints the expectation beside the outcome.

Sections:

    detect     each fault, what should have caught it, what did
    control    what fired on the clean arm, which is the false positive rate
    fresh      the same run with the day-6 freshness check added
    timeline   the incident timeline for the loudest incidents
    schema     what the declared column list does to an upstream column drop
"""

import argparse
import json
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from obs import alerting, drift, freshness, history, store, timeline as tl  # noqa: E402
from obs.baseline import Baseline  # noqa: E402
from obs.tracker import reset_process_state  # noqa: E402
from pipeline import generate, inject, orders  # noqa: E402

import alert_report  # noqa: E402
import run_observed  # noqa: E402

WATCHED = ["order_amount_usd", "item_count", "coupon_code", "status", "channel",
           "customer_id"]
NARROW_WINDOW = 56
DATASET = "raw_orders"
PIPELINE = "orders"
TASK = "load_raw"


def write_partition(raw_root: Path, day: date, events):
    part = raw_root / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    with (part / "orders.jsonl").open("w", encoding="utf-8") as fh:
        for row in events:
            fh.write(json.dumps(row) + "\n")
    return len(events)


def fit_everything(base_con, window=NARROW_WINDOW):
    """Fit every monitor on the clean history, once.

    Both arms are judged by the objects this returns. Fitting per arm would let the fit
    see the fault, which is the mistake that makes an injection harness measure nothing.
    """
    volume_obs, _ = history.volume_history(base_con, DATASET, PIPELINE)
    duration_obs, _ = history.duration_history(base_con, PIPELINE, TASK)
    wide = Baseline.fit(history.keyed(volume_obs))
    narrow = Baseline.fit(history.keyed(history.recent(volume_obs, window)))
    duration = Baseline.fit(history.keyed(duration_obs), space="log")

    monitors = {}
    by_column = {}
    for column in WATCHED:
        obs, _ = history.column_history(base_con, DATASET, column, PIPELINE)
        if obs:
            by_column[column] = obs
            monitors[column] = drift.Monitor.fit(column, obs)

    # The pager gate needs a fire rate measured out of sample, per day 5. Reusing that
    # function rather than writing a second one here, because two implementations of the
    # same measurement drift and then the comparison between them measures the drift.
    fire_rates = alert_report.holdout_fire_rates(by_column)

    schema = history.partition_schema(
        base_con, f"dt={volume_obs[-1][2].isoformat()}", PIPELINE)
    _, wide_rate = wide.fire_rate(history.keyed(history.recent(volume_obs, window)))

    return {"wide": wide, "narrow": narrow, "duration": duration,
            "monitors": monitors, "schema": schema, "volume_fire_rate": wide_rate,
            "fire_rates": fire_rates, "last_clean": volume_obs[-1][2],
            "volume_obs": volume_obs}


def judge_history(fitted, base_arm, base_con, with_freshness=False):
    """Judge every partition of the clean history, so a timeline has a past to walk.

    In sample and labelled as such. These are the partitions the monitors were fitted
    on, so this is not evidence about the false alarm rate and it is not used as any. It
    is used for one thing, which is answering what the last partition was that the
    monitor looked at and did not complain about. In production that lookup is in sample
    too, because a monitor fitted on history is asked about the partitions in its own
    history every time somebody investigates an incident.

    The first version of this skipped the work and marked all 119 clean by assertion.
    That made `last_known_good` return a partition nobody had checked, which is the exact
    thing `timeline.last_known_good` has three failure states to avoid.
    """
    judged = {}
    for _weekday, rows, day in fitted["volume_obs"]:
        alerts, checks = judge_partition(fitted, base_arm, day, with_freshness, base_con)
        judged[day] = {"alerts": alerts, "checks": checks, "row_count": rows,
                       "duration_ms": base_arm["duration"].get(day, (None, None))[1]}
    return judged


def read_arm(con):
    """Everything the judge needs out of one arm's metadata, keyed by partition date."""
    volume, _ = history.volume_history(con, DATASET, PIPELINE)
    duration, _ = history.duration_history(con, PIPELINE, TASK)
    events, _ = history.event_time_history(con, DATASET, PIPELINE)
    columns = {}
    for column in WATCHED:
        obs, _ = history.column_history(con, DATASET, column, PIPELINE)
        columns[column] = {o["date"]: o for o in obs}
    return {
        "volume": {d: (w, v) for w, v, d in volume},
        "duration": {d: (w, v) for w, v, d in duration},
        "events": {o["date"]: o for o in events},
        "columns": columns,
    }


def judge_partition(fitted, arm, day, with_freshness=False, con=None):
    """Every monitor run against one partition. Returns the alerts and the check count.

    The check count is not decoration. `timeline.last_known_good` refuses to offer a
    partition nobody checked as a reference value, and it needs this number to tell an
    unchecked partition from a clean one.
    """
    alerts = []
    checks = 0

    entry = arm["volume"].get(day)
    if entry is not None:
        weekday, rows = entry
        checks += 1
        status, _hint = alerting.two_band_verdict(fitted["wide"], fitted["narrow"],
                                                  weekday, rows)
        if status == "outside_both":
            verdict = fitted["wide"].check(weekday, rows)
            alert = alerting.raise_alert("volume", "row_count", verdict, partition=day,
                                         fire_rate=fitted["volume_fire_rate"])
            if alert:
                alerts.append(alert)
        elif status == "between":
            alerts.append(alerting.Alert(
                monitor="volume", subject="row_count", partition=day, status="between",
                severity="ticket", value=rows,
                reason="outside the recent band and inside the full history band"))

    entry = arm["duration"].get(day)
    if entry is not None:
        weekday, ms = entry
        checks += 1
        alert = alerting.raise_alert("duration", "duration_ms",
                                     fitted["duration"].check(weekday, ms),
                                     partition=day)
        if alert:
            alerts.append(alert)

    for column, monitor in fitted["monitors"].items():
        observation = arm["columns"][column].get(day)
        if observation is None:
            continue
        values = monitor.signals(observation)
        for name in monitor.watched():
            value = values.get(name)
            if value is None:
                continue
            checks += 1
            verdict = monitor.check(name, observation["weekday"], value)
            alert = alerting.raise_alert(
                "drift", name, verdict, partition=day,
                fire_rate=fitted["fire_rates"].get((column, name)),
                subject=f"{column} {name}")
            if alert:
                alerts.append(alert)

    if con is not None:
        current = history.partition_schema(con, f"dt={day.isoformat()}", PIPELINE)
        for dataset, (schema_hash, _count, _seen) in sorted(current.items()):
            known = fitted["schema"].get(dataset)
            if known is None:
                continue
            checks += 1
            if known[0] != schema_hash:
                alerts.append(alerting.Alert(
                    monitor="schema", subject=f"{dataset} shape", partition=day,
                    status="changed", severity="page", value=schema_hash,
                    reason=f"column list changed from {known[0]}"))

    if with_freshness:
        observation = arm["events"].get(day)
        if observation is not None:
            checks += 1
            lag = freshness.check(observation)
            if not lag.clean:
                alerts.append(alerting.Alert(
                    monitor="freshness", subject="event_time range", partition=day,
                    status=lag.status, severity="page", value=lag.before,
                    reason=f"events reach {lag.before} days before this partition"))

    return alerts, checks


def run_arm(name, base_obs, base_wh, first_day, start, injected, scratch="/tmp"):
    """Load the ten future partitions into a copy of the base metadata.

    A copy rather than a rerun. Rebuilding the 119 clean partitions for each arm would
    cost forty seconds and would also let the two arms differ by whatever the machine was
    doing at the time, which is exactly the noise the paired design exists to remove.

    `scratch` is settable because these paths used to be constants. A previous run leaving
    files behind under a different owner made the next run die on shutil.copy, which is a
    silly way to lose a run.
    """
    obs_path = f"{scratch}/inc_{name}_obs.duckdb"
    wh_path = f"{scratch}/inc_{name}_wh.duckdb"
    for src, dst in ((base_obs, obs_path), (base_wh, wh_path)):
        shutil.copy(src, dst)
    raw_root = Path(f"{scratch}/inc_{name}_raw")

    con = duckdb.connect(wh_path)
    orders.create_tables(con)
    obs_con = store.connect(obs_path)
    reset_process_state()

    version = run_observed.code_version()
    days = {}
    for i, (scenario, _fn, _kw, _owner, _why) in enumerate(inject.SCENARIOS):
        day = first_day + timedelta(days=i)
        events = list(generate.events_for_day(day, start))
        if injected:
            prior = list(generate.events_for_day(inject.previous_day(day), start))
            events = inject.apply(scenario, events, prior)
        write_partition(raw_root, day, events)
        run_observed.observe_day(con, obs_con, day, raw_root, version)
        days[day] = scenario
    con.close()
    return obs_path, days


def loudest(alerts):
    return min((a.severity for a in alerts), key=alerting.rank, default="none")


def subjects(alerts):
    return {f"{a.monitor}:{a.subject}" for a in alerts}


def detect_section(fitted, control, injected, days, with_freshness, label):
    """Each fault against its paired clean partition.

    Detection is a set difference on the subjects that fired and not a difference in how
    many fired. The first version compared counts, which calls it a miss when a fault
    silences one signal and trips another, and calls it a hit when a signal that fires on
    everything happens to fire twice. Both happened in this run.

    `owner` is the stricter column and it is the one worth reading. Something new firing
    only says the stack noticed. The monitor named in `pipeline/inject.py` before the run
    firing says it noticed for the right reason.
    """
    print(f"\n== detect: {label} ==")
    print(f"{'fault':<16}{'should be caught by':<22}{'new subjects':<13}"
          f"{'severity':<9}{'any':<5}owner")
    scenarios = {s: (owner, why) for s, _f, _k, owner, why in inject.SCENARIOS}
    rows = []
    caught = 0
    owner_caught = 0
    testable = 0
    for day in sorted(days):
        scenario = days[day]
        owner, _why = scenarios[scenario]
        c_alerts, _ = judge_partition(fitted, control["arm"], day, with_freshness,
                                      control["con"])
        i_alerts, _ = judge_partition(fitted, injected["arm"], day, with_freshness,
                                      injected["con"])
        new = subjects(i_alerts) - subjects(c_alerts)
        hit = bool(new)
        # the declared owner is a prefix like "drift null_rate" or "volume". a new
        # subject counts for it when it starts with that, which is what lets one
        # declaration cover every column watched by the same signal.
        stem = owner.replace(" ", ":") if owner != "none declared" else None
        owner_hit = bool(stem) and any(s.startswith(stem.split(":")[0])
                                       and stem.split(":")[-1] in s for s in new)
        if scenario != "no_change":
            testable += 1
            caught += 1 if hit else 0
            owner_caught += 1 if owner_hit else 0
        shown = ", ".join(sorted(new))[:12] or "none"
        print(f"{scenario:<16}{owner:<22}{shown:<13}"
              f"{loudest(i_alerts):<9}{'yes' if hit else 'NO':<5}"
              f"{'yes' if owner_hit else 'NO'}")
        rows.append({"day": day, "scenario": scenario, "owner": owner,
                     "control": len(c_alerts), "injected": len(i_alerts),
                     "new": sorted(new), "hit": hit, "owner_hit": owner_hit,
                     "severity": loudest(i_alerts), "alerts": i_alerts,
                     "control_alerts": c_alerts})
    print(f"\n{caught} of {testable} faults fired a subject the clean arm did not")
    print(f"{owner_caught} of {testable} were caught by the monitor named beforehand")
    return rows, caught, owner_caught, testable


def what_fired(rows):
    print("\n== what fired, per fault ==")
    for row in rows:
        if not row["alerts"]:
            print(f"{row['scenario']:<16}nothing")
            continue
        subjects = sorted({f"{a.monitor}:{a.subject}" for a in row["alerts"]})
        print(f"{row['scenario']:<16}{len(subjects)} distinct subjects")
        for subject in subjects[:6]:
            print(f"    {subject}")
        if len(subjects) > 6:
            print(f"    and {len(subjects) - 6} more")


def control_section(rows):
    """The clean arm. Every alert here is a false positive by construction.

    This is the number that decides whether the detection column means anything. A stack
    that alerts on every partition detects every fault and is worth nothing. Every fire
    rate this project quoted before today was measured on partitions the monitors were
    fitted on, so this is its first out of sample false positive count.
    """
    print("\n== control: what the clean arm did on partitions the fit never saw ==")
    noisy = [r for r in rows if r["control"]]
    print(f"{len(noisy)} of {len(rows)} clean future partitions produced an alert")
    print(f"{'monitor':<12}{'subject':<28}{'partitions fired':>17}")
    per_subject = {}
    for row in rows:
        for subject in subjects(row["control_alerts"]):
            per_subject[subject] = per_subject.get(subject, 0) + 1
    for subject, count in sorted(per_subject.items(), key=lambda kv: -kv[1]):
        monitor, _, name = subject.partition(":")
        print(f"{monitor:<12}{name:<28}{count:>10} of {len(rows)}")
    dominant = max(per_subject.values(), default=0)
    print(f"\nthe worst single subject fires on {dominant} of {len(rows)} clean")
    print("partitions. a subject at that rate is not evidence of anything when it fires")
    print("on a broken one, so the detection column has to be read as a set difference")
    print("against this arm rather than as a count of what went off.")
    return len(noisy), per_subject


def timeline_section(fitted, arm, con, days, with_freshness, past, limit=2):
    print("\n== timeline: the incident view, for the loudest incidents ==")
    judged = dict(past)
    for day in sorted(days):
        alerts, checks = judge_partition(fitted, arm, day, with_freshness, con)
        judged[day] = {"alerts": alerts, "checks": checks,
                       "row_count": arm["volume"].get(day, (None, None))[1],
                       "duration_ms": arm["duration"].get(day, (None, None))[1]}

    ranked = sorted([d for d in days if judged[d]["alerts"]],
                    key=lambda d: (alerting.rank(loudest(judged[d]["alerts"])),
                                   -len(judged[d]["alerts"])))
    shown = 0
    for day in ranked:
        if shown >= limit:
            break
        incidents = alerting.group_incidents(judged[day]["alerts"])
        if not incidents:
            continue
        runs = [tl.Run(*row) for row in
                history.partition_runs(con, f"dt={day.isoformat()}", PIPELINE)]
        current = history.partition_schema(con, f"dt={day.isoformat()}", PIPELINE)
        previous = history.partition_schema(
            con, f"dt={(day - timedelta(days=1)).isoformat()}", PIPELINE)
        built = tl.assemble(incidents[0], runs, current, previous, judged)
        print()
        print(f"[{days[day]}]")
        print(tl.render(built))
        shown += 1
    return judged


def schema_section(control_con, injected_con, days):
    print("\n== schema: what a declared column list does to an upstream column drop ==")
    day = [d for d in days if days[d] == "dropped_column"]
    if not day:
        print("no dropped_column scenario in this run")
        return None
    day = day[0]
    key = f"dt={day.isoformat()}"
    clean = history.partition_schema(control_con, key, PIPELINE)
    broken = history.partition_schema(injected_con, key, PIPELINE)
    print(f"partition {key}")
    for dataset in sorted(clean):
        left = clean[dataset][0]
        right = broken.get(dataset, (None,))[0]
        same = "IDENTICAL" if left == right else "different"
        print(f"  {dataset:<16}clean {left}   dropped {right}   {same}")
    nulls = injected_con.execute(
        """
        SELECT c.null_count, d.row_count
          FROM obs_column_metric c
          JOIN obs_run r ON r.run_id = c.run_id
          JOIN obs_dataset_metric d ON d.run_id = c.run_id AND d.dataset = c.dataset
         WHERE r.partition_key = ? AND c.dataset = ? AND c.column_name = 'channel'
        """,
        [key, DATASET],
    ).fetchone()
    if nulls:
        print(f"  channel null_count {nulls[0]} of {nulls[1]} rows")
    print()
    print("the loader declares its column list rather than inferring it, which was a")
    print("day-1 decision made so a null heavy day could not silently retype a column.")
    print("the cost lands here. an upstream column that disappears is read as a column")
    print("full of nulls, so the schema hash never moves and the schema monitor cannot")
    print("see it. the null rate monitor can. the schema is observed after the load and")
    print("the load is the thing that normalises it away.")
    return clean, broken


def chart(rows_before, rows_after, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    names = [r["scenario"] for r in rows_before]
    control = [r["control"] for r in rows_before]
    before = [r["injected"] for r in rows_before]
    after = [r["injected"] for r in rows_after]

    y = range(len(names))
    fig, ax = plt.subplots(figsize=(9, 5.2))
    height = 0.27
    ax.barh([i + height for i in y], control, height=height, label="clean arm",
            color="#b0b7c3")
    ax.barh(list(y), before, height=height, label="injected, five monitors",
            color="#4c78a8")
    ax.barh([i - height for i in y], after, height=height,
            label="injected, with freshness", color="#e45756")
    ax.set_yticks(list(y))
    ax.set_yticklabels(names)
    ax.set_xlabel("alerts raised on that partition")
    ax.set_title("Injected faults against the clean arm, same dates and same fitted "
                 "monitors")
    ax.legend(loc="lower right", fontsize=8)
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=140)
    print(f"\nwrote {path}")


def main():
    ap = argparse.ArgumentParser(description="inject faults and count detections")
    ap.add_argument("--obs-db", default="/tmp/obs.duckdb",
                    help="metadata for the clean history, built by run_observed.py")
    ap.add_argument("--db", default="/tmp/wh.duckdb")
    ap.add_argument("--start", default="2026-01-01",
                    help="first day of the clean history, for the generator trend")
    ap.add_argument("--chart")
    ap.add_argument("--scratch", default="/tmp",
                    help="where the per arm copies go. change it if a previous run left "
                         "files you cannot overwrite")
    args = ap.parse_args()

    base_con = store.connect(args.obs_db)
    fitted = fit_everything(base_con)
    start = date.fromisoformat(args.start)
    first_day = fitted["last_clean"] + timedelta(days=1)
    print(f"clean history ends {fitted['last_clean']}, injecting from {first_day}")
    print(f"{len(fitted['monitors'])} column monitors fitted, plus volume, duration "
          "and schema")
    base_con.close()

    control_db, days = run_arm("control", args.obs_db, args.db, first_day, start, False,
                               args.scratch)
    injected_db, _ = run_arm("injected", args.obs_db, args.db, first_day, start, True,
                             args.scratch)
    control_con = store.connect(control_db)
    injected_con = store.connect(injected_db)
    control = {"arm": read_arm(control_con), "con": control_con}
    injected = {"arm": read_arm(injected_con), "con": injected_con}

    rows_before, caught, owner_before, testable = detect_section(
        fitted, control, injected, days, False,
        "the five monitors that existed this morning")
    what_fired(rows_before)
    control_section(rows_before)

    print("\n== fresh: the same run with the day-6 freshness check added ==")
    print("event_time_min and event_time_max have been collected on every run since day")
    print("2 and nothing read them until today. obs/freshness.py is the reader and it")
    print("was written because late_arrival had no owner, not because it was planned.")
    rows_after, caught_after, owner_after, _ = detect_section(
        fitted, control, injected, days, True, "with freshness")
    print(f"\nany subject: {caught} of {testable} to {caught_after} of {testable}")
    print(f"declared owner: {owner_before} of {testable} to {owner_after} of {testable}")

    # what freshness does on the clean history. it has no band and no fit, so this is not
    # a fire rate. it is the check that the premise holds. ot-015 says the pipeline is
    # only correct because the generator never emits a late event, and this is the line
    # that either shows that or contradicts it.
    base_con = store.connect(args.obs_db)
    clean_events, skipped = history.event_time_history(base_con, DATASET, PIPELINE)
    clean_lags = freshness.scan(clean_events)
    base_con.close()
    print(f"\nfreshness over the {len(clean_events)} clean training partitions: "
          f"{len(clean_lags)} outside their own day, {skipped} unreadable")
    if clean_lags:
        for lag in clean_lags[:5]:
            print(f"  {lag.partition}  {lag.status}  before={lag.before} after={lag.after}")
    else:
        print("  the generator emits no late events, which is the ot-015 premise holding")

    base_con = store.connect(args.obs_db)
    past = judge_history(fitted, read_arm(base_con), base_con, True)
    clean_alerting = sum(1 for d in past if past[d]["alerts"])
    print(f"\nin sample, {clean_alerting} of {len(past)} training partitions alert")
    timeline_section(fitted, injected["arm"], injected_con, days, True, past)
    base_con.close()
    schema_section(control_con, injected_con, days)

    if args.chart:
        chart(rows_before, rows_after, args.chart)

    control_con.close()
    injected_con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
