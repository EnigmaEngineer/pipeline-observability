"""When a monitor fires, what a person needs on the screen next.

The project description promised an incident timeline that shows the upstream runs, the
schema at the time, and the last known good batch. Building it turned two of those three
into questions the metadata cannot answer on its own, which is the useful part of the
day.

Like `baseline.py`, `drift.py` and `alerting.py` this imports no duckdb. It takes rows
and returns a structure. `obs/history.py` is still the only file in the path that knows
SQL exists.

**Upstream is declared here and it is not derived.** Day 1 chose four tables and none of
them holds an edge. There is no dependency graph in this schema, so "which runs fed this
one" has no answer inside the metadata. The tempting fix is to infer it from start order
within a partition, and that is wrong. Two independent tasks that happen to run in
sequence would read as a dependency, and the timeline would then confidently point an
on call engineer at a task that has nothing to do with the failure. A wrong upstream is
worse than no upstream. So `TASK_GRAPH` is a declaration and the report prints that it
is one.

**Last known good has three answers and not two.** A partition with no alerts is only
good relative to the monitors that ran on it. If every monitor refused to judge that
partition then "no alerts" means nothing was checked. Days 3 and 5 already drew that
line, with `unbanded` and `unknown_key` kept out of `ok`, and this is where it pays off
a second time. A timeline that offers an unjudged partition as the last known good batch
is handing someone a reference value nobody ever verified.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

# Which task feeds which, declared. On this pipeline `build_daily` reads the table
# `load_raw` writes. That fact lives in `pipeline/orders.py` and in nothing the collector
# stores, so it has to be restated here. In a real deployment this comes out of the
# scheduler, which is the one system that does know the graph. It does not come out of
# run metadata, and a timeline built on run metadata alone has to say so.
TASK_GRAPH = {
    "build_daily": ["load_raw"],
    "load_raw": [],
}

# The states of a last known good lookup. Four rather than two, and the two failure
# states in the middle are not the same thing to the person reading them. "Every earlier
# partition was checked and every one of them alerted" says the incident is older than
# this partition. "No earlier partition was ever checked" says the monitor is new or
# broken. Collapsing those into one empty answer loses the more urgent of the two.
GOOD_CLEAN = "clean"
GOOD_ALL_ALERTING = "all_alerting"
GOOD_UNJUDGED = "unjudged"
GOOD_NONE = "none"


@dataclass
class Run:
    """One row of obs_run, reshaped so the timeline does not index into tuples."""
    run_id: str
    task: str
    partition_key: Optional[str]
    attempt: int
    started_at: object
    ended_at: object
    duration_ms: Optional[int]
    status: str
    error: Optional[str]
    code_version: Optional[str]
    cold_start: bool = False

    @property
    def failed(self):
        return self.status == "failed"

    @property
    def unfinished(self):
        return self.status == "running"


@dataclass
class SchemaFact:
    """What shape a dataset had on this partition, and when that shape started.

    `changed_here` is the field that matters during an incident. A schema that first
    appeared on the partition being investigated is a much stronger lead than one that
    has been stable since January, and the difference is one comparison the reader
    should not have to make themselves.
    """
    dataset: str
    schema_hash: Optional[str]
    column_count: Optional[int]
    first_seen_at: object = None
    changed_here: bool = False
    previous_hash: Optional[str] = None


@dataclass
class LastGood:
    state: str                       # clean, unjudged, or none
    partition: Optional[date] = None
    row_count: Optional[int] = None
    duration_ms: Optional[int] = None
    searched: int = 0

    @property
    def usable(self):
        """Whether this is safe to quote as a reference value."""
        return self.state == GOOD_CLEAN


@dataclass
class Timeline:
    partition: Optional[date]
    severity: str
    alerts: list = field(default_factory=list)
    runs: list = field(default_factory=list)
    upstream: list = field(default_factory=list)
    schema: list = field(default_factory=list)
    last_good: Optional[LastGood] = None
    notes: list = field(default_factory=list)
    # subject -> (fired, observed) on clean partitions the fit never saw. Day 7. Writing
    # the three worked incidents made the hole obvious. A timeline listing five alerts
    # with nothing to separate them hands an on call engineer a ranking they have to
    # invent, and on this feed three of those five fire on ordinary days as well. The
    # control arm has known that since day 6 and the incident view was never told.
    clean_rates: dict = field(default_factory=dict)

    @property
    def failed_runs(self):
        return [r for r in self.runs if r.failed]

    @property
    def monitors(self):
        return sorted({a.monitor for a in self.alerts})

    def noise_note(self, alert):
        """How often this alert's subject fires on clean data, or None if unmeasured.

        Unmeasured and quiet are different answers and they are not collapsed. A subject
        with no clean count is one nobody has checked, which the rest of this project has
        found is reliably the one that fires nightly.
        """
        counted = self.clean_rates.get(alert.subject)
        if counted is None:
            return None
        fired, observed = counted
        if not observed:
            return None
        return fired, observed


def last_known_good(partition, judged):
    """Walk back from a partition to the most recent one that was checked and clean.

    `judged` maps a partition date to a dict with `alerts` and `checks`. `checks` is how
    many monitor verdicts that partition actually received. A partition with zero checks
    is skipped rather than accepted, because a partition nothing looked at is not
    evidence of anything.

    The return says which of four things happened. `clean` found one. `all_alerting`
    means earlier partitions were checked and every one of them alerted, which says the
    incident started before this partition. `unjudged` means earlier partitions exist
    and not one was ever checked, which says the monitor is the problem rather than the
    data. `none` means there were no earlier partitions at all.
    """
    earlier = sorted([p for p in judged if partition is None or p < partition],
                     reverse=True)
    if not earlier:
        return LastGood(state=GOOD_NONE, searched=0)
    seen_any_check = False
    for i, day in enumerate(earlier, start=1):
        entry = judged[day]
        if not entry.get("checks"):
            continue
        seen_any_check = True
        if not entry.get("alerts"):
            return LastGood(state=GOOD_CLEAN, partition=day,
                            row_count=entry.get("row_count"),
                            duration_ms=entry.get("duration_ms"), searched=i)
    state = GOOD_ALL_ALERTING if seen_any_check else GOOD_UNJUDGED
    return LastGood(state=state, searched=len(earlier))


def upstream_runs(runs, task_graph=None):
    """Runs of the tasks that feed the failing ones, for the same partition.

    Returns the feeder runs only. The failing task's own run is already in `runs` and
    repeating it under a heading that says upstream would be misleading.
    """
    graph = TASK_GRAPH if task_graph is None else task_graph
    tasks = {r.task for r in runs}
    wanted = set()
    for task in tasks:
        wanted.update(graph.get(task, []))
    wanted -= tasks_that_alerted(runs)
    return [r for r in runs if r.task in wanted]


def tasks_that_alerted(runs):
    """Placeholder for the day when alerts carry the task that produced them.

    They do not yet. Every alert this project raises comes from a dataset level monitor
    and the dataset is the subject, not the task. So this returns nothing and
    `upstream_runs` returns every declared feeder. Named rather than inlined as an empty
    set because the shape of the missing information is worth seeing.
    """
    return set()


def schema_facts(current, previous=None):
    """Schema shape per dataset, with a flag for whether it started on this partition.

    `current` and `previous` map a dataset name to `(schema_hash, column_count,
    first_seen_at)`. `previous` of None means this is the first partition, which is not
    the same as an unchanged schema and does not set `changed_here`.
    """
    facts = []
    for dataset in sorted(current):
        schema_hash, column_count, first_seen = current[dataset]
        prior = (previous or {}).get(dataset)
        prior_hash = prior[0] if prior else None
        facts.append(SchemaFact(
            dataset=dataset,
            schema_hash=schema_hash,
            column_count=column_count,
            first_seen_at=first_seen,
            changed_here=bool(prior_hash and prior_hash != schema_hash),
            previous_hash=prior_hash,
        ))
    return facts


def assemble(incident, runs, current_schema, previous_schema, judged,
             task_graph=None, clean_rates=None):
    """Everything a person needs about one incident, in one object.

    `incident` is an `alerting.Incident`. `runs` are the `Run` records for that
    partition, all tasks and all attempts, in start order.

    `clean_rates` maps a subject to `(fired, observed)` on clean partitions the fit never
    saw. Optional, and an absent one is not treated as a quiet one.
    """
    timeline = Timeline(
        partition=incident.partition,
        severity=incident.severity,
        alerts=list(incident.alerts),
        runs=list(runs),
        upstream=upstream_runs(runs, task_graph),
        schema=schema_facts(current_schema, previous_schema),
        last_good=last_known_good(incident.partition, judged),
        clean_rates=dict(clean_rates or {}),
    )
    for note in _notes(timeline):
        timeline.notes.append(note)
    return timeline


def _notes(timeline):
    """Lines the reader would otherwise have to work out by comparing two things.

    Deliberately short. A timeline that explains itself at length is a timeline that
    does not trust its own layout, and the point of the layout is that an on call
    engineer reads it at 3am.
    """
    notes = []
    changed = [f.dataset for f in timeline.schema if f.changed_here]
    if changed:
        notes.append(f"schema changed on this partition for {', '.join(changed)}")
    failures = timeline.failed_runs
    if failures:
        tasks = sorted({r.task for r in failures})
        notes.append(f"a run failed here: {', '.join(tasks)}")
    stuck = [r for r in timeline.runs if r.unfinished]
    if stuck:
        notes.append(f"{len(stuck)} runs never finished")
    retried = [r for r in timeline.runs if r.attempt > 1]
    if retried:
        notes.append(f"{len(retried)} of the runs here were retries")
    if any(r.cold_start for r in timeline.runs):
        notes.append("at least one run here was the first of its task in its process")
    if timeline.last_good and not timeline.last_good.usable:
        notes.append("no earlier partition was both checked and clean, "
                     "so there is no reference value to compare against")
    return notes


def render(timeline, width=78):
    """The timeline as text. One incident, one screen.

    Order is fixed and it is the order the questions get asked in. What fired. What ran.
    What fed it. What shape the data was. What the last good one looked like.
    """
    out = []
    rule = "-" * width
    partition = timeline.partition or "unknown partition"
    out.append(rule)
    out.append(f"{partition}   severity {timeline.severity}   "
               f"{len(timeline.alerts)} alerts across {len(timeline.monitors)} monitors")
    out.append(rule)

    out.append("what fired")
    for alert in sorted(timeline.alerts, key=lambda a: (a.severity, a.subject)):
        suffix = f"   [capped by {alert.suppressed_by}]" if alert.suppressed_by else ""
        counted = timeline.noise_note(alert)
        if counted:
            fired, observed = counted
            suffix += f"   [also fires on {fired} of {observed} clean]"
        # padded to a width the longest real subject fits in, with a space after it
        # regardless. a subject wider than the column ran straight into the reason and
        # produced "quantile_shiftabove the band" in the first run of this.
        out.append(f"  {alert.severity:<7}{alert.subject:<32} {alert.reason}{suffix}")

    out.append("")
    out.append("runs on this partition")
    for run in timeline.runs:
        cold = " cold" if run.cold_start else ""
        duration = "n/a" if run.duration_ms is None else f"{run.duration_ms} ms"
        out.append(f"  {run.started_at}  {run.task:<12}attempt {run.attempt}  "
                   f"{run.status:<8}{duration:>10}{cold}")
        if run.error:
            out.append(f"      {run.error}")

    out.append("")
    if timeline.upstream:
        out.append("upstream runs, per the declared graph and not per the metadata")
        for run in timeline.upstream:
            out.append(f"  {run.task:<12}{run.status:<8}"
                       f"{'n/a' if run.duration_ms is None else run.duration_ms}")
    else:
        out.append("upstream runs: none declared for the tasks on this partition")

    out.append("")
    out.append("schema at the time")
    for fact in timeline.schema:
        marker = "  CHANGED HERE" if fact.changed_here else ""
        out.append(f"  {fact.dataset:<16}{fact.schema_hash}  "
                   f"{fact.column_count} columns  first seen {fact.first_seen_at}"
                   f"{marker}")

    out.append("")
    good = timeline.last_good
    if good is None or good.state == GOOD_NONE:
        out.append("last known good: no earlier partition exists")
    elif good.state == GOOD_ALL_ALERTING:
        out.append(f"last known good: NONE. {good.searched} earlier partitions were "
                   "checked and every one of them alerted, so this started earlier")
    elif good.state == GOOD_UNJUDGED:
        out.append(f"last known good: NONE. {good.searched} earlier partitions exist "
                   "and not one of them was ever checked")
    else:
        out.append(f"last known good: {good.partition}, {good.searched} partitions back"
                   f"   rows {good.row_count}   {good.duration_ms} ms")

    if timeline.notes:
        out.append("")
        for note in timeline.notes:
            out.append(f"note: {note}")
    return "\n".join(out)
