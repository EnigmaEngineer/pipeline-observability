"""Verdicts become alerts, alerts get a severity, and most of them get thrown away.

Like `baseline.py` and `drift.py` this imports no duckdb. It takes verdicts and turns them
into things a person is expected to answer for. `obs/history.py` is still the only file in
the path that knows SQL exists.

Days 3 and 4 built monitors. A monitor answers "is this partition unusual". An alert
answers a different question, which is "should someone stop what they are doing". Those
are not the same question and the gap between them is where most monitoring projects go
wrong. The three rules below are the whole design.

**A monitor that cannot judge must never wake anyone.** Day 3 made a zero width band
return `unbanded` and an unseen key return `unknown_key` rather than folding both into
`ok`. That distinction only pays off here. Both route to `info` and neither can be
promoted by any policy, because an alert derived from a band that refused to judge carries
exactly as much information as the refusal did.

**A signal that fires often cannot be a page.** The day-4 report prints a fire rate per
signal, on the training history, which makes it a floor on the false alarm rate rather
than an estimate. `page_eligible` reads that number back. A signal firing on 18 percent of
its own training partitions will fire about once a week forever, and a pager that goes off
once a week for a thing nobody acts on is worse than no pager. This is the 07-30
`support@5` lesson pointed the other way. A check that almost never binds is
uninformative, and a check that almost always binds cannot be urgent.

**Suppression is a severity cap, not a delete.** A maintenance window that deletes alerts
means the one real incident that happened during a deploy is gone with no record. A window
that caps severity at `ticket` still writes everything down and only stops the phone
ringing. The cost is that somebody has to read the tickets, which is the correct cost.

What this file does not do is decide when the pipeline should be restarted or who is on
call. Routing to a person is a config problem and it would be one dictionary of names with
no logic in it.
"""

from dataclasses import dataclass, field
from datetime import date
from math import comb
from typing import Optional

# Most urgent first. Comparisons go through `rank` rather than through the strings,
# because "info" < "page" is alphabetical and would be wrong in the direction that
# matters.
SEVERITIES = ("page", "ticket", "info")


def rank(severity):
    return SEVERITIES.index(severity)


def cap(severity, ceiling):
    """The less urgent of two severities."""
    return severity if rank(severity) >= rank(ceiling) else ceiling


@dataclass
class Alert:
    monitor: str            # volume, duration, drift, coverage
    subject: str            # what was watched, e.g. "order_amount_usd null_rate"
    partition: Optional[date]
    status: str             # the verdict status that produced it
    severity: str
    reason: str
    value: object = None
    score: Optional[float] = None
    suppressed_by: Optional[str] = None
    original_severity: Optional[str] = None

    @property
    def paging(self):
        return self.severity == "page"


@dataclass
class Window:
    """A period during which alerts are capped rather than removed.

    `monitors` of None means every monitor. `ceiling` of "info" is as close to silence as
    this gets, and it still leaves a record.
    """
    start: date
    end: date                        # inclusive, because partitions are whole days here
    reason: str
    ceiling: str = "ticket"
    monitors: Optional[frozenset] = None

    def covers(self, alert):
        if alert.partition is None:
            return False
        if not (self.start <= alert.partition <= self.end):
            return False
        return self.monitors is None or alert.monitor in self.monitors


# Above this fire rate a signal is not allowed to page whatever the policy says. One in
# twenty is roughly one alert a month on a daily partition, which is about the most a page
# can be worth answering. Chosen rather than measured. The volume band on the trailing
# window measures 0.143 today and the full history band measures 0.018, so this line falls
# between the two and moving it changes that answer.
MAX_PAGE_FIRE_RATE = 0.05


def page_eligible(fire_rate, limit=MAX_PAGE_FIRE_RATE):
    """Whether a signal is quiet enough to be worth a page.

    An unknown fire rate is not eligible. A signal nobody has counted is exactly the one
    that turns out to fire nightly.

    **The rate has to be measured out of sample and this function cannot check that.** A
    signal whose history never moved is stored as a constant, and a constant's fire rate
    against the history that defined it is zero by construction rather than by
    measurement. So an in sample rate approves every constant for a reason that is a
    tautology, which is what the first version of `scripts/alert_report.py` did to all ten
    of the signals it let page. The caller passes a rate measured on partitions the fit
    never saw. There is no way to enforce that from here and the comment is the guard.
    """
    return fire_rate is not None and fire_rate <= limit


# What a signal moving actually means, and therefore how loud it should be. The default at
# the bottom exists so a signal added later gets a defensible severity rather than an
# exception, and `ticket` is the right default because a new unclassified signal has not
# earned a page.
#
# The asymmetries are the content here. Volume falling is missing data and volume rising is
# usually a replay, so they are not the same event. A vocabulary losing a value means
# something upstream stopped producing it, which is worse than gaining one.
POLICY = {
    ("volume", "row_count"): {"low": "page", "high": "ticket"},
    ("duration", "duration_ms"): {"high": "ticket", "low": "info"},
    ("drift", "null_rate"): {"high": "page", "changed_up": "page",
                             "changed_down": "info"},
    ("drift", "distinct_count"): {"changed_up": "ticket", "changed_down": "page"},
    ("drift", "quantile_shift"): {"high": "ticket", "low": "info"},
    ("drift", "share_tv"): {"high": "ticket", "low": "info"},
    ("drift", "ks_bound"): {"changed_up": "ticket", "high": "ticket"},
}

DEFAULT_POLICY = {"high": "ticket", "low": "ticket",
                  "changed_up": "ticket", "changed_down": "ticket"}

# Statuses that mean the monitor declined to judge. No policy entry can promote these.
CANNOT_JUDGE = ("unbanded", "unknown_key")

# A subject that fired on every clean partition the fit never saw is not a monitor. The
# page gate above can only make an alert quieter, and quieter is not enough here, because
# such a subject carries nothing at any severity. Day 6 measured `duration_ms` firing on 10
# of 10 clean out of sample partitions at `info`, which put a meaningless line on every
# incident view in the project.
#
# The threshold is 1.0 and that is deliberately not a tuned number. The measured clean
# rates on this feed are 1.000 for duration and 0.300, 0.100 and 0.100 for the other three
# subjects that fire at all. Setting the line at 0.5 because those leave a gap would be
# choosing a threshold from the ten partitions it is about to be judged on, which is the
# day-3 mistake wearing new clothes. Everything below 1.0 keeps alerting and carries its
# measured rate instead.
QUARANTINE_FIRE_RATE = 1.0


def fire_rate_lower_bound(fired, observed, alpha=0.05):
    """Exact one sided lower bound on a fire rate, by inverting the binomial tail.

    Ten partitions is a small sample and every out of sample rate in this project rests on
    one. Printing 10 of 10 as 1.000 invites a reader to treat it as certainty, and printing
    3 of 10 as 0.300 invites the opposite mistake, which is dismissing it as too few
    observations to act on. The bound answers both. At 10 of 10 it is 0.741. At 3 of 10 it
    is 0.087, which is still above `MAX_PAGE_FIRE_RATE`, so a subject at that count fails
    the pager gate on the least favourable reading of its own evidence.

    Clopper Pearson, found by bisection rather than by a beta quantile, because that keeps
    this module on the standard library. Checked against the closed form for the all fired
    case, where the bound is alpha to the power one over n, in `tests/test_alerting.py`.
    """
    if not observed or fired <= 0:
        return 0.0
    lo, hi = 0.0, 1.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        tail = sum(comb(observed, i) * mid ** i * (1.0 - mid) ** (observed - i)
                   for i in range(fired, observed + 1))
        if tail > alpha:
            hi = mid
        else:
            lo = mid
    return lo


def quarantine(clean_counts, limit=QUARANTINE_FIRE_RATE):
    """Subjects held out of the alert stream entirely, mapped to the reason.

    `clean_counts` maps a subject to `(fired, observed)` measured on clean partitions the
    fit never saw. The return goes where `coverage_gaps` goes, because both are facts about
    a monitor rather than about any partition, and the 08-04 lesson is that a fact about a
    monitor gets stated once at fit time.

    This does not fix a quarantined subject. It stops it lying every day. `ot-023` is the
    live example and the README says what the real fixes would cost.
    """
    held = {}
    for subject, (fired, observed) in sorted(clean_counts.items()):
        if not observed:
            continue
        rate = fired / observed
        if rate < limit:
            continue
        bound = fire_rate_lower_bound(fired, observed)
        held[subject] = (f"fired on {fired} of {observed} clean partitions the fit never "
                         f"saw, so its true rate is at least {bound:.3f}")
    return held


def coverage_gaps(watched, judge):
    """Signals a monitor holds and cannot judge, reported once rather than every day.

    This function exists because the first version of this file was wrong about where an
    `unbanded` verdict belongs. Routing it to an `info` alert produced 238 of 255 alerts
    on the history below, two per partition forever, every one of them the monitor saying
    the same thing about itself. That is not an alert. Whether a band could be fitted is a
    property of the monitor and not of the partition it was pointed at, so it gets said
    once at fit time and then never again.

    `judge` takes a signal name and returns a verdict, which is how the caller keeps the
    Monitor type out of this file.
    """
    gaps = {}
    for name in watched:
        verdict = judge(name)
        if verdict is not None and verdict.status in CANNOT_JUDGE:
            gaps[name] = verdict.status
    return gaps


def outcome(verdict):
    """The policy key for a verdict, or None when nothing happened.

    A constant signal returns `changed` with no direction on it, so the direction is
    worked out here from the value it was compared against. Both directions exist in this
    project. `null_rate` on `order_amount_usd` has been 0 for the whole history and can
    only go up. A categorical `distinct_count` can go either way and the two mean opposite
    things.

    A constant that changed and cannot be ordered, which is any non numeric constant,
    comes back as `changed_up`. That is a deliberate bias towards the louder of the two
    when the direction is unknowable, and it is the only place in this file where an
    unknown gets the more urgent answer rather than the less urgent one. The reason is
    that a constant only changes once.
    """
    if verdict is None or verdict.status == "ok":
        return None
    # every status except `changed` is already its own policy key, including the two that
    # mean the monitor declined to judge. an explicit branch for those was here and a
    # mutant that deleted it survived, because the line below returned the same value.
    # it read as a safety check and was three lines of nothing.
    if verdict.status != "changed":
        return verdict.status
    try:
        return "changed_down" if verdict.value < verdict.expected else "changed_up"
    except TypeError:
        return "changed_up"


def severity_for(monitor, signal, verdict, fire_rate=None,
                 limit=MAX_PAGE_FIRE_RATE):
    """Severity for one verdict, before any suppression window is applied.

    `fire_rate` is how often this signal fired across the history it was fitted on. It can
    only ever make an alert quieter. There is no path here where a noisy signal gets
    promoted.
    """
    key = outcome(verdict)
    if key is None:
        return None
    if key in CANNOT_JUDGE:
        return "info"
    severity = POLICY.get((monitor, signal), DEFAULT_POLICY).get(key, "ticket")
    if severity == "page" and not page_eligible(fire_rate, limit):
        return "ticket"
    return severity


REASONS = {
    "high": "above the band",
    "low": "below the band",
    "changed_up": "a value this signal had never taken",
    "changed_down": "lost a value it had always had",
    "unbanded": "no band could be fitted, so nothing was judged",
    "unknown_key": "no history for this key, so nothing was judged",
}


def raise_alert(monitor, signal, verdict, partition=None, fire_rate=None,
                subject=None, limit=MAX_PAGE_FIRE_RATE, quarantined=False):
    """One verdict to at most one alert.

    Returns None when the verdict was fine and also when the monitor could not judge it.
    The second case is not an alert and `coverage_gaps` is where it goes instead.

    `quarantined` is the third way to get None back and it is the day-7 addition. The
    caller decides it with `quarantine` above, because that needs a clean arm to measure
    against and this function only ever sees one verdict.
    """
    if quarantined:
        return None
    if verdict is not None and verdict.status in CANNOT_JUDGE:
        return None
    severity = severity_for(monitor, signal, verdict, fire_rate, limit)
    if severity is None:
        return None
    key = outcome(verdict)
    reason = REASONS.get(key, key)
    if severity == "ticket" and POLICY.get((monitor, signal), {}).get(key) == "page":
        reason += f", held off the pager at a fire rate of {fire_rate:.3f}"
    return Alert(
        monitor=monitor,
        subject=subject or signal,
        partition=partition,
        status=verdict.status,
        severity=severity,
        reason=reason,
        value=verdict.value,
        score=verdict.score,
    )


def apply_windows(alerts, windows):
    """Cap every alert that falls inside a window. Nothing is removed.

    Returns a new list. The originals are left alone so a caller can print the before and
    after, which is the only way to see what a window actually cost.

    When windows overlap the quietest ceiling wins and every reason is recorded. The first
    version took whichever window came first in the list and stopped, and a mutant that
    deleted the stop survived because the fixture never had two windows in it. That is the
    08-02 lesson again. A fixture with one row per group cannot test a rule about choosing
    between rows.

    Lowest ceiling rather than first match, because a window is somebody saying noise is
    expected here. Two of them overlapping is two people saying it, and the answer to that
    is not to listen to whoever filed first.
    """
    out = []
    for alert in alerts:
        covering = [w for w in windows if w.covers(alert)]
        if not covering:
            out.append(alert)
            continue
        # max by rank, not min. rank counts down from page, so the quietest ceiling is
        # the highest rank. min here reads correctly in English and does the opposite.
        ceiling = max((w.ceiling for w in covering), key=rank)
        new = cap(alert.severity, ceiling)
        if new == alert.severity:
            out.append(alert)
            continue
        reasons = ", ".join(w.reason for w in covering)
        out.append(Alert(**{**alert.__dict__,
                            "severity": new,
                            "original_severity": alert.severity,
                            "suppressed_by": reasons}))
    return out


# The cold start rule, and why it is not shipped as a suppression.
#
# ot-018 asked for the first run of a (pipeline, task) in a process to be labelled so the
# duration monitor could stop firing on restarts. The label is now written, by the tracker,
# in obs_run.cold_start. Suppressing on it is a different question and the answer here is
# no. `cold_start_cost` is the measurement that says why, and the argument is in the
# README. Short version: this history is a backfill, so one run in 119 is cold. A daily
# schedule runs every partition in its own process, so every run is cold. A rule validated
# at 1 in 119 would silence the monitor completely in production and the data to tell the
# difference does not exist in this repo.


def cold_start_cost(observations, cold_flags):
    """What suppressing cold starts would remove, and what that number is worth.

    `observations` and `cold_flags` are parallel. Returns the counts plus the share of
    runs that carry the label, which is the number that decides whether the rule is safe.
    A share near 0 means the rule is cheap and untested. A share near 1 means it is a
    switch that turns the monitor off.
    """
    total = len(cold_flags)
    cold = sum(1 for f in cold_flags if f)
    return {
        "runs": total,
        "cold": cold,
        "share": (cold / total) if total else None,
        "warm": total - cold,
        "n_observations": len(observations),
    }


@dataclass
class Incident:
    """Every alert for one partition, collapsed into one thing a person reads.

    Twenty five watched signals across six columns will not fail one at a time. A schema
    change moves several of them at once and sending twenty five messages about one event
    is how an on call rota learns to filter the channel.
    """
    partition: Optional[date]
    alerts: list = field(default_factory=list)

    @property
    def severity(self):
        """The loudest alert in the group. An incident is as urgent as its worst part."""
        return min((a.severity for a in self.alerts), key=rank, default="info")

    @property
    def monitors(self):
        return sorted({a.monitor for a in self.alerts})

    def __len__(self):
        return len(self.alerts)


def group_incidents(alerts):
    """Collapse alerts into one incident per partition, newest partition last."""
    grouped = {}
    for alert in alerts:
        grouped.setdefault(alert.partition, Incident(alert.partition)).alerts.append(alert)
    ordered = sorted(grouped.values(),
                     key=lambda i: (i.partition is None, i.partition or date.min))
    return ordered


def counts_by_severity(alerts):
    out = {s: 0 for s in SEVERITIES}
    for alert in alerts:
        out[alert.severity] += 1
    return out


# The two band scheme, which is this project's answer to ot-017.
#
# The volume band fitted over the whole 119 partition history is 868 wide and 35 percent of
# that width is the feed's own growth trend rather than spread. Fitted over the last 56 it
# is 564 wide and fires five times as often. Day 4 moved the decision here on the grounds
# that alerting is the first consumer that pays for a band being wider than it needs to be.
#
# Having got here, the choice between them is a false one. A value outside the wide band is
# outside both and is unambiguous. A value between the two is unusual against recent traffic
# and normal against the year, which is a real state with a real name and it is not an
# emergency. So the wide band sets the page threshold and the narrow one sets the ticket
# threshold, and the trend that made the wide band too wide is exactly what makes it the
# right threshold for the louder of the two.


def two_band_verdict(wide, narrow, key, value):
    """Judge one value against a wide band and a narrow one.

    Returns `(status, severity_hint)` where status is `outside_both`, `between`,
    `inside_both`, or whatever refusal the wide band gave. The narrow band is only
    consulted when the wide band says the value is fine, because a value the wide band
    rejects does not become less serious for also being rejected by the narrow one.
    """
    outer = wide.check(key, value)
    if outer.status in CANNOT_JUDGE:
        return outer.status, "info"
    if outer.status in ("high", "low"):
        return "outside_both", "page"
    inner = narrow.check(key, value)
    if inner.status in CANNOT_JUDGE:
        return "inside_wide_only", "info"
    if inner.status in ("high", "low"):
        return "between", "ticket"
    return "inside_both", None


def two_band_counts(wide, narrow, observations):
    """How the history lands across the two bands. This is the number that says whether
    the middle region is a real state or an empty one."""
    counts = {}
    for key, value in observations:
        status, _ = two_band_verdict(wide, narrow, key, value)
        counts[status] = counts.get(status, 0) + 1
    return counts
