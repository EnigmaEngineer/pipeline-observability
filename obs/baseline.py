"""Seasonal baselines for volume and run duration.

Nothing in this file imports duckdb. It works on lists of `(key, value)` pairs and knows
nothing about where they came from. `obs/history.py` is the part that talks to the
metadata tables. The split is deliberate. Every band, every estimator and every decision
below can be tested against a list built inside a test, and the only thing needing a
database is the query that produced the list.

A band is a centre and a spread in some space, widened by k. Four things about it are
choices rather than defaults, and each one is measured in `scripts/baseline_report.py`
rather than asserted here.

**The space.** Raw or log. The generator that feeds this project is multiplicative, day of
week factor times trend times lognormal noise, so an additive band on raw counts is fitting
the wrong shape. Log space turns that into an additive problem. Both are built from the
same code path so the comparison cannot be rigged by hand writing the losing side.

**The estimator.** Mean with standard deviation, or median with a scaled MAD. The mean is
the wrong tool for a baseline whose training history contains the anomalies it is supposed
to catch. One doubled day inflates the standard deviation and hides the next one. The MAD
does not move. That is the argument. The measurement of how much it does not move is in
the report.

**Whether the seasonal key earns its place at all.** Splitting 119 observations into seven
groups of 17 is only worth it if the between group difference is larger than the within
group spread. For volume it is, by a wide margin. For duration it is not, and that is the
day's real finding. `seasonal_gain` is the function that decides this, and it is meant to
be run before trusting a keyed baseline rather than after.

**What a zero spread means.** More than half of an integer millisecond duration column can
be the same value, and then the MAD is exactly 0 and the band collapses to a point. A band
of zero width is not a conservative band, it is a monitor that fires on everything. So a
degenerate band is marked and refuses to judge, rather than quietly becoming an alert
generator. This is not hypothetical here. It happens on `build_daily`.
"""

import math
import statistics as st
from dataclasses import dataclass

# Three sigma. Not tuned, and it should not be read as tuned. It is the conventional
# starting point and the report measures what it costs, which is the number that matters.
DEFAULT_K = 3.0

# Below this many observations a band is not built at all. Seven is one weekday cycle. A
# band from three points has a spread that is mostly an accident of which three.
MIN_OBSERVATIONS = 7

# Scales the MAD so it estimates the same quantity as the standard deviation on normal
# data. The constant is 1 over the 75th percentile of the standard normal, because for a
# normal sample the median absolute deviation converges on 0.6745 sigma. Without it a
# median band and a mean band are not comparable and the report would be measuring this
# constant rather than the estimators. It is only exact for normal data, so on a skewed
# series the two estimators are near enough to compare and not the same quantity.
MAD_TO_SIGMA = 1.4826


def _log(v):
    if v <= 0:
        raise ValueError(
            f"log space needs strictly positive values, got {v!r}. A task fast enough "
            "to record 0 ms cannot be fitted here. The answer is to record a finer unit, "
            "not to invent a floor."
        )
    return math.log(v)


SPACES = {
    "raw": (lambda v: float(v), lambda v: v),
    "log": (_log, math.exp),
}


def mean_sd(xs):
    """Sample standard deviation. Needs two points and says so."""
    if len(xs) < 2:
        raise ValueError("mean_sd needs at least two observations")
    return st.mean(xs), st.stdev(xs)


def median_mad(xs):
    """Median absolute deviation, scaled to compare with a standard deviation.

    Breakdown point is 50 percent. Half the training history can be garbage and the centre
    still lands in the right place. The standard deviation moves on one bad point.
    """
    if not xs:
        raise ValueError("median_mad needs at least one observation")
    centre = st.median(xs)
    return centre, st.median([abs(x - centre) for x in xs]) * MAD_TO_SIGMA


ESTIMATORS = {"mean_sd": mean_sd, "median_mad": median_mad}


@dataclass
class Band:
    key: object
    centre: float          # in the fitting space
    spread: float          # in the fitting space
    n: int
    k: float
    space: str
    estimator: str

    @property
    def degenerate(self):
        """A spread of zero. The band has no width and cannot judge anything."""
        return self.spread <= 0

    def _out(self, x):
        return SPACES[self.space][1](x)

    @property
    def lo(self):
        return self._out(self.centre - self.k * self.spread)

    @property
    def hi(self):
        return self._out(self.centre + self.k * self.spread)

    @property
    def middle(self):
        return self._out(self.centre)

    @property
    def width(self):
        """Width in the original units, which is the only width a reader can act on.

        In log space the band is not symmetric around the centre once it comes back out,
        so this is a real width and not twice a half width.
        """
        return self.hi - self.lo

    def score(self, value):
        """Signed distance from the centre in units of the spread."""
        return (SPACES[self.space][0](value) - self.centre) / self.spread


@dataclass
class Verdict:
    key: object
    value: float
    status: str            # ok, high, low, unbanded, unknown_key, changed
    score: float = None
    band: Band = None
    # What the value was compared against when there is no band. Only a constant signal
    # sets it. Day 5 needs it because a constant that moved up and a constant that moved
    # down are different incidents, and 'changed' on its own cannot say which.
    expected: object = None


def fit_bands(observations, k=DEFAULT_K, space="log", estimator="median_mad",
              min_n=MIN_OBSERVATIONS):
    """Build one band per key from `(key, value)` pairs.

    Every configuration goes through here. There is no second path for the comparison
    cases in the report, because a baseline that loses a measurement against a version I
    wrote separately has lost to my typing rather than to the design.
    """
    if space not in SPACES:
        raise ValueError(f"unknown space {space!r}, have {sorted(SPACES)}")
    if estimator not in ESTIMATORS:
        raise ValueError(f"unknown estimator {estimator!r}, have {sorted(ESTIMATORS)}")
    forward = SPACES[space][0]
    estimate = ESTIMATORS[estimator]

    grouped = {}
    for key, value in observations:
        grouped.setdefault(key, []).append(forward(value))

    bands = {}
    for key, xs in grouped.items():
        if len(xs) < min_n:
            continue
        centre, spread = estimate(xs)
        bands[key] = Band(key=key, centre=centre, spread=spread, n=len(xs),
                          k=k, space=space, estimator=estimator)
    return bands


class Baseline:
    """Bands plus the rule for reading a value against them.

    It holds the bands and nothing else. The first version also carried `space`,
    `estimator` and `k` on the object, and nothing ever read any of them because every
    band already carries its own. Three attributes that exist to make a class look
    complete are three things a reader has to check are consistent.
    """

    def __init__(self, bands):
        self.bands = bands

    @classmethod
    def fit(cls, observations, k=DEFAULT_K, space="log", estimator="median_mad",
            min_n=MIN_OBSERVATIONS):
        return cls(fit_bands(observations, k=k, space=space, estimator=estimator,
                             min_n=min_n))

    def check(self, key, value):
        """Judge one value.

        A key with no band and a band with no width are different failures and they get
        different words. Collapsing them into `high` or `ok` is how a monitor ends up
        reporting confidence it does not have.
        """
        band = self.bands.get(key)
        if band is None:
            return Verdict(key=key, value=value, status="unknown_key")
        if band.degenerate:
            return Verdict(key=key, value=value, status="unbanded", band=band)
        score = band.score(value)
        if score > band.k:
            status = "high"
        elif score < -band.k:
            status = "low"
        else:
            status = "ok"
        return Verdict(key=key, value=value, status=status, score=score, band=band)

    def fire_rate(self, observations):
        """How often the band binds on a set of observations.

        A monitor that never fires on its own training history is not safe, it is
        uninformative, and the only way to know which one you have is to count. Runs on
        the training data by design here, so the number is a floor on the false alarm
        rate rather than an estimate of the true one.
        """
        counts = {"ok": 0, "high": 0, "low": 0, "unbanded": 0, "unknown_key": 0}
        for key, value in observations:
            counts[self.check(key, value).status] += 1
        total = sum(counts.values())
        fired = counts["high"] + counts["low"]
        return counts, (fired / total if total else 0.0)


HOLDOUT_SPLIT = 0.7
MIN_HOLDOUT_TRAIN = 14


def holdout_fire_rate(observations, split=HOLDOUT_SPLIT, min_train=MIN_HOLDOUT_TRAIN,
                      **fit_kwargs):
    """Fit on the front of a series and count fires on the back of it.

    `observations` are `(key, value, date)` triples in order, the shape `history` returns.
    Returns `(counts, rate, n_train, n_test)`, or None when there is not enough history to
    split. Returning None rather than a rate matters, because `page_eligible` refuses an
    unknown rate and would silently approve a zero.

    **This exists because the number `page_eligible` was reading for volume was in sample
    until day 7.** Day 5 established that the gate needs an out of sample rate, fixed every
    drift signal, and left volume alone. Volume is the only subject the policy lets page. The
    band was fitted on all 119 partitions and counted on the last 56 of those same 119, which
    reads 0.036 and clears the 0.05 limit. Split properly at 83 and 36 it reads 0.083 and does
    not, and on the ten unseen partitions of the injection harness it is 0.300. So the one
    estimate that approved a page was the only one measured on partitions the fit had seen.

    It lives here rather than in `scripts/alert_report.py` for a duller reason. A measurement
    that decides a severity has to be reachable by a test, and nothing in `tests/` imports a
    script.
    """
    if not observations:
        return None
    cut = int(len(observations) * split)
    train, test = observations[:cut], observations[cut:]
    if len(train) < min_train or not test:
        return None
    band = Baseline.fit([(k, v) for k, v, _ in train], **fit_kwargs)
    counts, rate = band.fire_rate([(k, v) for k, v, _ in test])
    return counts, rate, len(train), len(test)


def variance_explained(observations):
    """Fraction of the variance in the values that the key accounts for.

    This is a one way analysis of variance written out, and it is here rather than pulled
    from a library because the whole project has one dependency and adding scipy to
    compute a ratio of two sums of squares is not a trade worth making.

    The adjusted figure charges for the degrees of freedom the group means cost. With
    seven groups and 119 points the difference is small. With seven groups and 14 points
    it would not be, and that is the case this number exists to expose.
    """
    values = [v for _, v in observations]
    n = len(values)
    grouped = {}
    for key, value in observations:
        grouped.setdefault(key, []).append(value)
    groups = len(grouped)
    if n < 2 or groups < 2:
        return None
    grand = st.mean(values)
    total_ss = sum((v - grand) ** 2 for v in values)
    if total_ss == 0:
        return None
    between_ss = sum(len(g) * (st.mean(g) - grand) ** 2 for g in grouped.values())
    r2 = between_ss / total_ss
    df_error = n - groups
    if df_error <= 0:
        return None
    adjusted = 1 - (1 - r2) * (n - 1) / df_error
    return {"r2": r2, "adjusted": adjusted, "groups": groups, "n": n,
            "pooled_sd": math.sqrt(total_ss / (n - 1)),
            "residual_sd": math.sqrt((total_ss - between_ss) / df_error)}


def seasonal_gain(observations, k=DEFAULT_K, space="log", estimator="median_mad"):
    """Decide whether the seasonal key is worth having, by measuring it.

    Compares the mean width of the keyed bands against the width of one pooled band over
    the same observations. Both come from `fit_bands`, so the pooled case is the same code
    with the key thrown away rather than a second implementation.

    A ratio near 1 means the key bought nothing and the pooled band is the honest choice.
    Below 1 means the keyed bands are tighter, which is the only reason to pay for seven
    of them instead of one.
    """
    keyed = fit_bands(observations, k=k, space=space, estimator=estimator)
    pooled = fit_bands([(None, v) for _, v in observations], k=k, space=space,
                       estimator=estimator)
    if not keyed or None not in pooled:
        return None
    pooled_band = pooled[None]
    usable = [b for b in keyed.values() if not b.degenerate]
    if not usable or pooled_band.degenerate:
        return {"keyed_width": None, "pooled_width": pooled_band.width,
                "ratio": None, "degenerate_keys": len(keyed) - len(usable),
                "variance": variance_explained(observations)}
    keyed_width = st.mean([b.width for b in usable])
    return {
        "keyed_width": keyed_width,
        "pooled_width": pooled_band.width,
        "ratio": keyed_width / pooled_band.width,
        "degenerate_keys": len(keyed) - len(usable),
        "variance": variance_explained(observations),
    }


# A keyed baseline has to be meaningfully tighter than one pooled band to be worth seven
# bands instead of one. This is where the line sits. It is a choice and it is not
# load bearing on this data, because volume comes in at 0.41 and both durations come in
# above 1.0, so anything between 0.5 and 1.0 gives the same two answers.
SEASONAL_KEY_MIN_GAIN = 0.9


def choose_keying(observations, k=DEFAULT_K, space="log", estimator="median_mad"):
    """Say whether this series should be banded per key or pooled, and show the working.

    The blueprint line for this project reads "seasonal baseline model for volume and
    duration" as if seasonality were a property of the project rather than of a series.
    It is not. Volume here is strongly weekly and duration is not weekly at all, so this
    is a measurement and not a setting.
    """
    out = seasonal_gain(observations, k=k, space=space, estimator=estimator)
    if out is None or out["ratio"] is None:
        return {"keying": "pooled", "reason": "no usable keyed bands", "gain": out}
    if out["degenerate_keys"]:
        return {"keying": "pooled",
                "reason": f"{out['degenerate_keys']} keys came out with zero spread",
                "gain": out}
    if out["ratio"] <= SEASONAL_KEY_MIN_GAIN:
        return {"keying": "keyed",
                "reason": f"keyed bands are {(1 - out['ratio']) * 100:.0f} percent "
                          "narrower than one pooled band",
                "gain": out}
    return {"keying": "pooled",
            "reason": f"keyed bands are {(out['ratio'] - 1) * 100:.0f} percent wider "
                      "than one pooled band, so the key costs rather than pays",
            "gain": out}


def leave_one_out_edges(observations, key, k=DEFAULT_K, space="log",
                        estimator="median_mad"):
    """Refit one key's band with each of its observations dropped in turn.

    Seventeen points per weekday is not many. This says how much a single observation
    moves the edge, which is the honest way to report the uncertainty in a band without
    pretending a bootstrap on seventeen points is a confidence interval.
    """
    xs = [v for k_, v in observations if k_ == key]
    if len(xs) < MIN_OBSERVATIONS + 1:
        return None
    edges = []
    for i in range(len(xs)):
        held = [(key, v) for j, v in enumerate(xs) if j != i]
        bands = fit_bands(held, k=k, space=space, estimator=estimator,
                          min_n=MIN_OBSERVATIONS)
        band = bands.get(key)
        if band and not band.degenerate:
            edges.append((band.lo, band.hi))
    if not edges:
        return None
    los = [e[0] for e in edges]
    his = [e[1] for e in edges]
    return {"n": len(xs), "lo_min": min(los), "lo_max": max(los),
            "hi_min": min(his), "hi_max": max(his)}
