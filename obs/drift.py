"""Distribution drift checks over the stored column metrics.

Like `obs/baseline.py` this imports no duckdb. It works on the values day 2 wrote into
`obs_column_metric` and knows nothing about how they got there. `obs/history.py` is the
part that talks to the database.

The blueprint line for today reads "distribution drift checks on key columns". Three
things a column offers look like drift signals. Two of them are not.

**The quantile vector cannot answer the question you want to ask.** The question is
whether today's distribution differs from the usual one, and the natural statistic is the
Kolmogorov Smirnov distance, the largest gap between the two cumulative functions. Seven
quantiles are seven points on the inverse of one cumulative function. They pin the curve
at seven places and say nothing about the shape between them. So a KS distance cannot be
computed here. It can only be bounded from below, which `ks_bound` does. The bound is
provably valid and on this feed it returns exactly zero for every pair of partitions in
the history. It can prove drift. It can never prove the absence of it.

`blind_spot` says how much drift the schema is structurally unable to see. It is the
largest gap between consecutive probabilities, because within one gap both cumulative
functions are free to take any shape while every stored quantile stays put. At the
probabilities day 2 chose that is 0.25, and `tests/test_drift.py` builds a pair of samples
whose seven quantiles agree and whose true KS distance is 0.25.

**`distinct_count` is a volume signal.** Its correlation with row count on `customer_id`
over the 119 partition history is +0.9999, and a weekday baseline fitted on it comes out
with the same shape as the volume baseline from day 3 to three decimal places. Banding it
produces a monitor that fires on a traffic change and calls it a cardinality problem.
Dividing by row count reduces the coupling and does not remove it, because the expected
number of distinct values in a sample is not linear in the sample size. `volume_coupling`
measures this rather than assuming it, and `usable_signals` refuses a signal whose
coupling is too high.

**Null rate and the categorical share vector do work.** Both are proportions of the
partition, both came out pooled rather than weekday keyed, and both move when the thing
they watch moves.

So what ships as a drift detector is the per probability quantile shift, the null rate and
the total variation distance on categorical shares. The KS bound ships next to them as a
certificate rather than a detector, and it is labelled that way everywhere it appears.
"""

import math
import statistics as st

from .baseline import DEFAULT_K, Baseline, Verdict, choose_keying
from .model import QUANTILE_PROBS


def prob_gaps(probs=QUANTILE_PROBS):
    """Gaps between consecutive probabilities, including the two tails.

    The tails count. Everything below the lowest probability is one unresolved region in
    the same way the middle gaps are, and on a heavy tailed column it is the region an
    incident is most likely to land in.
    """
    ordered = sorted(probs)
    return ([ordered[0]]
            + [b - a for a, b in zip(ordered, ordered[1:])]
            + [1.0 - ordered[-1]])


def blind_spot(probs=QUANTILE_PROBS):
    """Largest KS distance two columns can differ by with every stored quantile equal.

    Inside a gap of width g both cumulative functions have to start at the lower
    probability and reach the upper one at the same two x values, and between those they
    are unconstrained. One can hold nearly all the gap's mass at the bottom while the
    other holds it at the top, which separates them by g. So the answer is the largest
    gap and nothing about the data changes it.
    """
    return max(prob_gaps(probs))


def worst_case_pair(probs=QUANTILE_PROBS, n=20000):
    """Two samples whose stored quantiles agree and whose real KS distance is the blind
    spot.

    `blind_spot` says the largest gap is an upper limit on what the schema cannot see.
    That is an argument. This builds the pair that reaches it, so the limit is a fact the
    test suite checks rather than a claim the docstring makes.

    Both samples are the identity outside the widest gap, so a quantile there is its own
    probability. Inside the gap both pin the first and last point, which is what keeps the
    two boundary quantiles equal. Between those, one sample holds the mass at the bottom
    and the other holds it at the top. Returns `(low_heavy, high_heavy)`, both sorted.
    """
    ordered = sorted(probs)
    gaps = prob_gaps(ordered)
    widest = max(range(len(gaps)), key=lambda i: gaps[i])
    lo = 0.0 if widest == 0 else ordered[widest - 1]
    hi = 1.0 if widest == len(ordered) else ordered[widest]
    if hi - lo <= 0:
        raise ValueError("no gap to build a worst case in")

    step = (hi - lo) / 8.0
    low, high = [], []
    for i in range(n):
        u = (i + 0.5) / n
        if not (lo < u < hi):
            low.append(u)
            high.append(u)
            continue
        first = u - 1.0 / n <= lo
        last = u + 1.0 / n >= hi
        if first:
            # both pin the first interior point, which holds the lower quantile equal
            low.append(lo + step / 4)
            high.append(lo + step / 4)
        elif last:
            # and the last, which holds the upper one equal
            low.append(hi - step / 4)
            high.append(hi - step / 4)
        else:
            low.append(lo + step)
            high.append(hi - step)
    return sorted(low), sorted(high)


def empirical_ks(xs, ys):
    """Two sample KS distance computed from the full samples.

    Only used to check what the bound is missing. Nothing in the monitor can call this,
    because the whole point is that the raw rows are gone by the time a monitor runs and
    all that is left is seven numbers per column.
    """
    xs, ys = sorted(xs), sorted(ys)
    n, m = len(xs), len(ys)
    if not n or not m:
        return None
    import bisect
    best = 0.0
    for v in sorted(set(xs) | set(ys)):
        best = max(best, abs(bisect.bisect_right(xs, v) / n
                             - bisect.bisect_right(ys, v) / m))
    return best


def sample_quantiles(xs, probs=QUANTILE_PROBS):
    """Linear interpolated quantiles, matching what DuckDB's quantile_cont returns.

    Here so a test can compare a constructed sample against the vector the collector
    would have stored for it, without needing a database to do it.
    """
    xs = sorted(xs)
    if not xs:
        return None
    out = []
    for p in probs:
        pos = (len(xs) - 1) * p
        below = math.floor(pos)
        above = math.ceil(pos)
        if below == above:
            out.append(float(xs[below]))
        else:
            out.append(xs[below] * (above - pos) + xs[above] * (pos - below))
    return out


def ks_bound(ref_q, obs_q, probs=QUANTILE_PROBS):
    """Lower bound on the KS distance between two columns, from their quantile vectors.

    At x the quantile vector does not pin a cumulative function to a value. It pins it to
    an interval. `Q(p) = x` means `F(x) >= p`, and the next probability whose quantile is
    strictly above x is an upper limit. Only what is left over after both intervals are
    taken at their most generous is a real separation.

    The first version of this assumed `F(x) = p` at `x = Q(p)`, which is true for a
    continuous column and false the moment values repeat. On `item_count`, whose stored
    vector is `1 1 1 1 2 4 6`, it made two byte identical vectors bound apart by 0.49. A monitor built on it would have fired on every partition of every integer
    column forever and been unarguable, because the number was large and had a real
    theorem behind it.

    Returns 0.0 when the two intervals overlap everywhere, which is not evidence of
    agreement. See `blind_spot`.
    """
    probs = list(probs)
    if len(ref_q) != len(probs) or len(obs_q) != len(probs):
        raise ValueError(
            f"quantile vectors must have one value per probability, got "
            f"{len(ref_q)} and {len(obs_q)} for {len(probs)} probabilities"
        )

    def window(vector, x):
        """What the vector says F(x) is worth, as (lowest possible, highest possible)."""
        lo, hi = 0.0, 1.0
        for p, y in zip(probs, vector):
            if y <= x:
                lo = max(lo, p)
            if y > x:
                hi = min(hi, p)
        return lo, hi

    best = 0.0
    for x in list(ref_q) + list(obs_q):
        ref_lo, ref_hi = window(ref_q, x)
        obs_lo, obs_hi = window(obs_q, x)
        best = max(best, ref_lo - obs_hi, obs_lo - ref_hi)
    return max(best, 0.0)


def quantile_shift(ref_q, obs_q, scale=None):
    """Per probability shift of the stored quantile values, in units of `scale`.

    This is the reading that survives. It does not answer the distribution question, it
    answers a narrower one that the stored data can actually support: has the value at
    this probability moved. Scaled by a robust spread so a column measured in dollars and
    a column measured in items produce comparable numbers.

    Default scale is the reference interquartile range. It is robust and it is already
    in the vector, so nothing extra has to be stored to use it.
    """
    if len(ref_q) != len(obs_q):
        raise ValueError("quantile vectors must be the same length")
    if scale is None:
        scale = iqr(ref_q)
    if scale <= 0:
        raise ValueError(
            "quantile shift needs a positive scale. An interquartile range of zero means "
            "more than half the column is one value, and a shift in it cannot be "
            "expressed as a multiple of a spread that does not exist."
        )
    return [(o - r) / scale for r, o in zip(ref_q, obs_q)]


def iqr(q, probs=QUANTILE_PROBS):
    """Interquartile range from a quantile vector, by position rather than by lookup.

    The vector is ordered by probability and 0.25 and 0.75 are both in the stored set, so
    this reads two positions. It raises rather than guessing when they are not there,
    because a silently substituted pair of probabilities would change the units of every
    shift downstream.
    """
    probs = list(probs)
    try:
        lo = probs.index(0.25)
        hi = probs.index(0.75)
    except ValueError:
        raise ValueError(
            f"iqr needs 0.25 and 0.75 among the probabilities, have {probs}"
        ) from None
    return q[hi] - q[lo]


def max_abs_shift(ref_q, obs_q, scale=None):
    """The largest per probability shift. One scalar per column per partition."""
    return max(abs(s) for s in quantile_shift(ref_q, obs_q, scale))


def null_rate(null_count, row_count):
    """Nulls as a share of the partition. Undefined on an empty partition.

    Returning 0.0 for an empty partition would tell a monitor the column is perfectly
    populated at the moment there is nothing in it, which is the wrong answer to give
    about the one case that is definitely an incident.
    """
    if not row_count:
        return None
    return null_count / row_count


def shares(top_values, row_count):
    """Category shares from a stored top-k map.

    `top_values` is the top five by count and a column with more than five values loses
    the tail, so the shares do not sum to 1. The remainder is returned under a key of
    None rather than being folded into the last category or dropped. A monitor that
    silently renormalised would report a stable distribution while the tail it cannot see
    doubled.
    """
    if not row_count or top_values is None:
        return None
    out = {k: v / row_count for k, v in top_values.items()}
    seen = sum(out.values())
    out[None] = max(0.0, 1.0 - seen)
    return out


def total_variation(ref_shares, obs_shares):
    """Half the L1 distance between two share maps. Zero when identical, 1 when disjoint.

    Categories present in one and not the other count at their full share. That is the
    behaviour you want. A new payment status appearing is the thing a categorical monitor
    exists to catch, and it should not be made small by averaging it against four
    categories that did not move.
    """
    if ref_shares is None or obs_shares is None:
        return None
    keys = set(ref_shares) | set(obs_shares)
    return 0.5 * sum(abs(ref_shares.get(k, 0.0) - obs_shares.get(k, 0.0)) for k in keys)


def pearson(xs, ys):
    """Correlation coefficient. Returns None when either side has no spread."""
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mx, my = st.mean(xs), st.mean(ys)
    sx = sum((x - mx) ** 2 for x in xs)
    sy = sum((y - my) ** 2 for y in ys)
    if sx <= 0 or sy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(sx * sy)


def volume_coupling(values, row_counts):
    """How much of a candidate signal is really the partition's row count.

    A drift signal is supposed to say something about the shape of a column. If it tracks
    how many rows arrived then it is a volume monitor with a misleading name, and there is
    already a volume monitor. On `customer_id` distinct counts this returns +0.9999.
    """
    return pearson(values, row_counts)


# Above this a signal is refused. Set from the measurement rather than from taste. The
# signals that survive on this feed come in below 0.4 and the distinct counts come in
# above 0.99, so the gap is wide and any line drawn inside it gives the same answer. A
# feed where a real signal sat at 0.85 would need this argued again rather than nudged.
MAX_VOLUME_COUPLING = 0.9


def usable_signals(series, row_counts, limit=MAX_VOLUME_COUPLING):
    """Split candidate signals into ones worth banding and ones that are volume in
    disguise.

    `series` maps a signal name to its per partition values, in the same partition order
    as `row_counts`. Returns `(usable, refused)` where refused carries the measured
    coupling so a report can print why rather than asserting it.
    """
    usable, refused = {}, {}
    for name, values in series.items():
        clean = [(v, n) for v, n in zip(values, row_counts) if v is not None]
        if len(clean) < 2:
            refused[name] = {"reason": "not enough non null observations",
                             "coupling": None, "n": len(clean)}
            continue
        r = volume_coupling([v for v, _ in clean], [n for _, n in clean])
        if r is not None and abs(r) > limit:
            refused[name] = {"reason": "tracks row count", "coupling": r,
                             "n": len(clean)}
        else:
            usable[name] = {"coupling": r, "n": len(clean)}
    return usable, refused


def reference_quantiles(observations):
    """Elementwise median of the stored quantile vectors across the history.

    A median rather than a mean, for the reason the day-3 baseline uses one. The history
    contains whatever anomalies the feed already had and a reference built by averaging
    them in is a reference that has learned them.

    This is fitted on the same partitions the fire rate is then measured over, so that
    rate is a floor on the false alarm rate and not an estimate of the real one. Same
    caveat as day 3 and it stays until day 6 puts a held out failure in.
    """
    vectors = [o["quantiles"] for o in observations if o.get("quantiles")]
    if not vectors:
        return None
    keys = sorted(vectors[0], key=float)
    if any(sorted(v, key=float) != keys for v in vectors):
        raise ValueError(
            "quantile vectors in the history do not agree on their probabilities, so "
            "there is no reference they can share. QUANTILE_PROBS is fixed in model.py "
            "precisely so this cannot happen, which makes this a corrupt history."
        )
    return {k: st.median([v[k] for v in vectors]) for k in keys}


def reference_shares(observations):
    """Elementwise median of the category shares across the history."""
    vectors = [shares(o["top_values"], o["row_count"]) for o in observations]
    vectors = [v for v in vectors if v]
    if not vectors:
        return None
    keys = set()
    for v in vectors:
        keys |= set(v)
    return {k: st.median([v.get(k, 0.0) for v in vectors]) for k in keys}


def _ordered(qmap):
    """Quantile map to a vector ordered by probability."""
    return [qmap[k] for k in sorted(qmap, key=float)]


def signal_series(observations, probs=QUANTILE_PROBS):
    """Candidate drift signals for one column, one value per partition.

    Every signal here is a share of the partition or a shift measured in units of a
    spread, except the two distinct counts, which are included on purpose so the coupling
    check has something to refuse. Leaving them out would make `usable_signals` look like
    a formality.
    """
    ref_q = reference_quantiles(observations)
    ref_s = reference_shares(observations)
    scale = iqr(_ordered(ref_q), probs) if ref_q else None

    series = {}
    if ref_q and scale and scale > 0:
        ref_vec = _ordered(ref_q)
        series["quantile_shift"] = [
            max_abs_shift(ref_vec, _ordered(o["quantiles"]), scale)
            if o.get("quantiles") else None
            for o in observations
        ]
        series["ks_bound"] = [
            ks_bound(ref_vec, _ordered(o["quantiles"]), probs)
            if o.get("quantiles") else None
            for o in observations
        ]
    if ref_s:
        series["share_tv"] = [
            total_variation(ref_s, shares(o["top_values"], o["row_count"]))
            for o in observations
        ]
    series["null_rate"] = [null_rate(o["null_count"], o["row_count"])
                           for o in observations]
    series["distinct_count"] = [o["distinct_count"] for o in observations]
    series["distinct_ratio"] = [
        o["distinct_count"] / o["row_count"]
        if o["distinct_count"] is not None and o["row_count"] else None
        for o in observations
    ]
    return series


class Monitor:
    """Bands for the signals of one column that survived the coupling check.

    Thin on purpose. All the banding is `obs/baseline.py` and all the deciding is the
    functions above. What this adds is the record of which signals were refused and why,
    because a monitor that quietly watches four things when someone asked it to watch six
    is worse than one that says so.

    A signal whose history never moved is held as a constant rather than as a band. That
    is not a tidiness choice. A band from a robust spread of zero is degenerate, day 3
    made a degenerate band refuse to judge, and the signals that come out flat here are
    the ones whose first movement matters most. `status` has held four distinct values
    for all 119 partitions and a fifth appearing is the incident a categorical monitor
    exists for. `order_amount_usd` has never had a null. Under a band both stay silent
    forever. Under equality both fire on the first change, which is the right answer for
    a signal whose reference point is known without needing a spread.
    """

    def __init__(self, column, bands, constants, refused, keying, reference):
        self.column = column
        self.bands = bands
        self.constants = constants
        self.refused = refused
        self.keying = keying
        self.reference = reference

    @classmethod
    def fit(cls, column, observations, k=DEFAULT_K, limit=MAX_VOLUME_COUPLING):
        series = signal_series(observations)
        row_counts = [o["row_count"] for o in observations]
        weekdays = [o["weekday"] for o in observations]
        usable, refused = usable_signals(series, row_counts, limit)

        bands, constants, keying = {}, {}, {}
        for name in usable:
            pairs = [(w, v) for w, v in zip(weekdays, series[name]) if v is not None]
            if not pairs:
                continue
            values = {v for _, v in pairs}
            if len(values) == 1:
                constants[name] = {"value": pairs[0][1], "n": len(pairs)}
                continue
            # raw space, not log. these signals are shares and scaled shifts, several of
            # which are legitimately zero, and log space would raise on the first one.
            decision = choose_keying(pairs, k=k, space="raw")
            keying[name] = decision
            fitted = pairs if decision["keying"] == "keyed" else [(None, v)
                                                                 for _, v in pairs]
            bands[name] = Baseline.fit(fitted, k=k, space="raw")
        return cls(column, bands, constants, refused,
                   keying, {"quantiles": reference_quantiles(observations),
                            "shares": reference_shares(observations)})

    def watched(self):
        """Every signal this monitor will actually judge, banded or constant."""
        return sorted(set(self.bands) | set(self.constants))

    def check(self, name, weekday, value):
        """Judge one signal on one partition.

        A signal this monitor does not hold returns None rather than a passing verdict.
        A caller that cannot tell "fine" from "not watched" will eventually report the
        second as the first.
        """
        if name in self.constants:
            expected = self.constants[name]["value"]
            return Verdict(key=None, value=value, expected=expected,
                           status="ok" if value == expected else "changed")
        baseline = self.bands.get(name)
        if baseline is None:
            return None
        key = weekday if self.keying[name]["keying"] == "keyed" else None
        return baseline.check(key, value)
