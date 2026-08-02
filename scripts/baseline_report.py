"""Measure the baseline's choices instead of asserting them.

Every configuration compared here is built by `obs.baseline.fit_bands`. None of the losing
sides are hand written. That rule exists because a comparison you construct yourself is a
thing you can accidentally rig, which happened on this repo two days ago when a per column
profiler baseline was measured against work it was not doing.

    python scripts/baseline_report.py --obs-db /tmp/obs.duckdb --chart docs/baseline.png

Six things get measured.

1. Whether the seasonal key pays for itself, per series. This is the one that decides the
   shape of the shipped baseline and it does not answer the same way for volume and for
   duration.
2. Raw against log space, and mean against median, on band width and fire rate.
3. What a single contaminated day does to each estimator. This is the whole argument for
   the median and it should be a number.
4. How much one observation moves a band edge, given seventeen observations per weekday.
5. The cold start, and what it would cost to widen a band far enough to hold it.
6. How often each configuration binds on its own training history.
"""

import argparse
import statistics as st
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from obs import baseline, history  # noqa: E402
from obs.history import WEEKDAY_NAMES  # noqa: E402

CONFIGS = [
    ("log", "median_mad"),
    ("log", "mean_sd"),
    ("raw", "median_mad"),
    ("raw", "mean_sd"),
]

CONTAMINATION = 2.0  # one day at twice its real volume, which is a doubled load


def rule(title):
    print(f"\n{title}\n{'-' * len(title)}")


def show_bands(bands, names=None):
    print(f"{'key':<12}{'n':>4}{'low':>12}{'centre':>12}{'high':>12}{'width':>12}")
    for key in sorted(bands, key=lambda k: (k is None, k)):
        band = bands[key]
        label = "pooled" if key is None else (names[key] if names else str(key))
        if band.degenerate:
            print(f"{label:<12}{band.n:>4}{'spread is zero, no band':>48}")
            continue
        print(f"{label:<12}{band.n:>4}{band.lo:>12.1f}{band.middle:>12.1f}"
              f"{band.hi:>12.1f}{band.width:>12.1f}")


def config_table(observations, label):
    rule(f"{label}: four configurations over the same observations")
    print(f"{'space':<6}{'estimator':<12}{'mean width':>12}{'fired':>8}{'of':>6}"
          f"{'rate':>8}")
    for space, estimator in CONFIGS:
        try:
            model = baseline.Baseline.fit(observations, space=space,
                                          estimator=estimator)
        except ValueError as exc:
            print(f"{space:<6}{estimator:<12}  cannot fit: {exc}")
            continue
        usable = [b for b in model.bands.values() if not b.degenerate]
        counts, rate = model.fire_rate(observations)
        width = st.mean([b.width for b in usable]) if usable else float("nan")
        fired = counts["high"] + counts["low"]
        print(f"{space:<6}{estimator:<12}{width:>12.1f}{fired:>8}"
              f"{sum(counts.values()):>6}{rate:>8.3f}")
        if counts["unbanded"]:
            print(f"{'':<18}{counts['unbanded']} judged against a band of zero width")


def contamination_test(observations, label):
    """Double one observation and see which estimator notices.

    The point is not that the band should ignore a doubled day. It is that a doubled day
    in the *training history* must not widen the band so far that the next one gets
    through. That is the failure mode of a mean and it is why the default is a median.
    """
    rule(f"{label}: one day doubled inside the training history")
    idx = 0
    key = observations[idx][0]
    dirty = list(observations)
    dirty[idx] = (key, observations[idx][1] * CONTAMINATION)
    print(f"contaminated one {WEEKDAY_NAMES[key]} from {observations[idx][1]:.0f} "
          f"to {dirty[idx][1]:.0f}")
    print(f"{'space':<6}{'estimator':<12}{'clean high':>12}{'dirty high':>12}"
          f"{'moved':>9}{'still caught':>14}")
    for space, estimator in CONFIGS:
        clean = baseline.Baseline.fit(observations, space=space, estimator=estimator)
        dirt = baseline.Baseline.fit(dirty, space=space, estimator=estimator)
        a, b = clean.bands.get(key), dirt.bands.get(key)
        if not a or not b or a.degenerate or b.degenerate:
            print(f"{space:<6}{estimator:<12}  no usable band")
            continue
        caught = dirt.check(key, dirty[idx][1]).status
        print(f"{space:<6}{estimator:<12}{a.hi:>12.1f}{b.hi:>12.1f}"
              f"{(b.hi / a.hi - 1) * 100:>8.1f}%{caught:>14}")


def window_table(observations, label, windows=(0, 84, 56, 42, 28)):
    """What the fitting window costs, in both directions.

    A band fitted over the whole history has every bit of trend in it counted as spread.
    A shorter window tracks the trend and has fewer observations per key to estimate a
    spread from, and with a weekday key every observation costs seven. Neither end of that
    is free and this prints both prices rather than picking one.

    The last column is the honest test. Each band judges the same recent 28 partitions, so
    the fire rates are comparable to each other in a way that a fire rate on each window's
    own training data is not.
    """
    rule(f"{label}: the fitting window")
    recent_28 = observations[-28:]
    print(f"{'window':>8}{'per key':>9}{'bands':>7}{'mean width':>12}"
          f"{'own history':>13}{'last 28':>10}")
    for days in windows:
        window = observations[-days:] if days else observations
        bands = baseline.fit_bands(window)
        label_days = "all" if not days else str(days)
        if not bands:
            print(f"{label_days:>8}{len(window) / 7:>9.0f}{0:>7}"
                  f"{'too few observations per key':>35}")
            continue
        model = baseline.Baseline(bands)
        width = st.mean([b.width for b in bands.values() if not b.degenerate])
        _, own = model.fire_rate(window)
        _, on_recent = model.fire_rate(recent_28)
        print(f"{label_days:>8}{len(window) / 7:>9.0f}{len(bands):>7}{width:>12.1f}"
              f"{own:>13.3f}{on_recent:>10.3f}")
    first = [v for _, v in observations[:28]]
    last = [v for _, v in observations[-28:]]
    print(f"\nthe series itself moves {st.mean(last) / st.mean(first) - 1:+.1%} from the "
          f"first 28 partitions to the last 28, so a band fitted over all of it is "
          f"holding that drift as if it were noise")


def resolution_floor(observations, label):
    """The weekday medians against the unit the measurement is recorded in.

    A between group difference smaller than the resolution is not a small effect, it is an
    effect nothing here could have seen. Worth printing next to any claim that a series
    has no seasonality.
    """
    rule(f"{label}: weekday medians against the recording resolution")
    grouped = {}
    for key, value in observations:
        grouped.setdefault(key, []).append(value)
    medians = {k: st.median(v) for k, v in sorted(grouped.items())}
    for key, med in medians.items():
        print(f"{WEEKDAY_NAMES[key]:<12}{med:>8.1f}")
    spread = max(medians.values()) - min(medians.values())
    overall = st.median([v for _, v in observations])
    if overall <= 0:
        print("the median is zero, so there is no resolution to compare against")
        return
    print(f"largest gap between weekday medians is {spread:.1f}, "
          f"against a median of {overall:.1f} recorded in whole units")
    print(f"so the resolution is {100 / overall:.1f} percent of a typical value and the "
          f"seasonal effect is {spread:.0f} of them")


def edge_stability(observations, label):
    rule(f"{label}: refitting each weekday with one observation held out")
    print(f"{'key':<12}{'n':>4}{'low edge range':>26}{'high edge range':>26}")
    for key in sorted({k for k, _ in observations}):
        out = baseline.leave_one_out_edges(observations, key)
        if out is None:
            print(f"{WEEKDAY_NAMES[key]:<12}  not enough observations")
            continue
        print(f"{WEEKDAY_NAMES[key]:<12}{out['n']:>4}"
              f"{out['lo_min']:>13.1f}{out['lo_max']:>13.1f}"
              f"{out['hi_min']:>13.1f}{out['hi_max']:>13.1f}")


def gain(observations, label):
    decision = baseline.choose_keying(observations)
    out = decision["gain"]
    if out is None:
        print(f"{label:<28} no usable comparison")
        return decision
    var = out["variance"]
    ratio = "n/a" if out["ratio"] is None else f"{out['ratio']:.3f}"
    keyed_width = "n/a" if out["keyed_width"] is None else f"{out['keyed_width']:.1f}"
    print(f"{label:<28}{keyed_width:>12}{out['pooled_width']:>14.1f}{ratio:>10}"
          f"{var['r2'] * 100:>11.1f}%{var['adjusted'] * 100:>11.1f}%"
          f"{out['degenerate_keys']:>7}{decision['keying']:>9}")
    print(f"{'':<28}{decision['reason']}")
    return decision


def cold_start(con, task):
    """The first run in a process, judged against a baseline built without it."""
    rule(f"{task}: the first run of the process against a band fitted without it")
    ordered = history.run_order(con, task=task)
    first_key, first_ms = ordered[0]
    slowest = max(range(len(ordered)), key=lambda i: ordered[i][1])
    runner_up = sorted((ms for _, ms in ordered), reverse=True)[1]
    print(f"slowest run is ordinal {slowest + 1} of {len(ordered)} at "
          f"{ordered[slowest][1]} ms, next slowest {runner_up} ms")
    rest = [(None, ms) for _, ms in ordered[1:] if ms]
    bands = baseline.fit_bands(rest, space="log", estimator="median_mad")
    band = bands[None]
    print(f"first run  {first_key}  {first_ms} ms")
    print(f"warm band  {band.lo:.1f} to {band.hi:.1f} ms, centre {band.middle:.1f}, "
          f"n={band.n}")
    if band.degenerate:
        print("the warm band has zero spread, so nothing can be scored against it")
        return
    score = band.score(first_ms)
    print(f"the first run sits {score:.1f} spreads from the centre, k is {band.k}")
    if score <= band.k:
        print("it is already inside the warm band, so this task has no cold start to "
              "handle. Whatever the first run pays for, this one does not pay it.")
        return
    wide = baseline.Band(key=None, centre=band.centre, spread=band.spread,
                         n=band.n, k=score, space=band.space,
                         estimator=band.estimator)
    print(f"holding it inside the band needs k={score:.1f}, which puts the high edge "
          f"at {wide.hi:.1f} ms")
    print(f"that is {wide.hi / band.middle:.0f}x the median run, so buying silence on "
          f"the restart costs every regression smaller than {wide.hi / band.middle:.0f}x")


def chart(volume_obs, duration_obs, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    model = baseline.Baseline.fit(history.keyed(volume_obs))
    pooled = baseline.fit_bands(history.unkeyed(volume_obs))[None]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    ax = axes[0]
    keys = sorted(model.bands)
    for i, key in enumerate(keys):
        band = model.bands[key]
        ax.plot([i, i], [band.lo, band.hi], color="#1f77b4", lw=8, alpha=0.35,
                solid_capstyle="butt")
        ax.plot([i - 0.3, i + 0.3], [band.middle] * 2, color="#1f77b4", lw=2)
    for key, value, _ in volume_obs:
        verdict = model.check(key, value)
        ax.plot(keys.index(key), value, "o", ms=4,
                color="#d62728" if verdict.status != "ok" else "#333333", alpha=0.8)
    ax.axhline(pooled.lo, color="#888888", ls="--", lw=1)
    ax.axhline(pooled.hi, color="#888888", ls="--", lw=1)
    ax.set_xticks(range(len(keys)))
    ax.set_xticklabels([WEEKDAY_NAMES[k][:3] for k in keys])
    ax.set_ylabel("orders in the partition")
    ax.set_title("Volume: seven seasonal bands against one pooled band (dashed)")

    ax = axes[1]
    ms = [v for _, v in duration_obs]
    ax.plot(range(len(ms)), ms, lw=0.9, color="#333333")
    warm = baseline.fit_bands([(None, v) for v in ms[1:] if v], space="log",
                              estimator="median_mad")[None]
    ax.axhspan(warm.lo, warm.hi, color="#1f77b4", alpha=0.18)
    ax.plot(0, ms[0], "o", ms=7, color="#d62728")
    ax.annotate(f"first run of the process, {ms[0]} ms", xy=(0, ms[0]),
                xytext=(len(ms) * 0.15, ms[0] * 0.75), color="#d62728",
                arrowprops={"arrowstyle": "->", "color": "#d62728"})
    ax.set_yscale("log")
    ax.set_xlabel("run ordinal within the process")
    ax.set_ylabel("load_raw duration, ms, log scale")
    ax.set_title("Duration: the warm band, and the run that does not live in it")

    fig.suptitle("Measured on generated order data, 119 partitions, 2 core sandbox")
    fig.tight_layout()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    print(f"\nwrote {path}")


def main():
    ap = argparse.ArgumentParser(description="measure the baseline's design choices")
    ap.add_argument("--obs-db", default="warehouse/obs.duckdb")
    ap.add_argument("--chart", default=None)
    args = ap.parse_args()

    con = duckdb.connect(args.obs_db, read_only=True)
    volume, v_skipped = history.volume_history(con)
    load_raw, l_skipped = history.duration_history(con, task="load_raw")
    build_daily, b_skipped = history.duration_history(con, task="build_daily")
    print(f"volume {len(volume)} observations, {v_skipped} skipped")
    print(f"load_raw {len(load_raw)} observations, {l_skipped} skipped")
    print(f"build_daily {len(build_daily)} observations, {b_skipped} skipped")

    rule("does the seasonal key pay for itself")
    print(f"{'series':<28}{'keyed width':>12}{'pooled width':>14}{'ratio':>10}"
          f"{'r2':>12}{'adjusted':>11}{'flat':>7}{'ships':>9}")
    gain(history.keyed(volume), "volume, raw_orders")
    gain(history.keyed(load_raw), "duration, load_raw")
    gain(history.keyed(build_daily), "duration, build_daily")
    print("\nratio below 1 means the keyed bands are tighter than one pooled band.")
    print("flat counts keys whose spread came out as zero, which is a band with no width.")

    config_table(history.keyed(volume), "volume")
    config_table(history.keyed(load_raw), "load_raw duration")
    contamination_test(history.keyed(volume), "volume")
    window_table(history.keyed(volume), "volume")
    resolution_floor(history.keyed(load_raw), "load_raw duration")
    edge_stability(history.keyed(volume), "volume")
    cold_start(con, "load_raw")
    cold_start(con, "build_daily")

    rule("what ships: volume keyed by weekday, duration pooled")
    model = baseline.Baseline.fit(history.keyed(volume))
    show_bands(model.bands, WEEKDAY_NAMES)
    counts, rate = model.fire_rate(history.keyed(volume))
    print(f"fires on {counts['high'] + counts['low']} of {len(volume)} training "
          f"observations, rate {rate:.3f}")
    for name, series in (("load_raw", load_raw), ("build_daily", build_daily)):
        pooled = baseline.Baseline.fit(history.unkeyed(series))
        print(f"\n{name} duration, pooled")
        show_bands(pooled.bands)
        counts, rate = pooled.fire_rate(history.unkeyed(series))
        print(f"fires on {counts['high'] + counts['low']} of {len(series)} training "
              f"observations, rate {rate:.3f}")

    if args.chart:
        chart(volume, history.keyed(load_raw), args.chart)
    con.close()


if __name__ == "__main__":
    main()
