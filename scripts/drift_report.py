"""Measure every choice the drift checks make, rather than asserting them in a README.

Same job as `scripts/baseline_report.py` does for day 3. Every number the README quotes
about drift comes out of here, so a claim in the docs is a claim something committed will
reproduce.

    python scripts/drift_report.py --obs-db /tmp/obs.duckdb --chart docs/drift.png

Seven sections, and five of them exist because the obvious thing turned out to be wrong.

    signals        which candidate signals are really volume, measured not assumed
    keying         whether a drift signal needs the weekday key that volume needed
    blind spot     how much drift seven quantiles structurally cannot see
    bound          how often the KS lower bound binds on real partitions
    trend          whether these signals drift over the window the way volume does
    noise          whether they move because the partitions got bigger
    fire           how often each surviving band binds on its own training history
"""

import argparse
import math
import statistics as st
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from obs import drift, history  # noqa: E402
from obs.baseline import choose_keying  # noqa: E402
from obs.model import QUANTILE_PROBS  # noqa: E402

# The columns worth watching, and why these. order_amount_usd and item_count are the two
# numeric columns a business would notice moving. coupon_code is the only column with a
# meaningful null rate. status and channel are categorical with a small fixed vocabulary,
# which is the shape a new-category incident shows up in. customer_id is here because it
# is the high cardinality case and it is the one that fails.
WATCHED = ["order_amount_usd", "item_count", "coupon_code", "status", "channel",
           "customer_id"]


def fmt(v, places=4):
    return "n/a" if v is None else f"{v:.{places}f}"


def signals_table(observations_by_column):
    print("\n== signals: which of these is actually a drift signal ==")
    print("correlation with the partition row count. a signal that tracks row count is a")
    print("volume monitor under another name, and day 3 already built the volume monitor.")
    print(f"{'column':<18}{'signal':<17}{'coupling':>10}  verdict")
    kept = {}
    for column, obs in observations_by_column.items():
        series = drift.signal_series(obs)
        row_counts = [o["row_count"] for o in obs]
        usable, refused = drift.usable_signals(series, row_counts)
        kept[column] = (series, usable, refused)
        for name in sorted(series):
            if name in usable:
                values = {v for v in series[name] if v is not None}
                verdict = "keep, constant so far" if len(values) == 1 else "keep"
                coupling = usable[name]["coupling"]
            else:
                verdict = "REFUSED, " + refused[name]["reason"]
                coupling = refused[name]["coupling"]
            print(f"{column:<18}{name:<17}{fmt(coupling):>10}  {verdict}")
    return kept


def keying_table(observations_by_column, kept):
    print("\n== keying: does a drift signal need the weekday key that volume needed ==")
    print("day 3 found volume strongly weekly and duration not weekly at all. this is the")
    print("same question asked again per signal, through the same choose_keying call.")
    print(f"{'column':<18}{'signal':<17}{'ratio':>8}{'r2':>8}{'adj':>8}  keying")
    for column, (series, usable, _refused) in kept.items():
        obs = observations_by_column[column]
        for name in sorted(usable):
            pairs = [(o["weekday"], v) for o, v in zip(obs, series[name])
                     if v is not None]
            if len(pairs) < 2:
                continue
            decision = choose_keying(pairs, space="raw")
            gain = decision["gain"] or {}
            var = gain.get("variance") or {}
            print(f"{column:<18}{name:<17}"
                  f"{fmt(gain.get('ratio'), 3):>8}"
                  f"{fmt(var.get('r2'), 3):>8}"
                  f"{fmt(var.get('adjusted'), 3):>8}  {decision['keying']}")


def blind_spot_section():
    print("\n== blind spot: what seven quantiles structurally cannot see ==")
    gaps = drift.prob_gaps()
    print(f"probabilities   {list(QUANTILE_PROBS)}")
    print(f"gaps            {[round(g, 3) for g in gaps]}")
    print(f"largest gap     {drift.blind_spot():.2f}")
    print("inside one gap both cumulative functions are pinned at the two ends and free")
    print("in between, so they can separate by the whole gap with every stored quantile")
    print("equal. that is an argument. below is the pair that reaches it.")
    low, high = drift.worst_case_pair(n=20000)
    ql = drift.sample_quantiles(low)
    qh = drift.sample_quantiles(high)
    worst = max(abs(a - b) for a, b in zip(ql, qh))
    print(f"  largest disagreement between the two stored vectors  {worst:.2e}")
    print(f"  KS lower bound the monitor would compute             "
          f"{drift.ks_bound(ql, qh):.4f}")
    print(f"  true KS distance between the two samples             "
          f"{drift.empirical_ks(low, high):.4f}")


def bound_section(observations_by_column, warehouse=None):
    print("\n== bound: how often the KS lower bound binds on real partitions ==")
    for column, obs in observations_by_column.items():
        vectors = [o for o in obs if o.get("quantiles")]
        if len(vectors) < 2:
            continue
        probs = QUANTILE_PROBS
        pairs = 0
        nonzero = 0
        largest = 0.0
        for a, b in zip(vectors, vectors[1:]):
            va = [a["quantiles"][k] for k in sorted(a["quantiles"], key=float)]
            vb = [b["quantiles"][k] for k in sorted(b["quantiles"], key=float)]
            bound = drift.ks_bound(va, vb, probs)
            pairs += 1
            nonzero += bound > 0
            largest = max(largest, bound)
        print(f"  {column:<18} {nonzero:>3} of {pairs} consecutive pairs bound above "
              f"zero, largest {largest:.4f}")

    if warehouse is None:
        print("  (no warehouse given, so the bound is not compared against a true KS)")
        return
    print("\n  against the true KS computed from the rows themselves:")
    con = duckdb.connect(warehouse, read_only=True)
    days = [r[0] for r in con.execute(
        "SELECT DISTINCT dt FROM raw_orders ORDER BY dt LIMIT 9").fetchall()]
    ref_rows = [r[0] for r in con.execute(
        "SELECT order_amount_usd FROM raw_orders WHERE dt = ?", [days[0]]).fetchall()]
    ref_q = drift.sample_quantiles(ref_rows)
    for day in days[1:]:
        rows = [r[0] for r in con.execute(
            "SELECT order_amount_usd FROM raw_orders WHERE dt = ?", [day]).fetchall()]
        true = drift.empirical_ks(ref_rows, rows)
        bound = drift.ks_bound(ref_q, drift.sample_quantiles(rows))
        print(f"    {days[0]} vs {day}   true {true:.4f}   bound {bound:.4f}")
    con.close()


def trend_section(observations_by_column, kept, window=28):
    print(f"\n== trend: do these signals drift across the window, as volume does ==")
    print("ot-017 says the volume band holds 35 percent of its width as trend rather than")
    print("variability. the same question has to be answered for every signal here and")
    print("the answer is not the same one.")
    print(f"{'column':<18}{'signal':<17}{'first' + str(window):>12}"
          f"{'last' + str(window):>12}{'change':>9}")
    for column, (series, usable, _refused) in kept.items():
        obs = observations_by_column[column]
        for name in sorted(usable):
            values = [v for v in series[name] if v is not None]
            if len(values) < 2 * window:
                continue
            first = st.median(values[:window])
            last = st.median(values[-window:])
            change = (last / first - 1) * 100 if first else None
            print(f"{column:<18}{name:<17}{first:>12.5f}{last:>12.5f}"
                  f"{fmt(change, 2) + '%':>9}")
    rows = [o["row_count"] for o in next(iter(observations_by_column.values()))]
    print(f"{'(row_count)':<18}{'volume':<17}"
          f"{st.median(rows[:window]):>12.5f}{st.median(rows[-window:]):>12.5f}"
          f"{(st.median(rows[-window:]) / st.median(rows[:window]) - 1) * 100:>8.2f}%")


def noise_section(observations_by_column, kept):
    """The second order version of ot-017, and the reason the first order version is not
    a problem here.

    A signal like `quantile_shift` or `share_tv` is a distance from a fixed reference, so
    on a feed that is not drifting its typical size is not a property of the data at all.
    It is sampling error, and sampling error falls as one over the square root of the
    partition size. Volume grows across this window, so these signals shrink across it
    without anything about the distributions changing.

    Bucketed by row count rather than by date on purpose. Bucketing by date confounds the
    partition size with whatever else moved over four months, and the claim here is
    specifically about size.
    """
    print("\n== noise: the level does not trend, the noise does ==")
    print("a distance from a fixed reference is sampling error when nothing is wrong, and")
    print("sampling error falls as one over the square root of the partition size. these")
    print("partitions are bucketed by row count, not by date, so nothing but size varies.")
    print(f"{'column':<18}{'signal':<15}{'small n':>9}{'large n':>9}"
          f"{'observed':>10}{'predicted':>11}")
    for column, (series, usable, _refused) in kept.items():
        obs = observations_by_column[column]
        for name in ("quantile_shift", "share_tv"):
            if name not in usable:
                continue
            pairs = [(o["row_count"], v) for o, v in zip(obs, series[name])
                     if v is not None]
            if len({v for _, v in pairs}) <= 1 or len(pairs) < 12:
                continue
            pairs.sort()
            third = len(pairs) // 3
            small, large = pairs[:third], pairs[-third:]
            v_small = st.median([v for _, v in small])
            v_large = st.median([v for _, v in large])
            n_small = st.median([n for n, _ in small])
            n_large = st.median([n for n, _ in large])
            if not v_small or not n_small:
                continue
            observed = v_large / v_small
            predicted = math.sqrt(n_small / n_large)
            print(f"{column:<18}{name:<15}{v_small:>9.4f}{v_large:>9.4f}"
                  f"{observed:>10.3f}{predicted:>11.3f}")
    print("  a ratio near the predicted column means the signal moved because the")
    print("  partitions got bigger, not because anything drifted.")


def fire_section(observations_by_column, kept):
    print("\n== fire: how often each band binds on its own training history ==")
    print("measured on the partitions the band was fitted on, so this is a floor on the")
    print("false alarm rate and not an estimate of it. holds until day 6.")
    print("a signal that never moved is held as a constant and fires on any change, which")
    print("is why several rows below read 0.000 and are not asleep.")
    print(f"{'column':<18}{'signal':<17}{'kind':>10}{'fired':>7}{'rate':>8}")
    monitors = {}
    for column, obs in observations_by_column.items():
        monitor = drift.Monitor.fit(column, obs)
        monitors[column] = monitor
        series = kept[column][0]
        for name in monitor.watched():
            pairs = [(o["weekday"], v) for o, v in zip(obs, series[name])
                     if v is not None]
            if name in monitor.constants:
                fired = sum(monitor.check(name, w, v).status != "ok" for w, v in pairs)
                kind = "constant"
                rate = fired / len(pairs) if pairs else 0.0
            else:
                kind = monitor.keying[name]["keying"]
                fitted = pairs if kind == "keyed" else [(None, v) for _, v in pairs]
                counts, rate = monitor.bands[name].fire_rate(fitted)
                fired = counts["high"] + counts["low"]
            print(f"{column:<18}{name:<17}{kind:>10}{fired:>7}{rate:>8.3f}")
    return monitors


def chart(path, observations_by_column, kept):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print(f"\nno matplotlib, skipping {path}")
        return
    obs = observations_by_column["order_amount_usd"]
    cust = observations_by_column["customer_id"]
    dates = [o["date"] for o in obs]

    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    shift = kept["order_amount_usd"][0]["quantile_shift"]
    axes[0].plot(dates, shift, lw=0.9, color="#1f77b4")
    axes[0].set_ylabel("max quantile shift\n(IQR units)")
    axes[0].set_title("a drift signal that is one: order_amount_usd quantile shift")

    ax2 = axes[1]
    distinct = [o["distinct_count"] for o in cust]
    ax2.plot(dates, distinct, lw=0.9, color="#d62728", label="customer_id distinct")
    ax2b = ax2.twinx()
    ax2b.plot(dates, [o["row_count"] for o in cust], lw=0.9, ls="--",
              color="#333333", label="row_count")
    ax2.set_ylabel("distinct")
    ax2b.set_ylabel("rows")
    r = drift.volume_coupling(distinct, [o["row_count"] for o in cust])
    ax2.set_title(f"a drift signal that is not: distinct_count against row count, "
                  f"r = {r:+.4f}")
    fig.autofmt_xdate()
    fig.tight_layout()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=130)
    print(f"\nchart written to {path}")


def main():
    ap = argparse.ArgumentParser(description="measure the drift checks")
    ap.add_argument("--obs-db", default="warehouse/obs.duckdb")
    ap.add_argument("--db", default=None,
                    help="pipeline warehouse, for the true KS comparison")
    ap.add_argument("--dataset", default="raw_orders")
    ap.add_argument("--chart", default=None)
    ap.add_argument("--window", type=int, default=28)
    args = ap.parse_args()

    # read only, and not through store.connect, because that applies the DDL and a
    # report has no business creating tables in the database it is reading.
    con = duckdb.connect(args.obs_db, read_only=True)
    observations_by_column = {}
    for column in WATCHED:
        obs, skipped = history.column_history(con, args.dataset, column)
        if not obs:
            print(f"no history for {column}, skipping")
            continue
        if skipped:
            print(f"{column}: {skipped} rows had an unreadable partition key")
        observations_by_column[column] = obs
    con.close()

    if not observations_by_column:
        print("no column history at all. run scripts/run_observed.py first.")
        return 1

    n = len(next(iter(observations_by_column.values())))
    print(f"{len(observations_by_column)} columns over {n} partitions "
          f"from {args.obs_db}")

    kept = signals_table(observations_by_column)
    keying_table(observations_by_column, kept)
    blind_spot_section()
    bound_section(observations_by_column, args.db)
    trend_section(observations_by_column, kept, args.window)
    noise_section(observations_by_column, kept)
    fire_section(observations_by_column, kept)
    if args.chart:
        chart(args.chart, observations_by_column, kept)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
