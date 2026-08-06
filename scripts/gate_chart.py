"""The four estimates of the volume band's fire rate, against the gate that reads it.

    python scripts/gate_chart.py --obs-db /tmp/obs.duckdb --chart docs/volume_gate.png

One chart because the day-7 finding is one comparison. The number that approved a page was
the only one measured on partitions the band had already seen. Every estimate that held some
data back fails the same gate, and so does the least favourable reading of the smallest of
them.

The 3 of 10 figure comes out of `scripts/incident_report.py` rather than from here, because
it needs the injection harness to build ten future partitions. It is passed in as an argument
so this file cannot invent it.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from obs import alerting, history, store  # noqa: E402
from obs.baseline import Baseline, holdout_fire_rate  # noqa: E402

NARROW_WINDOW = 56


def main():
    ap = argparse.ArgumentParser(description="the volume gate, in one chart")
    ap.add_argument("--obs-db", default="/tmp/obs.duckdb")
    ap.add_argument("--dataset", default="raw_orders")
    ap.add_argument("--pipeline", default="orders")
    ap.add_argument("--window", type=int, default=NARROW_WINDOW)
    ap.add_argument("--arm-fired", type=int, default=3,
                    help="clean arm fires, from scripts/incident_report.py")
    ap.add_argument("--arm-total", type=int, default=10)
    ap.add_argument("--chart", default="docs/volume_gate.png")
    args = ap.parse_args()

    con = store.connect(args.obs_db)
    obs, _ = history.volume_history(con, args.dataset, args.pipeline)
    con.close()
    if len(obs) < args.window + 7:
        print("not enough volume history")
        return 1

    wide = Baseline.fit(history.keyed(obs))
    _, in_sample = wide.fire_rate(history.keyed(history.recent(obs, args.window)))
    split = holdout_fire_rate(obs)
    if split is None:
        print("not enough history to hold any out")
        return 1
    _counts, held, n_train, n_test = split
    arm = args.arm_fired / args.arm_total
    bound = alerting.fire_rate_lower_bound(args.arm_fired, args.arm_total)

    bars = [
        (f"in sample\nfit {len(obs)}, count last {args.window}", in_sample),
        (f"held out\nfit {n_train}, count {n_test}", held),
        (f"clean arm\n{args.arm_fired} of {args.arm_total} unseen", arm),
        (f"that arm's exact\n95% lower bound", bound),
    ]
    for label, value in bars:
        print(f"{label.replace(chr(10), ' | '):<44}{value:.3f}  "
              f"{'pages' if alerting.page_eligible(value) else 'refused'}")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = [b[0] for b in bars]
    values = [b[1] for b in bars]
    # the one bar that clears the gate is the one nobody should have trusted, so it is the
    # bar that gets the different colour. colouring by pass or fail would make three bars
    # look like the anomaly when it is the first one that is.
    colours = ["#e45756" if alerting.page_eligible(v) else "#4c78a8" for v in values]

    fig, ax = plt.subplots(figsize=(8.4, 4.6))
    ax.bar(labels, values, color=colours, width=0.6)
    ax.axhline(alerting.MAX_PAGE_FIRE_RATE, color="#333333", linestyle="--", linewidth=1.2)
    # annotated on the left. the first version sat it at the right hand end, where it
    # overlapped the fourth bar's value label at exactly the point the reader is meant to
    # be comparing the two.
    ax.annotate(f"pager gate, {alerting.MAX_PAGE_FIRE_RATE:.2f}",
                xy=(-0.42, alerting.MAX_PAGE_FIRE_RATE), xytext=(0, 7),
                textcoords="offset points", ha="left", fontsize=9)
    for i, value in enumerate(values):
        ax.text(i, value + 0.008, f"{value:.3f}", ha="center", fontsize=9)
    ax.set_ylabel("fire rate of the volume band")
    ax.set_ylim(0, max(values) * 1.25)
    ax.set_title("Four estimates of one fire rate. Only the in sample one clears the gate.")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    Path(args.chart).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.chart, dpi=140)
    print(f"\nwrote {args.chart}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
