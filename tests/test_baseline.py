"""The baseline is judged on the cases where a band should refuse to answer.

Fit quality is not tested here and that is on purpose. The source feed is generated, so a
band that fits it well has recovered a shape that was handed to it. What can be tested is
behaviour. A contaminated history must not widen the band. A band with no width has to say
so rather than fire on everything. A key with no band is a different answer from a value
inside one. And the history reader has to count what it drops.
"""

import sys
from datetime import date, datetime, timedelta

from obs import baseline, history, store
from obs.model import DatasetMetric, RunRecord, SchemaVersion
from tests.tiny import Checks

MONDAY = date(2026, 3, 2)


def flat(key, value, n):
    return [(key, value)] * n


def seeded(key, values):
    return [(key, v) for v in values]


def add_run(con, run_id, partition, attempt, status, duration_ms, minute,
            task="load_raw", cold=False):
    """`minute` is separate from `attempt` on purpose. Start order and attempt order are
    not the same thing, and the first version of this fixture derived one from the other,
    which made the run_order assertion pass for a reason that was not true."""
    store.insert_run(con, RunRecord(
        run_id=run_id, pipeline="orders", task=task, partition_key=partition,
        started_at=datetime(2026, 3, 2, 9, 0, 0) + timedelta(minutes=minute),
        status=status, attempt=attempt, duration_ms=duration_ms, cold_start=cold))


def add_metric(con, run_id, rows, dataset="raw_orders"):
    version = SchemaVersion.from_columns(dataset, [("a", "VARCHAR")], datetime(2026, 3, 2))
    store.upsert_schema_version(con, version)
    store.insert_dataset_metric(con, DatasetMetric(
        run_id=run_id, dataset=dataset, schema_hash=version.schema_hash,
        row_count=rows, collected_at=datetime(2026, 3, 2)))


def run():
    c = Checks("test_baseline")

    # --- the estimators, and the reason there are two of them ---------------------
    centre, spread = baseline.median_mad([10, 10, 10, 10, 1000])
    c.eq(centre, 10, "one wild value does not move the median")
    mean_centre, mean_spread = baseline.mean_sd([10, 10, 10, 10, 1000])
    c.ok(mean_centre > 200, "the same value drags the mean past 200")
    c.ok(mean_spread > spread * 50,
         "and inflates the standard deviation to more than fifty times the scaled MAD")

    clean = seeded(0, [100, 104, 96, 102, 98, 101, 99, 103, 97])
    dirty = [(0, 400)] + clean[1:]
    robust_clean = baseline.fit_bands(clean, estimator="median_mad")[0]
    robust_dirty = baseline.fit_bands(dirty, estimator="median_mad")[0]
    loose_clean = baseline.fit_bands(clean, estimator="mean_sd")[0]
    loose_dirty = baseline.fit_bands(dirty, estimator="mean_sd")[0]
    robust_growth = robust_dirty.width / robust_clean.width
    loose_growth = loose_dirty.width / loose_clean.width
    c.ok(loose_growth > robust_growth * 2,
         f"one contaminated point widens the mean band {loose_growth:.1f}x against "
         f"{robust_growth:.1f}x for the median band")
    c.eq(baseline.Baseline({0: robust_dirty}).check(0, 400).status, "high",
         "and the contaminated value is still caught by the median band")

    # --- a band with no width must not become an alert generator ------------------
    degenerate = baseline.fit_bands(flat(0, 7, 20))[0]
    c.ok(degenerate.degenerate, "twenty identical values give a spread of zero")
    verdict = baseline.Baseline({0: degenerate}).check(0, 9999)
    c.eq(verdict.status, "unbanded",
         "a zero width band refuses to judge instead of calling everything high")
    c.eq(verdict.score, None, "and reports no score, because there is nothing to divide by")

    # --- an unknown key is not the same answer as a value inside a band -----------
    model = baseline.Baseline.fit(seeded(0, [10, 12, 11, 13, 9, 10, 11, 12]))
    c.eq(model.check(5, 11).status, "unknown_key", "a key with no band says so")
    c.eq(model.check(0, 11).status, "ok", "a value at the centre is ok")
    c.eq(model.check(0, 10000).status, "high", "a value far above is high")
    c.eq(model.check(0, 0.001).status, "low", "a value far below is low")
    c.ok(model.check(0, 10000).score > 0 > model.check(0, 0.001).score,
         "the score is signed")

    # --- log space -----------------------------------------------------------------
    # asserting only that this raises ValueError proves nothing, because math.log raises
    # ValueError on its own for both of these. The guard is only doing work if the message
    # is the one written here. A mutant that removed the check survived until this checked
    # the text.
    for bad, label in ((0, "zero"), (-4, "negative")):
        try:
            baseline.fit_bands(seeded(0, [1, 2, 3, 4, 5, 6, bad]))
            c.ok(False, f"log space accepted a {label}")
        except ValueError as exc:
            c.ok("not to invent a floor" in str(exc),
                 f"log space refuses a {label} with its own message, not math's: {exc}")
    log_band = baseline.fit_bands(seeded(0, [10, 12, 11, 13, 9, 10, 11, 12]))[0]
    above = log_band.hi - log_band.middle
    below = log_band.middle - log_band.lo
    c.ok(above > below,
         "a log band is asymmetric in the original units, wider above than below")
    raw_band = baseline.fit_bands(seeded(0, [10, 12, 11, 13, 9, 10, 11, 12]),
                                  space="raw")[0]
    c.ok(abs((raw_band.hi - raw_band.middle) - (raw_band.middle - raw_band.lo)) < 1e-9,
         "and a raw band is symmetric, which is the difference the space makes")

    c.raises(ValueError, lambda: baseline.fit_bands(seeded(0, [1, 2]), space="cubed"),
             "an unknown space is rejected by name")
    c.raises(ValueError,
             lambda: baseline.fit_bands(seeded(0, [1, 2]), estimator="vibes"),
             "so is an unknown estimator")

    # --- too few observations is no band, not a narrow one -------------------------
    c.eq(len(baseline.fit_bands(seeded(0, [1, 2, 3]))), 0,
         "three observations do not earn a band")
    c.eq(len(baseline.fit_bands(seeded(0, [1, 2, 3]), min_n=3)), 1,
         "and the threshold is the only thing stopping them")

    # --- variance explained --------------------------------------------------------
    separated = seeded(0, [10] * 8) + seeded(1, [1000] * 8)
    var = baseline.variance_explained(separated)
    c.ok(var["r2"] > 0.99, "fully separated groups explain nearly all the variance")
    # the first version of this used [10, 11] repeated, which put every 10 in group 0 and
    # every 11 in group 1. That is perfectly separated, not overlapping, and it explained
    # 100 percent of the variance. The two groups have to actually overlap.
    overlapping = seeded(0, [10, 12, 11, 13, 9, 11, 12, 10]) + \
        seeded(1, [11, 10, 13, 9, 12, 11, 10, 12])
    c.ok(baseline.variance_explained(overlapping)["r2"] < 0.05,
         "two groups drawn from the same spread explain almost none of the variance")
    noise = [(i % 7, v) for i, v in enumerate([5, 9, 4, 8, 6, 7, 5, 8, 4, 9,
                                               6, 7, 5, 8, 9, 4, 6, 7, 8, 5, 9])]
    plain = baseline.variance_explained(noise)
    c.ok(plain["adjusted"] < plain["r2"],
         "the adjusted figure is always below the raw one, because groups cost")
    c.ok(plain["adjusted"] < 0.2,
         "and seven groups over noise adjust down to nearly nothing")
    c.eq(baseline.variance_explained(flat(0, 5, 10)), None,
         "no variance at all returns nothing rather than dividing by zero")

    # --- choosing whether to key at all -------------------------------------------
    keyed_call = baseline.choose_keying(
        seeded(0, [100, 104, 96, 102, 98, 101, 99]) +
        seeded(1, [500, 504, 496, 502, 498, 501, 499]))
    c.eq(keyed_call["keying"], "keyed",
         "two well separated groups are worth banding separately")
    c.ok(keyed_call["gain"]["ratio"] < 1,
         "and the keyed bands come out narrower than the pooled one")
    same = [5, 9, 4, 8, 6, 7, 5, 8, 4, 9, 6, 7, 5, 8]
    pooled_call = baseline.choose_keying(seeded(0, same) + seeded(1, same))
    c.eq(pooled_call["keying"], "pooled",
         "two groups drawn from the same spread are not worth splitting")
    # This case is built so the ratio alone would say keyed. Key 1 is tight and the pooled
    # band has to span both groups, so the surviving band looks like a big win. Only the
    # collapsed key 0 changes the answer, which is what makes this a test of the guard
    # rather than of the ratio.
    #
    # The estimator is the standard deviation here and that is not laziness. On a
    # deliberately bimodal fixture the MAD of the pooled set collapses to zero too,
    # because more than half the points sit on one value, and then both sides are
    # degenerate and there is no comparison left to make. That is a real property of the
    # MAD and it is in the README. It just makes it useless for showing this particular
    # guard working.
    lumpy = flat(0, 100, 10) + seeded(1, [1000, 1010, 990, 1005, 995, 1002, 998, 1001])
    flat_call = baseline.choose_keying(lumpy, estimator="mean_sd")
    c.eq(flat_call["keying"], "pooled",
         "a key whose spread collapses to zero forces the pooled answer")
    c.ok("zero spread" in flat_call["reason"],
         "and the reason names the collapsed key rather than the width ratio")
    c.eq(flat_call["gain"]["degenerate_keys"], 1, "the collapsed key is counted")
    c.ok(flat_call["gain"]["ratio"] < baseline.SEASONAL_KEY_MIN_GAIN,
         "even though the width ratio on the surviving key would have said keyed")
    c.ok(baseline.fit_bands(lumpy)[0].degenerate
         and baseline.fit_bands(history.unkeyed(
             [(k, v, None) for k, v in lumpy]))[None].degenerate,
         "under the MAD both the flat key and the pooled set come out with no spread")

    # --- fire rate ------------------------------------------------------------------
    counts, rate = model.fire_rate(seeded(0, [11, 11, 10000]))
    c.eq(counts["high"], 1, "fire_rate counts the value above the band")
    c.eq(counts["ok"], 2, "and the two inside it")
    c.ok(abs(rate - 1 / 3) < 1e-9, "the rate is fired over total")

    c.eq(baseline.leave_one_out_edges(seeded(0, [1, 2, 3, 4, 5, 6, 7]), 0), None,
         "leaving one out of seven leaves six, which is under the minimum")
    loo = baseline.leave_one_out_edges(seeded(0, [10, 12, 11, 13, 9, 10, 11, 12, 30]), 0)
    c.ok(loo["hi_max"] > loo["hi_min"],
         "and with enough observations the held out edge really does move")

    # --- history: what counts as one observation ------------------------------------
    c.eq(history.partition_date("dt=2026-03-02"), MONDAY, "a partition key parses")
    c.eq(history.partition_date("2026-03-02"), None, "a key with no prefix does not")
    c.eq(history.partition_date("dt=not-a-date"), None, "nor does a bad date")
    c.eq(history.partition_date(None), None, "nor does a null partition key")
    c.eq(MONDAY.weekday(), 0, "Monday is index 0, matching WEEKDAY_NAMES")
    c.eq(history.WEEKDAY_NAMES[MONDAY.weekday()], "Monday", "and the name agrees")

    # Four partitions, each shaped to break one thing.
    #   03-02  failed, then succeeded twice with different counts. Latest success wins.
    #   03-03  one clean run.
    #   03-04  only ever failed. Nothing about it belongs in a baseline of normal.
    #   03-05  succeeded, then a later attempt failed. The success is still the truth.
    # The first version of this had one success per partition, which meant three separate
    # mutants of the attempt and status logic all survived.
    con = store.connect()
    add_run(con, "r1", "dt=2026-03-02", 1, "failed", 900, minute=1)
    add_metric(con, "r1", 10)
    add_run(con, "r2", "dt=2026-03-02", 2, "success", 12, minute=2)
    add_metric(con, "r2", 2000)
    add_run(con, "r3", "dt=2026-03-02", 3, "success", 14, minute=3)
    add_metric(con, "r3", 2050)
    add_run(con, "r4", "dt=2026-03-03", 1, "success", 11, minute=4)
    add_metric(con, "r4", 2100)
    add_run(con, "r5", "dt=2026-03-04", 1, "failed", 777, minute=5)
    add_metric(con, "r5", 50)
    add_run(con, "r6", "dt=2026-03-05", 1, "success", 9, minute=6)
    add_metric(con, "r6", 2150)
    add_run(con, "r7", "dt=2026-03-05", 2, "failed", 500, minute=7)
    add_run(con, "r8", "garbage", 1, "success", 11, minute=8)
    add_metric(con, "r8", 2200)

    volume, skipped = history.volume_history(con)
    c.eq([v for _, v, _ in volume], [2050, 2100, 2150],
         "one observation per partition, from the last attempt that succeeded")
    c.eq(skipped, 1, "and the unreadable partition key is counted rather than dropped")
    c.eq([k for k, _, _ in volume], [0, 1, 3],
         "keyed by weekday, and the day that only ever failed is not in the history")

    durations, d_skipped = history.duration_history(con)
    c.eq([v for _, v, _ in durations], [14, 11, 9],
         "durations follow the same rule, so 900 and 777 and 500 stay out")
    c.eq(d_skipped, 1, "the unreadable key is counted here too")
    c.eq(len(history.duration_history(con, task="nothing")[0]), 0,
         "an unknown task returns no observations rather than everything")

    c.eq(history.keyed(volume), [(0, 2050), (1, 2100), (3, 2150)],
         "keyed drops the date and leaves the pairs the model wants")
    c.eq(history.unkeyed(volume), [(None, 2050), (None, 2100), (None, 2150)],
         "unkeyed puts them all under one key for the pooled comparison")
    c.eq(len(history.recent(volume, 2)), 2, "a trailing window keeps that many partitions")
    c.eq(history.recent(volume, 2)[0][1], 2100, "and takes them from the end")
    c.eq(len(history.recent(volume, 0)), 3, "a window of zero means the whole history")
    c.eq(len(history.recent(volume, 99)), 3,
         "and a window longer than the history is the history, not an error")

    c.eq([p for p, _ in history.run_order(con)],
         ["dt=2026-03-02", "dt=2026-03-02", "dt=2026-03-03", "dt=2026-03-05",
          "garbage"],
         "run_order keeps start order and both retries, which is where a cold start shows")

    # --- coverage, which is ot-016 -------------------------------------------------
    # Nothing above is missing a metric, so the check comes back clean on this fixture
    # and a clean check proves nothing. r9 is the case: a run that succeeded and wrote
    # no dataset metric, which is exactly what collect_into leaves behind when it
    # swallows an exception.
    cov = history.coverage(con, "raw_orders")
    c.eq(len(cov["no_dataset_metric"]), 0, "a fixture with no gaps reports no gaps")
    c.eq(cov["producers"], ["load_raw"],
         "the producing task is read out of the metadata rather than assumed")

    add_run(con, "r9", "dt=2026-03-06", 1, "success", 10, minute=9)
    cov = history.coverage(con, "raw_orders")
    c.eq([r[1] for r in cov["no_dataset_metric"]], ["dt=2026-03-06"],
         "a successful run that wrote no dataset metric is found")

    # a run of a task that never produces this dataset must not be reported. before the
    # scoping fix this returned every build_daily run and the first real run of the
    # report showed 119 false positives.
    add_run(con, "r10", "dt=2026-03-06", 1, "success", 8, minute=10, task="build_daily")
    add_metric(con, "r10", 1, dataset="daily_orders")
    cov = history.coverage(con, "raw_orders")
    c.eq([r[1] for r in cov["no_dataset_metric"]], ["dt=2026-03-06"],
         "a build_daily run is not a raw_orders silence")
    c.eq(len(history.coverage(con, "daily_orders")["no_dataset_metric"]), 0,
         "and daily_orders is clean when asked about on its own terms")

    # every dataset metric here was written without column metrics, so this is the
    # level below and it should find all of them
    c.ok(len(cov["no_column_metric"]) > 0, "a dataset metric with no columns is found")

    # the third silence. absent unless an expected set is supplied, and the returned
    # flag says which of those two happened so a caller cannot read a missing check as
    # a passing one.
    c.eq(cov["never_ran"], None, "with no expected set the third check is absent")
    c.eq(cov["never_ran_checked"], False, "and says so rather than looking clean")
    with_expected = history.coverage(
        con, "raw_orders", expected_partitions=["dt=2026-03-02", "dt=2026-09-09"])
    c.eq(with_expected["never_ran"], ["dt=2026-09-09"],
         "a partition nothing ever ran for is only findable from outside")
    c.eq(with_expected["never_ran_checked"], True, "and the flag flips when it is given")

    # --- the cold start key, which is ot-018 ---------------------------------------
    cold_con = store.connect()
    add_run(cold_con, "k1", "dt=2026-03-02", 1, "success", 900, minute=1, cold=True)
    add_run(cold_con, "k2", "dt=2026-03-03", 1, "success", 11, minute=2)
    add_run(cold_con, "k3", "dt=2026-03-04", 1, "success", 12, minute=3)
    cold_obs, cold_skipped = history.cold_start_history(cold_con)
    c.eq([k for k, _, _ in cold_obs], [True, False, False],
         "the cold flag comes back as the key, ready for fit_bands")
    c.eq([v for _, v, _ in cold_obs], [900, 11, 12], "with the durations beside it")
    c.eq(cold_skipped, 0, "and nothing skipped on readable keys")

    # one cold observation cannot make a band, and the honest answer to a cold run is
    # that nothing is known about it rather than that it is fine.
    cold_bands = baseline.Baseline.fit([(k, v) for k, v, _ in cold_obs], space="log")
    c.eq(cold_bands.check(True, 900).status, "unknown_key",
         "a single cold observation gives no band and no verdict")
    cold_con.close()

    # holdout_fire_rate, the day-7 fix to the number the pager gate reads for volume. the
    # fixture is a quiet front half and a back half that steps well outside it, so an
    # implementation that fitted on everything would report a much lower rate than one that
    # fitted on the front only. a fixture whose two halves looked alike could not tell them
    # apart, which is the 08-02 lesson pointed at a split rather than at a group.
    from datetime import date as _date
    quiet = [(None, 100.0 + (i % 5), _date(2026, 1, 1) + timedelta(days=i))
             for i in range(70)]
    loud = [(None, 900.0, _date(2026, 3, 12) + timedelta(days=i)) for i in range(30)]
    series = quiet + loud
    split = baseline.holdout_fire_rate(series)
    c.ok(split is not None, "a long enough series can be split")
    counts, rate, n_train, n_test = split
    c.eq((n_train, n_test), (70, 30), "the split lands where the fraction says")
    c.eq(rate, 1.0, "every held out observation is outside a band fitted on the quiet half")
    c.eq(counts["high"], 30, "and every one of them is high rather than merely counted")

    # the mutant worth killing is a fit over the whole series rather than over the training
    # half, so the fixture has to make those two disagree. under median and MAD they do not,
    # because thirty loud values out of a hundred leave the median alone and the band tight,
    # so both report 1.0 and the mutant survives. Under mean and standard deviation the loud
    # half drags the band out far enough to cover itself, which is the same contamination
    # effect day 3 measured on the doubled day. That is the pair that separates them.
    pulled = baseline.holdout_fire_rate(series, estimator="mean_sd")
    c.eq(pulled[1], 1.0, "held out, the loud half is still outside the quiet half's band")
    whole = baseline.Baseline.fit([(k, v) for k, v, _ in series], estimator="mean_sd")
    _, in_sample = whole.fire_rate([(k, v) for k, v, _ in series[70:]])
    c.eq(in_sample, 0.0,
         "fitted on everything the same band covers the loud half, which is the actual bug")

    c.eq(baseline.holdout_fire_rate([]), None, "an empty series cannot be split")
    c.eq(baseline.holdout_fire_rate(series[:15]), None,
         "too little training history returns None rather than a rate off four points")
    c.eq(baseline.holdout_fire_rate(series, split=1.0), None,
         "a split leaving no test set returns None rather than a rate over nothing")

    con.close()
    return c


if __name__ == "__main__":
    sys.exit(run().report())
