"""The drift checks are judged on what they refuse to claim.

Three things here are not ordinary unit tests and they are the reason this file exists.

The blind spot is asserted against a constructed pair rather than described. `blind_spot`
returns 0.25 by arithmetic on the probabilities. That is only worth anything if a pair of
samples can actually reach it, so the test builds one and checks the true KS distance.

The tied vector case is a regression test for a bug this suite did not have on the day the
code was written. `ks_bound` assumed a cumulative function equals its probability at its
own quantile, which holds for a continuous column and fails as soon as values repeat. On
`item_count` it made two byte identical vectors bound apart by 0.49.

The history fixture has collisions built into it. On 2026-08-02 three separate mutants of
`obs/history.py` survived because the fixture gave every partition one successful attempt,
so the rule about which attempt counts was green without ever running. This one carries a
partition that failed then succeeded twice with different values, one that only ever
failed, and one that succeeded and then failed on a later attempt.
"""

import random
from datetime import date, datetime, timedelta

from obs import drift, history, store
from obs.model import (
    QUANTILE_PROBS,
    ColumnMetric,
    DatasetMetric,
    RunRecord,
    SchemaVersion,
)
from tests.tiny import Checks

DATASET = "raw_orders"
COLUMN = "order_amount_usd"


def add_run(con, run_id, partition, attempt, status, minute):
    store.insert_run(con, RunRecord(
        run_id=run_id, pipeline="orders", task="load_raw", partition_key=partition,
        started_at=datetime(2026, 3, 2, 9, 0) + timedelta(minutes=minute),
        status=status, attempt=attempt, duration_ms=10))


def add_column(con, run_id, rows, quantiles=None, nulls=0, distinct=None,
               top=None, column=COLUMN):
    version = SchemaVersion.from_columns(DATASET, [("a", "VARCHAR")],
                                         datetime(2026, 3, 2))
    store.upsert_schema_version(con, version)
    store.insert_dataset_metric(con, DatasetMetric(
        run_id=run_id, dataset=DATASET, schema_hash=version.schema_hash,
        row_count=rows, collected_at=datetime(2026, 3, 2)))
    store.insert_column_metrics(con, [ColumnMetric(
        run_id=run_id, dataset=DATASET, column_name=column, data_type="DOUBLE",
        null_count=nulls, distinct_count=distinct, quantiles=quantiles,
        top_values=top)])


def vector(shift=0.0):
    """A plausible stored quantile vector, optionally moved."""
    return {str(p): v + shift for p, v in
            zip(QUANTILE_PROBS, [9.0, 15.0, 27.0, 41.0, 66.0, 119.0, 178.0])}


def observation(day, rows, quantiles=None, nulls=0, distinct=None, top=None):
    return {"weekday": day.weekday(), "date": day, "quantiles": quantiles,
            "null_count": nulls, "distinct_count": distinct, "top_values": top,
            "row_count": rows}


def run():
    c = Checks("test_drift")

    # --- the helper this file leans on, checked before it is trusted --------------
    # raises_message exists because asserting an exception type proved nothing on
    # 08-02. If it ever stopped comparing the text it would go back to proving nothing,
    # and every guard below would quietly lose its test. A mutant that made it accept
    # any message survived the whole suite until this was here.
    def boom():
        raise ValueError("the specific words that matter")

    probe = Checks("probe")
    probe.raises_message(ValueError, "specific words", boom, "matching fragment")
    c.eq(len(probe.failures), 0, "raises_message passes when the message matches")
    probe.raises_message(ValueError, "words that are not there", boom, "wrong fragment")
    c.eq(len(probe.failures), 1,
         "and fails when the type is right but the message is not")

    # --- what the schema structurally cannot see ---------------------------------
    gaps = drift.prob_gaps()
    c.eq(len(gaps), len(QUANTILE_PROBS) + 1,
         "there is one more gap than there are probabilities, because the tails count")
    c.ok(abs(sum(gaps) - 1.0) < 1e-12, "and the gaps cover the whole unit interval")
    c.eq(drift.blind_spot(), 0.25,
         "the widest gap at the stored probabilities is a quarter of the mass")

    low, high = drift.worst_case_pair(n=20000)
    ql = drift.sample_quantiles(low)
    qh = drift.sample_quantiles(high)
    c.ok(max(abs(a - b) for a, b in zip(ql, qh)) < 1e-12,
         "the constructed pair stores byte identical quantile vectors")
    c.eq(drift.ks_bound(ql, qh), 0.0,
         "so the bound a monitor could compute from them is exactly zero")
    true = drift.empirical_ks(low, high)
    c.ok(abs(true - drift.blind_spot()) < 0.01,
         f"while their real KS distance is {true:.4f}, which is the blind spot reached "
         "rather than argued")

    # --- the bound is a bound, and the tie case that broke it --------------------
    ties = [1, 1, 1, 1, 2, 4, 6]
    c.eq(drift.ks_bound(ties, ties), 0.0,
         "two identical vectors with repeated values bound apart by zero. this is the "
         "08-03 regression: the first version returned 0.49 here")
    c.ok(drift.ks_bound(ties, [1, 1, 2, 2, 3, 5, 7]) > 0,
         "and a real shift in a tied column still separates")
    smooth = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
    c.eq(drift.ks_bound(smooth, smooth), 0.0, "identical smooth vectors bound at zero")
    c.ok(drift.ks_bound(smooth, [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0])
         > 0.9, "and two disjoint columns bound near one")

    # the bound has to be read from both vectors, not just the reference. the window a
    # vector puts around F(x) changes only at that vector's own stored values, so a
    # separation can exist at an observed quantile that no reference quantile sits on.
    # a mutant that looked only from the reference side survived the rest of this file,
    # and on this pair it reports no detectable drift at all.
    c.eq(drift.ks_bound([0, 2, 2, 5, 5, 5, 5], [1, 2, 3, 4, 4, 5, 5]), 0.25,
         "a separation visible only at the observed vector's own values is still found")

    # the property that makes it usable at all. a bound that ever exceeded the real
    # distance would be a monitor that fires on agreement.
    rng = random.Random(4)
    violations = 0
    checked = 0
    for _ in range(40):
        a = sorted(rng.gauss(0, 1) for _ in range(400))
        b = sorted(rng.gauss(rng.choice([0, 0.3, 1.5]), rng.choice([1, 2]))
                   for _ in range(400))
        bound = drift.ks_bound(drift.sample_quantiles(a), drift.sample_quantiles(b))
        checked += 1
        if bound > drift.empirical_ks(a, b) + 1e-9:
            violations += 1
    c.eq(violations, 0,
         f"the bound never exceeded the true distance across {checked} random pairs")

    c.raises_message(ValueError, "one value per probability",
                     lambda: drift.ks_bound([1, 2, 3], [1, 2, 3]),
                     "a vector of the wrong length is rejected by name")

    # --- the readings that do work ------------------------------------------------
    ref = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]
    c.eq(drift.iqr(ref), 20.0, "the interquartile range reads the stored 0.25 and 0.75")
    c.raises_message(ValueError, "needs 0.25 and 0.75",
                     lambda: drift.iqr([1, 2, 3], probs=(0.1, 0.5, 0.9)),
                     "and refuses to substitute a different pair silently")
    shifted = [v + 10.0 for v in ref]
    c.eq(drift.max_abs_shift(ref, shifted), 0.5,
         "a shift of half an interquartile range reads as 0.5")
    c.raises_message(ValueError, "cannot be expressed as a multiple",
                     lambda: drift.quantile_shift([5.0] * 7, [6.0] * 7),
                     "a column with no spread has no units to express a shift in")

    c.eq(drift.null_rate(0, 0), None,
         "an empty partition has no null rate, rather than a perfect one")
    c.eq(drift.null_rate(5, 20), 0.25, "and a normal one is a share of the rows")

    # --- categorical shares -------------------------------------------------------
    s = drift.shares({"a": 60, "b": 20}, 100)
    c.ok(abs(s[None] - 0.2) < 1e-12,
         "the mass outside the stored top values is kept under a key of None")
    c.ok(abs(sum(s.values()) - 1.0) < 1e-12, "so the shares still sum to one")
    c.eq(drift.total_variation(s, s), 0.0, "a distribution against itself is zero")
    c.eq(drift.total_variation({"a": 1.0}, {"b": 1.0}), 1.0,
         "two disjoint vocabularies are one")
    new_category = drift.total_variation({"a": 0.9, "b": 0.1},
                                         {"a": 0.9, "b": 0.05, "new": 0.05})
    c.ok(new_category > 0.0,
         "a category appearing that was never there moves the distance")

    # --- the coupling check, which is the day's main refusal ----------------------
    rows = [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700]
    series = {
        "tracks_rows": [n * 0.98 for n in rows],
        "independent": [0.5, 0.4, 0.6, 0.45, 0.55, 0.5, 0.42, 0.58],
    }
    usable, refused = drift.usable_signals(series, rows)
    c.ok("tracks_rows" in refused,
         "a signal that moves with the row count is refused as a drift signal")
    c.eq(refused["tracks_rows"]["reason"], "tracks row count",
         "and the report is told why rather than left to guess")
    c.ok(abs(refused["tracks_rows"]["coupling"]) > 0.99,
         "with the measured coupling attached")
    c.ok("independent" in usable, "a signal that does not track it survives")

    # the refusals that actually happen on the real feed are negative. distinct_ratio on
    # a column with a fixed vocabulary is a constant over the row count, so it falls as
    # traffic rises and its coupling is about minus 0.99. a check that only looked at
    # positive correlation would have let every one of those through.
    inverse = {"falls_as_rows_rise": [4.0 / n for n in rows]}
    _usable, inverse_refused = drift.usable_signals(inverse, rows)
    c.ok("falls_as_rows_rise" in inverse_refused,
         "a signal that moves against the row count is refused too, not only one that "
         "moves with it")
    c.ok(inverse_refused["falls_as_rows_rise"]["coupling"] < -0.9,
         "and its coupling is recorded as the negative number it is")

    # --- the history reader, on a fixture built to collide ------------------------
    con = store.connect(":memory:")
    days = [date(2026, 3, 2) + timedelta(days=i) for i in range(10)]

    # a partition that failed, then succeeded twice with different values. the later
    # success is the one whose output is in the warehouse.
    add_run(con, "r-a1", "dt=2026-03-02", 1, "failed", 0)
    add_column(con, "r-a1", 999, vector(50.0), nulls=900, distinct=1)
    add_run(con, "r-a2", "dt=2026-03-02", 2, "success", 5)
    add_column(con, "r-a2", 1000, vector(), nulls=10, distinct=500)
    add_run(con, "r-a3", "dt=2026-03-02", 3, "success", 9)
    add_column(con, "r-a3", 1200, vector(1.0), nulls=12, distinct=600)

    # a partition that only ever failed. it contributes nothing.
    add_run(con, "r-b1", "dt=2026-03-03", 1, "failed", 20)
    add_column(con, "r-b1", 5, vector(80.0), nulls=5, distinct=1)

    # a partition that succeeded and then failed on a later attempt. the successful
    # attempt is not the highest one, and the failed row must not win by being later.
    add_run(con, "r-c1", "dt=2026-03-04", 1, "success", 30)
    add_column(con, "r-c1", 1100, vector(2.0), nulls=11, distinct=550)
    add_run(con, "r-c2", "dt=2026-03-04", 2, "failed", 35)
    add_column(con, "r-c2", 3, vector(90.0), nulls=3, distinct=1)

    for i, day in enumerate(days[3:], start=3):
        add_run(con, f"r-{i}", f"dt={day.isoformat()}", 1, "success", 60 + i)
        add_column(con, f"r-{i}", 1000 + i * 10, vector(i * 0.1),
                   nulls=10 + i, distinct=500 + i)

    observations, skipped = history.column_history(con, DATASET, COLUMN)
    c.eq(len(observations), 9,
         "nine partitions produced an observation, and the one that only ever failed "
         "produced none")
    c.eq(skipped, 0, "and nothing was dropped for an unreadable partition key")
    first = observations[0]
    c.eq(first["row_count"], 1200,
         "the last successful attempt is the observation, not the first")
    c.eq(first["null_count"], 12, "and its column metrics come from that same attempt")
    second = observations[1]
    c.eq(second["row_count"], 1100,
         "a partition that succeeded then failed keeps the successful attempt")
    c.ok(all(o["date"] != date(2026, 3, 3) for o in observations),
         "the partition with no successful attempt is absent rather than zero")
    c.ok(observations == sorted(observations, key=lambda o: o["date"]),
         "observations come back in partition date order")

    unreadable = store.connect(":memory:")
    add_run(unreadable, "r-x", "latest", 1, "success", 0)
    add_column(unreadable, "r-x", 100, vector())
    _obs, dropped = history.column_history(unreadable, DATASET, COLUMN)
    c.eq(dropped, 1, "a partition key that is not a date is counted, not silently lost")

    # --- the reference, and the corruption it refuses to average over -------------
    good = [observation(days[i], 1000, vector(i * 0.1)) for i in range(8)]
    reference = drift.reference_quantiles(good)
    c.eq(len(reference), len(QUANTILE_PROBS),
         "the reference has one value per stored probability")
    contaminated = good + [observation(days[8], 1000, vector(500.0))]
    c.ok(abs(drift.reference_quantiles(contaminated)["0.5"]
             - reference["0.5"]) < 1.0,
         "one wildly wrong partition does not drag the reference, because it is a median")

    mismatched = good + [observation(days[8], 1000, {"0.1": 1.0, "0.9": 2.0})]
    c.raises_message(ValueError, "corrupt history",
                     lambda: drift.reference_quantiles(mismatched),
                     "a history whose vectors disagree on their probabilities is refused")

    # --- the monitor ---------------------------------------------------------------
    varied = [observation(days[i % 7] + timedelta(days=i), 1000 + i,
                          vector((i % 3) * 0.4), nulls=0, distinct=4,
                          top={"placed": 900, "cancelled": 100})
              for i in range(20)]
    monitor = drift.Monitor.fit("status", varied)
    c.ok("distinct_count" in monitor.constants,
         "a signal that never moved is held as a constant, not as a band with no width")
    c.eq(monitor.check("distinct_count", 0, 4).status, "ok",
         "the constant value passes")
    c.eq(monitor.check("distinct_count", 0, 5).status, "changed",
         "and a fifth category fires, which a degenerate band would not have")
    c.eq(monitor.check("null_rate", 0, 0.0).status, "ok",
         "a column that has never had a null passes at zero")
    c.eq(monitor.check("null_rate", 0, 0.02).status, "changed",
         "and fires on the first null it ever sees")
    c.eq(monitor.check("not_a_signal", 0, 1.0), None,
         "a signal the monitor does not hold returns nothing rather than a pass")
    c.ok("quantile_shift" in monitor.bands,
         "a signal that does move gets a real band")
    c.ok(set(monitor.watched()) == set(monitor.bands) | set(monitor.constants),
         "and what it watches is exactly the bands plus the constants")

    coupled = [observation(days[i % 7] + timedelta(days=i), 1000 + i * 50,
                           vector(), nulls=0, distinct=990 + i * 49)
               for i in range(20)]
    coupled_monitor = drift.Monitor.fit("customer_id", coupled)
    c.ok("distinct_count" in coupled_monitor.refused,
         "a distinct count that rides the row count is refused by the monitor too")
    c.ok("distinct_count" not in coupled_monitor.watched(),
         "and does not appear in what it claims to be watching")

    return c
