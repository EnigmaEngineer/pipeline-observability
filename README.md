# pipeline-observability

Catch a broken data pipeline before the dashboard consumers do. This repo holds a small
orders pipeline and the run-metadata schema that watches it. Freshness, volume, schema and
distribution monitors get built on top of that metadata over the next six days.

```
pip install -r requirements.txt
python -m pipeline.generate --start 2026-03-02 --end 2026-06-28 --out data/raw
python -m pipeline.run --start 2026-03-02 --end 2026-06-28
python -m tests.run_all
```

## Where the pieces sit

```
  data/raw/dt=YYYY-MM-DD/orders.jsonl        generated source events
        |
        |  pipeline.orders.load_raw          declared columns, partition overwrite
        v
  raw_orders  ----------------------------->  obs_dataset_metric   (day 2)
        |                                     obs_column_metric    (day 2)
        |  pipeline.orders.build_daily        obs_schema_version   (day 2)
        v                                     obs_run              (day 2)
  daily_orders                                       |
                                                     v
                                      baselines, drift, alerts, timeline
                                                (days 3 to 6)
```

The observability side is `obs/`. The thing being observed is `pipeline/`. They do not
import each other yet and that separation is the point. The collector on day 2 reaches
into the pipeline's warehouse from the outside, the same way it would have to if the
pipeline were a dbt project or someone else's job.

## The metadata schema

Four tables in `obs/schema.py`. The grain of each is the decision that matters, and each
one is enforced by a key rather than by a comment.

| table | grain | holds |
|---|---|---|
| `obs_run` | one row per pipeline, task, partition and attempt | timing, status, error, what triggered it |
| `obs_schema_version` | one row per distinct column list a dataset has had | the ordered columns and when the shape was first seen |
| `obs_dataset_metric` | one row per run and dataset | row count, byte size, event time range, schema hash |
| `obs_column_metric` | one row per run, dataset and column | nulls, distinct count, min, max, mean, quantiles, top values |

Three choices in there are worth arguing with.

**Schema is stored once per shape and referenced by hash.** The obvious alternative is a
snapshot row per run. That copies an unchanging column list thousands of times to answer a
question asked once per incident. Hashing makes "did the schema change between these two
runs" a string comparison. The hash is order sensitive on purpose. A column reorder breaks
a positional load and is a real incident, not noise.

**Column summaries are quantiles at fixed probabilities, not histograms.** Histogram bins
need edges chosen up front and edges chosen in week one are wrong by week five. Fixed
probabilities compare across any two days with no shared setup. The cost is that seven
quantiles cannot reconstruct a distribution, so a bimodal shift that leaves the quantiles
in place is invisible to this schema.

**`min_value` and `max_value` are text.** One table holds columns of every type, and
splitting into numeric, text and timestamp variants triples the width to serve a field a
human reads during an incident. The numeric summary a monitor reads is `mean_value` plus
`quantiles_json`. The cost is that you cannot write a SQL predicate over `min_value`
without a cast.

## The pipeline being watched

A two stage orders ELT. JSONL partitions land in `raw_orders`, then `daily_orders` is
built from them. Both stages overwrite their partition instead of appending, because a
retry is routine and an append-only load turns one retry into a doubled day. A doubled day
is also the exact shape a volume monitor is meant to catch, so an append here would let
the pipeline manufacture its own alerts.

Load columns are declared, not inferred. `read_json` will guess, and its guess changes
when the data changes, so a null-heavy day can silently retype a column. In a project
about detecting schema drift the loader has to be the one place that does not drift on its
own.

## Measured on this machine today

Sandbox is 2 cores and about 3.9 GB of RAM. Python 3.10, DuckDB 1.5.5.

```
generate  254,346 events across 119 partitions        2.7 s
load      254,346 rows, 119 partitions, 96,303 and 100,706 rows/s on two runs
rerun     last 7 partitions, row count unchanged
tests     4 modules, 57 cases, all passing
smoke     27,629 rows over 14 partitions, unchanged after a full rerun
```

Daily order volume over those 119 days, straight out of `daily_orders`:

| day | days observed | mean orders | sd |
|---|---|---|---|
| Monday | 17 | 2313 | 149 |
| Tuesday | 17 | 2229 | 187 |
| Wednesday | 17 | 2271 | 206 |
| Thursday | 17 | 2348 | 191 |
| Friday | 17 | 2526 | 164 |
| Saturday | 17 | 1691 | 134 |
| Sunday | 17 | 1582 | 126 |

Day of week accounts for 80.4 percent of the variance in daily volume, or 79.3 percent
adjusted for the six degrees of freedom the seven group means cost. Pooled standard
deviation is 367 orders and the residual after removing day of week is 163.

That gap is the argument for day 3. A single static band has to be wide enough to hold
both a normal Sunday and a normal Friday. On this data that band spans 1094 to 3014
orders, a width of 1920. The per day-of-week band at the same three sigma is 977 wide. So
a Friday has to lose 57 percent of its orders to break the static band and 19 percent to
break the seasonal one.

**Read those numbers with the caveat below.** They are measured, and what they are
measured on is synthetic.

## Known limitations

**The source data is generated, and its weekly shape is assumed rather than observed.**
The day of week factors in `pipeline/generate.py` are a plausible retail pattern that I
chose. So the 80.4 percent figure above is a true statement about this feed and not
evidence about real traffic. Anyone reading it as the latter is reading it wrong.

**That creates a circularity risk for day 3 and it is not fully solved.** If the generator
lays down a clean weekly pattern and the baseline learns that pattern, nothing has been
shown. Two things push back on it. The generator uses multiplicative day of week factors
times a trend times lognormal noise, and a baseline assuming additive weekday offsets on
raw counts will be wrong in a way the residuals expose. More importantly the day 3 test is
not fit quality. It is whether the baseline stays quiet through the nuisances that go in
on day 6 as injected failures. Judge it there.

**`daily_orders` groups on the partition date, not on `ordered_at`.** An event that
happened on the 3rd and landed in the 4th's file is counted on the 4th. The generator
never does that, so the pipeline is correct here only because its source is well behaved.
That is the single most likely place a real feed would break this, and late arrival is on
the day-6 list of injected failures.

**Nothing writes to the metadata tables yet.** The schema exists, it is tested and it is
empty. The collector is day 2. A collector guessing at row counts before it has been
written would be worse than an honest gap.

**The Snowflake DDL has never been run.** It is generated from the same template as the
DuckDB DDL so the two cannot drift apart, and it is unverified. It also carries a real
behaviour difference. Snowflake accepts `PRIMARY KEY` and `FOREIGN KEY` and enforces
neither, so the grain guarantees that hold here are documentation there. The collector has
to treat a duplicate grain as its own problem rather than expect the warehouse to reject
it.

**No Airflow yet.** The blueprint lists it and `pipeline/run.py` is a plain loop today.
The DAG comes when there is more than one task worth scheduling.

## Week plan

- Day 1: metadata schema, repo, target pipeline to instrument (done)
- Day 2: collector and storage
- Day 3: seasonal baselines for volume and duration
- Day 4: distribution drift checks
- Day 5: alerting, severity, suppression windows
- Day 6: incident timeline and injected failures
- Day 7: README with three worked incident examples

## Tests

`python -m tests.run_all` is the only entrypoint and CI calls nothing else. A module that
reports zero cases fails the run, because the usual way a suite lies is not a wrong
assertion. It is a file the runner imported and never executed.

`scripts/smoke.py` covers the command line path. It generates two weeks, runs the range,
snapshots the counts, runs the same range again and compares.

Every check here was falsified before it was kept. Reverting the fix and confirming the
test goes red is the only way to know a test tests anything.

| mutation | result |
|---|---|
| `load_raw` appends instead of overwriting the partition | 8/11, doubles to 4076 rows |
| `schema_hash` joins name and type pairs with a newline | 14/15, two schemas collide |
| drop the composite key on `obs_column_metric` | 14/15, duplicate grain accepted |
| a test module that asserts nothing | run_all fails it at 0 cases |
| the same appending load, against the smoke check | exit 1, 27,629 rows become 55,258 |
| drop the transaction around the delete and the insert | 11/12, a bad file empties the partition |
