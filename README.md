# pipeline-observability

Catch a broken data pipeline before the dashboard consumers do. This repo holds a small
orders pipeline and the run-metadata schema that watches it. Freshness, volume, schema and
distribution monitors get built on top of that metadata over the next six days.

```
pip install -r requirements.txt
python -m pipeline.generate --start 2026-03-02 --end 2026-06-28 --out data/raw
python scripts/run_observed.py --start 2026-03-02 --end 2026-06-28
python -m tests.run_all
```

`pipeline.run` runs the same pipeline with nothing watching it. Keeping both is what makes
the cost of the collector measurable.

## Where the pieces sit

```
  data/raw/dt=YYYY-MM-DD/orders.jsonl        generated source events
        |
        |  pipeline.orders.load_raw          declared columns, partition overwrite
        v
  raw_orders  ----------------------------->  obs_dataset_metric   (done)
        |            obs.collect              obs_column_metric    (done)
        |  pipeline.orders.build_daily        obs_schema_version   (done)
        v            obs.tracker              obs_run              (done)
  daily_orders                                       |
                                                     v
                                      baselines, drift, alerts, timeline
                                                (days 3 to 6)
```

The observability side is `obs/`. The thing being observed is `pipeline/`. They do not
import each other and that separation is the point. `scripts/run_observed.py` is the only
file that knows about both. The collector reaches into the pipeline's warehouse from the
outside, the same way it would have to if the pipeline were a dbt project or someone
else's job.

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

## The collector

`obs/tracker.py` records that a run happened. `obs/collect.py` profiles what it produced.
Four decisions in there are worth arguing with.

**The run row is written before the work, not after.** Timing a run and inserting when it
finishes records every run except the ones worth knowing about. A killed process never
reaches the insert, so the metadata says the run never happened, and a freshness check
then reads a run that died exactly like a pipeline nobody scheduled. Writing at the start
with status `running` costs a second write per run. What it buys is a row saying a run
began and never came back, which `tracker.stale_runs` can find.

**Collection is timed outside the run it belongs to.** Profiling inside the tracked block
would charge the run for the cost of watching it. Median recorded duration here is 10 ms
for `load_raw` and 7 ms for `build_daily` against roughly 41 ms to collect a dataset, so
the day-3 duration baseline would have been learning mostly the price of observability.

**Every column summary comes from one SELECT, and not for the reason I expected.** The
plan was that one scan would beat one query per column. It does not. On 254,346 rows the
single pass takes 81.2 ms and the eleven column loop takes 78.5 ms, because DuckDB is
columnar and a query reading one column never touched the other ten. The single pass stays
for two other reasons. It is one consistent snapshot rather than eleven, so a row count
and a null count cannot come from different states of a live table. And it is one round
trip instead of twelve, which is free locally and is the whole cost against a warehouse
across a network.

**Distinct counts are exact.** `approx_count_distinct` is 1.15x faster and its error is
nothing like a small wobble. See the table below. A monitor that cannot see a fourth
`status` value appear is not worth 1.15x.

**Quantiles are exact too, and that one was close.** They are the most expensive thing the
collector does, 30.9 ms of the 81.2, and `approx_quantile` is 2.57x faster at a worst
error of 0.27 percent. The reason it is still not used is that a t-digest depends on the
order rows arrive in. Reading the same 254,346 rows in a different physical order moved
p05 by 0.35 percent with nothing about the data changed. Day 4 is a drift check, and
starting it with a noise floor that comes from the estimator rather than the data is a bad
trade for 19 ms on a job that runs once a day. At a thousand times this volume the answer
flips.

## Measured on this machine today

Sandbox is 2 cores and about 3.9 GB of RAM. Python 3.10, DuckDB 1.5.5.

```
generate   254,346 events across 119 partitions               2.7 s
pipeline   the same range with nothing watching it            2.2 s   median of 3
observed   the same range with the collector wrapped round   11.9 s   median of 3
collected  238 runs, 2 schema versions, 238 dataset metrics, 2261 column metrics
tests      6 modules, 107 cases, all passing
smoke      27,629 rows over 14 partitions, unchanged after a full rerun
```

Collection costs 9.7 s across 238 datasets, about 41 ms each. That is 5.4x the pipeline it
watches. The ratio is a property of how little each run does here rather than a fact about
collectors. A partition is around 2,100 rows, and profiling it is eleven aggregates over
that data plus a group by for each categorical column.

Profiling the full `raw_orders` table, median of three after a warm up query:

| approach | queries | time |
|---|---|---|
| single pass | 1 | 81.2 ms |
| one query per column, same aggregates | 12 | 78.5 ms |
| single pass, quantiles removed | 1 | 50.8 ms |
| single pass, approximate distinct counts | 1 | 70.6 ms |

Quantiles on two numeric columns are 30.4 ms of the 81.2. They are the most expensive
thing the collector does by a wide margin.

`approx_count_distinct` against exact counts on the same table:

| column | exact | approximate | error |
|---|---|---|---|
| `order_id` | 254,346 | 299,919 | 17.9% |
| `customer_id` | 84,649 | 66,950 | 20.9% |
| `order_amount_usd` | 18,096 | 16,220 | 10.4% |
| `dt` | 119 | 166 | 39.5% |
| `status` | 4 | 3 | 25.0% |
| `channel`, `country`, `coupon_code` | 4 to 6 | exact | 0% |

Those errors are far larger than the accuracy usually quoted for HyperLogLog. They are
reported as measured. I have not worked out why DuckDB's estimator is this far off on a
column with 119 values and I am not going to guess at a mechanism in a README.

The `status` row is the one that settled it. A column with four values reported as three
is not a tolerance a threshold can absorb, it is the appearance of a new category being
made invisible.

**The slowest run in the entire history is the first one.** Over 119 partitions `load_raw`
has a median of 10 ms and a maximum of 734 ms, and the 734 is the first partition. The
next slowest is 15 ms. Nothing about that date is unusual. The cost is opening the
database and loading the JSON reader, and it lands on whichever run happens to go first.
Any duration baseline built on day 3 has to deal with that or it will page someone every
time the process restarts.

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

**A collector failure leaves a gap, and a gap is ambiguous.** `collect_into` catches
everything, because the pipeline should not fall over when the thing watching it does.
What is left behind is a successful run with no dataset metric row. Day 5 can alert on
that. What it cannot do is tell a broken collector apart from a dataset nobody pointed the
collector at, and both look like silence.

**`next_attempt` scans `obs_run` on every run start.** There is no index, so the cost
grows with the history rather than staying flat. At 238 rows it is invisible. At ten
million it is a full scan every time a task starts, which is the kind of thing that looks
free for a year and then does not.

**Attempt numbers are read then written, which is not atomic.** Under DuckDB in one
process this is fine, and the unique key would reject a real collision anyway. Under
Snowflake it is not fine, because Snowflake accepts a unique constraint and enforces
nothing, so two schedulers starting the same partition would both write the same attempt
number and neither would be told.

**Top values are only collected below 50 distinct values, and that cutoff is a guess.**
The categorical columns here sit at 4 to 6 values, so 50 is far enough above them to be
safe rather than tuned. A real feed with a 200 value category would get no top values at
all and nothing would say so.

**A quantile summary cannot see a shift that preserves the quantiles.** This is the day-1
schema tradeoff arriving in the collector. Seven probabilities do not reconstruct a
distribution, so a bimodal split that leaves the deciles where they were is invisible.
Day 4 has to say plainly what its drift check can and cannot detect.

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
- Day 2: collector and storage (done)
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
snapshots the counts, runs the same range again and compares. Then it runs the observed
path and holds the metadata to the warehouse. The row counts in `obs_dataset_metric` have
to add up to the rows actually in `raw_orders`. Every collector unit test points it at a
table built inside the test, so this is the only place its numbers meet a pipeline that
really ran.

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
| the profile query drops its `WHERE` clause | crashes on a parameter mismatch |
| the result row is sliced one position out | crashes converting a date to a float |
| `null_count` reports the non nulls instead | 31/32 |
| identifiers are not quoted | crashes on a column named `select` |
| `collect_into` raises instead of recording a gap | the failure escapes to the caller |
| top values ignore the partition filter | 30/32 |
| the run row is written after the work instead of before | the running row is not there to find |
| the tracker swallows the failure | 15/18 |
| `next_attempt` always returns 1 | the retry overwrites the first attempt |
| `stale_runs` ignores its cutoff | 17/18 |
| the collector profiles the table instead of the partition | smoke: 211,675 recorded against 27,629 loaded |
| the run is never closed out | smoke: 0 successful runs for 14 days of two tasks |

Six of those kill the suite by crashing it rather than by failing an assertion. A crash is
still a kill and it is a weaker one, because it means the test would not have said which
thing broke.

The null count mutation is the reason this table is worth building. It passed on the first
attempt. The fixture had four rows with two nulls in the column being checked, so counting
the nulls and counting the non nulls gave the same answer. The fixture now has a column
that is null in three rows out of four.
