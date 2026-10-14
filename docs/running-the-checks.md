# Running the checks

Moved out of the README so the front page stays short. Back to the [README](../README.md).

## Tests

`python -m tests.run_all` is the only entrypoint and CI calls nothing else. A module that
reports zero cases fails the run, because the usual way a suite lies is not a wrong
assertion. It is a file the runner imported and never executed.

`scripts/smoke.py` covers the command line path. It generates two weeks and runs the
range. Then it snapshots the counts, runs the same range again and compares. Then it
runs the observed path and holds the metadata to the warehouse. The row counts in
`obs_dataset_metric` have to add up to the rows actually in `raw_orders`. Every
collector unit test points it at a table built inside the test, so this is the only
place its numbers meet a pipeline that really ran.

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
| `ks_bound` assumes F(x) equals p at its own quantile | 52/53, two identical tied vectors bound apart by 0.49 |
| `ks_bound` reads only the reference vector | 52/53, a provable separation reports as zero |
| `blind_spot` takes the smallest gap | 52/53, the constructed pair no longer reaches it |
| `usable_signals` only checks positive correlation | 52/53, every inverse signal gets through |
| a constant signal is banded instead of held constant | 52/53, a fifth status category passes |
| `column_history` keeps the first attempt | 52/53, a failed retry becomes the observation |
| `column_history` drops the success filter | 52/53, a failed run's profile enters the history |
| `raises_message` stops comparing the message | 52/53, caught by its own probe |
| top values ignore the partition filter | 30/32 |
| the run row is written after the work instead of before | the running row is not there to find |
| the tracker swallows the failure | 15/18 |
| `next_attempt` always returns 1 | the retry overwrites the first attempt |
| `stale_runs` ignores its cutoff | 17/18 |
| the collector profiles the table instead of the partition | smoke: 211,675 recorded against 27,629 loaded |
| the run is never closed out | smoke: 0 successful runs for 14 days of two tasks |
| `median_mad` returns the mean and standard deviation | 53/58 |
| a zero spread band is not marked degenerate | crashes dividing by zero |
| an unknown key is answered `ok` | 57/58 |
| log space clamps a zero instead of raising | 56/58 |
| the last successful attempt becomes the first | 54/58 |
| the defensive sort goes and the rows arrive in reverse attempt order | 54/58 |
| the duration history keeps failed runs | 57/58 |
| an unreadable partition key is dropped without being counted | 56/58 |
| the adjusted variance does not charge for the groups | 57/58 |
| `choose_keying` ignores a key whose spread collapsed | 56/58 |
| `fit_bands` ignores its minimum observation count | 57/58 |
| the log band is built and read in raw space | 56/58 |
| the volume history keeps every attempt | 52/58 |
| the minimum observation count drops to 2, against the smoke check | exit 1, seven weekday bands from a fortnight |
| freshness ignores events from before the partition | 39/40 |
| freshness treats an unknown event time as clean | 38/40 |
| `scan` returns every partition instead of only the failures | 39/40 |
| a negative tolerance is accepted | 39/40 |
| `late_arrival` moves no event to the prior day | inject fails, the fault stops being a fault |
| `last_known_good` accepts a partition nothing checked | 38/40 |
| `all_alerting` and `unjudged` collapse into one state | 39/40 |
| `last_known_good` ignores the partition boundary | 38/40 |
| `upstream_runs` includes the failing task's own run | 39/40 |
| **`last_known_good` walks forward instead of back** | **survived, see below** |
| the quarantine threshold accepts a rate at the limit | 82/82, nothing is ever held out |
| the quarantine divides by a zero observation count | crashes on an unmeasured subject |
| the fire rate bound returns the raw rate instead | 81/82, 10 of 10 reports 1.000 |
| the bound's bisection compares the tail the wrong way | 81/82 |
| `raise_alert` ignores the quarantine flag | 81/82 |
| an unmeasured subject is reported as quiet | 48/49 |
| `noise_note` keeps a zero observation count | 48/49 |
| the render drops the clean rate marker | 47/49 |
| `check_shape` prints instead of raising | 18/21 |
| `check_shape` compares the two lists as sets | **survived, then killed by a reorder fixture** |
| `check_shape` treats a missing table as a mismatch | 20/21 |
| `expected_columns` keeps the constraint lines | 17/21 |
| `apply` skips the shape check | 20/21 |
| `holdout_fire_rate` fits on the whole series | 79/80 |
| `holdout_fire_rate` counts on the training half | 79/80 |
| `holdout_fire_rate` accepts too little training history | 79/80 |

Five of the first twelve baseline mutants survived and three of those five shared a
cause. The history fixture had one successful attempt per partition. So three rules were all
passing without ever being exercised. Which attempt counts, whether failures are excluded,
and whether a retry is double counted. The fixture now
has a partition that failed then succeeded twice, one that only ever failed, and one that
succeeded and then failed on a later attempt.

The fourth survivor is the more embarrassing one. The test asserted that log space raises
`ValueError` on a zero, and `math.log` raises `ValueError` on a zero all by itself, so
deleting the guard changed nothing. It now checks the message.

Six of those kill the suite by crashing it rather than by failing an assertion. A crash is
still a kill and it is a weaker one, because it means the test would not have said which
thing broke.

The null count mutation is the reason this table is worth building. It passed on the first
attempt. The fixture had four rows with two nulls in the column being checked, so counting
the nulls and counting the non nulls gave the same answer. The fixture now has a column
that is null in three rows out of four.

**The next survivor is that same fixture lesson repeating, in a file whose own docstring
cites it.** `last_known_good` walks a history backwards to find the most recent clean
partition. Reversing that sort makes it return the oldest clean partition instead, which is
a different answer to a different question, and the whole suite stayed green. The fixture
had one clean partition in it. With a single clean candidate, forwards and backwards land on
the same row and the direction rule never runs.

The header of `tests/test_timeline.py` claims that fixture tests a rule about choosing
between rows. It did, for the alerting rule and the unchecked rule, both of which had
competing candidates. It did not for the direction rule. Writing the lesson at the top of the
file is not the same as applying it to every rule in the file. The fixture now carries two
clean partitions with different row counts, so the wrong direction returns a different date
and a different reference value.

**Then it happened a third time.** `check_shape` compares the live column list against
the shipped one in order, because a column reorder breaks a positional load and is a
real incident. A mutant comparing them as sets passed the whole suite. The fixture only
ever removed a column, so the ordering rule never ran. Fixed with a table holding
exactly the right columns in the wrong order, and the mutant dies.

Two fixtures, found weeks apart, both of which tested one of the rules in the function
they were pointed at and looked like they tested all of them. The tell in both cases is
a fixture that can only produce one candidate for a rule about choosing between
candidates.
