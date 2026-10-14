# Limitations

Moved out of the README so the front page stays short. Back to the [README](../README.md).

## Known limitations

**The source data is generated, and its weekly shape is assumed rather than observed.**
The day of week factors in `pipeline/generate.py` are a plausible retail pattern that I
chose. So the 80.4 percent figure above is a true statement about this feed and not
evidence about real traffic. Anyone reading it as the latter is reading it wrong.

**That creates a circularity risk for the volume baseline and it is not solved.** If the
generator lays down a clean weekly pattern and the baseline learns that pattern, nothing
has been shown. The 80.4 percent and the 0.406 width ratio are both true statements about
this feed and neither is evidence that the design works. Two things push back. The
generator is multiplicative and the raw space band is additive, so those two configurations
are not the same model and the report runs both. More importantly the test of this baseline
is not fit quality. It is whether it stays quiet through the injected failures further
down.

That test has now run and the volume baseline half passed it. It fired on 3 of 10 clean
partitions, which is not quiet. The duration baseline failed it outright at 10 of 10 for a
reason that turned out to be the filesystem rather than the model. Both are above.

**The findings about the estimators do not depend on the generated data, and the findings
about the seasonal key do.** That one day doubled in the history moves a mean band 45
percent and a median band 4.3 percent is arithmetic. That volume is weekly and duration is
not is a fact about a feed I wrote.

**Figures taken from the generated data rest on an artefact nobody else has.** `data/` is
gitignored because it is generated. That is how a wrong row count survived five days in this
file. The rebuild command is printed above and it is verified deterministic, which lets a
reader check any number here. What is still missing is a checked in checksum of the corpus,
so nothing detects the corpus changing except a person re-reading the figures.

**The metadata schema is create only and still has no migration path.** `schema.check_shape`
now raises at the point of the problem instead of letting an insert fail on a column count
four frames later. It refuses to run against a database
of the wrong shape. It does not migrate one, so an existing database has to be rebuilt or
altered by hand.

**`incident_report.py` writes its arm copies to fixed scratch paths.** A `--scratch` flag
exists now because a previous run left files behind under a different owner and the next run
died on `shutil.copy`. The flag is a workaround. It still does not clean up after itself.

**Every monitor here is fitted once and never refitted.** A real baseline moves as the data
moves, and this one does not. Nothing in the project claims otherwise and nothing implements
it either. It matters more than it looks, because a band fitted on 119 days and never
updated will drift out of usefulness on its own.

**Faults are injected one per partition.** Real incidents arrive together and interact. A
harness that only ever tests one at a time cannot say anything about how the monitors behave
when two things break at once.

**The band has no trend term, so it is fitted on a series that is drifting.** Measured
above at 35 percent of the width on this data. The two ways out both cost something and
neither was taken today.

**Log space is the default and on this data it is not doing much.** The argument for it is
that the generator is multiplicative, so an additive band on raw counts is the wrong shape.
The measurement says the volume bands come out 868.2 wide in log space and 869.2 in raw, a
difference of 0.1 percent, because the spread here is small relative to the level and over
a small range the two are nearly the same. So the default is defended by the form of the
data and not by the numbers. It would start to matter on a series with a much larger
relative spread, and it already matters for duration, where the mean and standard deviation
band is 202.6 ms wide in raw space and 64.5 ms in log.

**k is 3 and it is not tuned.** It is the conventional starting point. The report measures
what it costs at 3, which is a fire rate of 0.025 on the training history for volume, and
nothing has calibrated it against a real cost of a false alarm because there is no real
consumer yet.

**Every fire rate quoted here is measured on the training data.** A band evaluated against
the observations it was fitted on is at its most flattering, so those rates are a floor and
not an estimate. There is no held out period, and there cannot be a meaningful one until
something puts known failures in. The injection harness is that something.

**A quarter of a distribution can move without this schema noticing.** Measured above and
demonstrated with a constructed pair. Storing more probabilities shrinks the blind spot and
never closes it, and the honest fix is a sketch that supports a real distance rather than
more fixed points. That is baked into the metadata schema, and reopening it would throw
away the history. It stays, named.

**The reference for every drift signal is fitted on the whole history and then measured
against it.** Same shape as the fire rates above and the same answer. These are floors.

**Nothing here watches a column that is not in `WATCHED`.** The list is six columns picked
by hand in `scripts/drift_report.py`. Generating them from observed profiles is the obvious
next thing and I did not build it.

**A cold start cannot be labelled from the metadata as it stands.** The 921 ms first run is
correctly flagged and there is no column that says why. Suppressing it needs the collector
to record that the process was cold, and inferring it from a gap in `started_at` would not
survive contact with a real daily schedule, where every run is 24 hours after the last one
and every run is cold. The label belongs in the collector and the suppression belongs next
to the alert routing.

**A log space band cannot hold a zero.** A task fast enough to record 0 ms raises instead
of being clamped, because a floor invented to keep the fit alive is a number nobody chose
appearing in an alert threshold. `build_daily` is already at 6 ms. If it gets faster, the
answer is to record microseconds.

**The MAD collapses when more than half the values are the same.** This is the flip side of
its 50 percent breakdown point rather than a defect, and on integer millisecond durations it
happens often. Four of seven `build_daily` weekday bands came out with zero width. They are
marked and refuse to judge, which is safe, and it does mean a robust estimator on
low resolution data can produce no baseline at all rather than a bad one.

**`daily_orders` groups on the partition date, not on `ordered_at`.** An event that
happened on the 3rd and landed in the 4th's file is counted on the 4th. Freshness
detects it at page severity and nothing repairs it, because a restatement needs a
watermark and a window and neither is here. The window is not chosen, and the reason it
is not chosen is that this feed has never been late, so there is no observed lag to pick
one from. That is the restatement problem and the argument is above.

**A real duration regression will not reach anyone.** `duration_ms` is quarantined, because it
fired on 10 of 10 clean out of sample partitions for a reason that turned out to be the
filesystem the input sat on. It is still collected and still banded and it does not alert.
Refitting per environment is the correct answer and it cannot be validated on one machine.

**A collector failure leaves a gap, and a gap is ambiguous.** `collect_into` catches
everything, because the pipeline should not fall over when the thing watching it does.
What is left behind is a successful run with no dataset metric row. The alerting layer
can fire on that. What it cannot do is tell a broken collector apart from a dataset
nobody pointed the collector at, and both look like silence.

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

**A quantile summary cannot see a shift that preserves the quantiles.** This is the
metadata schema tradeoff arriving in the collector. Seven probabilities do not reconstruct a
distribution, so a bimodal split that leaves the deciles where they were is invisible. The
drift check has to say plainly what it can and cannot detect, and it does.

**The Snowflake DDL has never been run.** It is generated from the same template as the
DuckDB DDL so the two cannot drift apart, and it is unverified. It also carries a real
behaviour difference. Snowflake accepts `PRIMARY KEY` and `FOREIGN KEY` and enforces
neither, so the grain guarantees that hold here are documentation there. The collector has
to treat a duplicate grain as its own problem rather than expect the warehouse to reject
it.

**No Airflow yet.** It belongs here and `pipeline/run.py` is a plain loop today.
The DAG comes when there is more than one task worth scheduling.
