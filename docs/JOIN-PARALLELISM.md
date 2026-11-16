# Join parallelism

`filter-join` counts an int inner shuffle-hash join. The plan is
`HashJoin(Exchange, Exchange)`. The count does not exchange. It tallies keys on
each owned side and multiplies (`IntPairFilterEncode.tryCount`). At 1e7 rows the
count is 33281390 on every JVM below.

A work unit is one `ForkJoinPool.execute` on the caller. `MorselScheduler` adds
one to a thread-local counter at that call. An inline dispatch does not. The
counter is not read on the morsel path. With no phase probe installed, the phase
notes do not read a clock.

One action visits 1224 morsels: 1222 on the filtered build (1e7 rows, two
partitions, batch 8192) and 2 on the probe (10240 rows). The pool does not see
those morsels. It sees one task per worker slot.

| threads | tasks submitted | build scan | probe scan |
| ---: | ---: | ---: | ---: |
| 4 | 6 | 4 | 2 |
| 8 | 10 | 8 | 2 |

`defaultShufflePartitions` stays 4. Setting it to 32 submits the same tasks.
Submits were identical on every round of every JVM.

This host has CPUs 0–3. `taskset -c 0,2,4,6` binds only 0,2 (4 and 6 are not
CPUs). `taskset -c 0,1,2,3` binds 0–3. Both pins are below. Warmup 80, rounds
30, steady is the upper median of the last 10. Five fresh JVMs, `java -Xmx2g
-cp`. The figure is the median of the five steady times.

## Phase split at 1e7

Milliseconds. Serial is open, classify, setup, merge, and the product, both
sides. Those phases submit nothing.

Named pin, CPUs 0,2:

| threads | action | build scan | probe scan | serial | action verdicts |
| ---: | ---: | ---: | ---: | ---: | --- |
| 4 | 23.001 | 22.311 | 0.140 | 0.136 | five `ok`, span 2.5% |
| 8 | 32.928 | 32.439 | 0.128 | 0.140 | five `ok`, span 22.3–33.7 ms |

Four CPUs, 0–3. Each JVM's own steady is `ok` or, once, `compilation`. The
cross-JVM span is wider than 10%, so the millisecond is the median, not a tight
repeat. Build scan is 93–97% of the action in every JVM.

| threads | action | build scan | probe scan | serial | span of action |
| ---: | ---: | ---: | ---: | ---: | --- |
| 4 | 17.320 | 16.784 | 0.165 | 0.157 | 14.325–18.238 |
| 8 | 15.219 | 14.195 | 0.146 | 0.167 | 14.151–18.783 |

Eight threads submit more tasks and do not beat four. On two cores they are
slower. On four cores the ranges overlap.

`JoinPhaseBench` also times the hash-join replicas. The count does not run them.
One JVM, not five, warmup 80, rounds 30, CPUs 0–3, 4 threads: exchange 15.401,
insert 3.485, append 231.115, and the count action in that same JVM 17.189,
with no exchange phase recorded. The same exchange on CPUs 0,2 was 15.254.

The count is limited by the build-side key scan. That scan already has one pool
task per worker, the serial phases are under 0.2 ms, and more workers than cores
do not shorten it. Raising the shuffle width cannot add tasks.

```
JOIN_PHASE_ROWS=10000000 JOIN_PHASE_WARMUP=80 JOIN_PHASE_ROUNDS=30 \
  JOIN_PHASE_THREADS=4 JOIN_PHASE_ONLY=action \
  taskset -c 0,2,4,6 java -Xmx2g -cp "$CP" \
  com.ewoodbury.scarlet.execution.JoinPhaseBench
```

Same command with `JOIN_PHASE_THREADS=8`, and both widths again under
`taskset -c 0,1,2,3`. Five JVMs each. `$CP` is `scarlet-tests/Test/fullClasspath`.
The replica line is the same command without `JOIN_PHASE_ONLY`, one JVM.
