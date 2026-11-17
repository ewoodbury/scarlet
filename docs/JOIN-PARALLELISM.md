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

## Materialising join at 1e7

`filter-joinsum` writes the match columns and sums `left * 31 + right`. The plan on this
JVM is `HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))`, batches 32,
fused 0. Output is 33281390 rows from 3328139 survivors. Checksum `515818008297280`. The
published action (`7955dca`, `sjc-gpu-01`) is 110.700 ms / 593.811 MB, 1.99x behind DuckDB
55.522 ms. The count above is `MatchCount`.

The writer is `JoinWrite.writeGrouped`. Longest chain 5184. Each probe partition is 2560
rows, under `JoinWrite.MaxProbeRows` (65536), and the key and value columns are
live-present, so `JoinPhaseOps.write` takes the grouped arm. That helper copies the branch
in `HashJoin.innerInt32`. `HashJoin`, `JoinWrite`, and `MatchCount` are unchanged.

One JVM is the split. Warmup 80, rounds 30, four threads. `taskset -c 0,2,4,6` binds CPUs
0,2 on this host (CPUs 0–3). Heap is `-Xmx4g`. `-Xmx10g` was killed here (7.7 GiB, no
swap). Steady is the upper median of the last 10. `compilation` stays. `machine` is
dropped. Alloc is the median of five process-wide deltas, in decimal MB. Each phase is its
own timed call.

| phase | steady_ms | verdict | alloc_mb |
| --- | ---: | --- | ---: |
| encode | 5.420 | ok | 82.590 |
| filters | 33.999 | compilation | 107.793 |
| exchange | 13.825 | ok | 68.484 |
| insert | 5.902 | ok | 13.512 |
| write | 92.181 | ok | 403.929 |
| sum | 14.961 | ok | 0.001 |
| action | 185.765 | ok | 593.831 |

Sum and action both return checksum `515818008297280`.

`filters` is `IntPairFilterEncode.tryBatches` (both predicates, survivors only). `encode`
is `BatchCodec.copiedIntPairs` of both inputs. The action's build side is the fused call,
so the encode row's 82.590 MB sits outside the action. filters + exchange + insert + write
is 593.718 MB. The action allocated 593.831 MB.

`write` is 92.181 ms and 403.929 MB. 92.181 ms is more than half of the published 110.700
ms, and it is the largest phase in the table, so the 1.99x is the write of the match
columns. The sum over those columns is 14.961 ms and 0.001 MB.

This host's action in that JVM is 185.765 ms (`ok`). The same-session
`CROSS_QUERY=filter-joinsum` before the harness, `-Xmx10g`, was 196.771 ms (`machine`) and
593.915 MB, same checksum. The 196.771 ms figure is this host. The published 110.700 ms
stays the `sjc-gpu-01` row.

A previous JVM on the split command recorded write `machine` at 179.398 ms (encode
`compilation` at 51.856 ms, action 185.882 ms, alloc matched). That write is dropped. The
table is the JVM whose write and action verdicts are `ok`.

filters + exchange + insert + write + sum is 160.868 ms beside the action's 185.765 ms.
Planner, interpreter, and collection outside those calls account for the rest. That
remainder is not a named phase.

```
JOIN_PHASE_ROWS=10000000 JOIN_PHASE_WARMUP=80 JOIN_PHASE_ROUNDS=30 \
  JOIN_PHASE_THREADS=4 JOIN_PHASE_ONLY=joinsum \
  taskset -c 0,2,4,6 java -Xmx4g -cp "$CP" \
  com.ewoodbury.scarlet.execution.JoinPhaseBench
```

One JVM. `$CP` is `scarlet-tests/Test/fullClasspath`. The whole action is the same pin on
`CrossEngineBench` with `CROSS_QUERY=filter-joinsum` and `java -Xmx10g`.

