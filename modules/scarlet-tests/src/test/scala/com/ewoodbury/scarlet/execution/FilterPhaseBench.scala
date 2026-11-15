package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.{Arrays, Locale}

import scala.util.control.NonFatal

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Phase split for a real `filter.count` and `filter.values.sum`. Not a test, and not part of
 * `make test`.
 *
 * The input matches [[CrossEngineBench]]: 1e6 int pairs, keys `row % 1024`, the same LCG values,
 * two source partitions, batch 8192, two filters (`key % 3 != 0`, `value >= 0`), spill on. The
 * pool width is `CROSS_THREADS`, default 4. `CROSS_WARMUP` and `CROSS_ROUNDS` default to 5 and
 * 30. p50 is the upper median, index `n/2` after sorting, the same pick as [[CrossEngineBench]].
 * Allocation is a later pass, five rounds, summed across live threads, and is not inside those
 * samples. `filter-countbykey` is timed as a guard action.
 *
 * [[PhaseBench]] times [[Selection.applySelected]] and an id-copy assemble. Those are not this
 * action. `tryCount` and `trySumInt` fold the source columns in [[FusedPairFold]] and do not
 * encode. The encode, filter, and kernel phases below still time the old pieces:
 * [[SourceEncode]] and [[Selection.count]] or [[Selection.sumInt32]]. Nothing builds a survivor
 * index.
 *
 * Each line is its own p50. Phases are not subtracted from each other or from the action.
 *
 *   - `action` is `count()` or `values.sum()`. It folds the source and publishes no batch. A row
 *     fallback fails the run.
 *   - `lower` is [[ColumnarPlanner.lower]] on that action's plan.
 *   - `compile` rebuilds the two selection steps the action compiles on every call.
 *   - `encode` is the scan copy that call pays. Count uses [[SourceEncode.encodePairs]]. Sum uses
 *     [[SourceEncode.encodeGroupedPairs]]. Both stay on the caller. The long-pair encode
 *     schedules; this one does not.
 *   - `encode-parallel` is not the product path. It schedules those same per-partition copies. The
 *     timed action does not call it.
 *   - `dispatch` is [[MorselScheduler.map]] over the scan morsels. The body returns the length.
 *   - `filter` is that map running [[Selection.count]] or [[Selection.sumInt32]]. The add is in
 *     the same loop as the predicates.
 *   - `kernel` is the same statements as `ColumnarPlanner.foldMorsels`: cut, dispatch, filter,
 *     partial sum. It is the body of the action after encode, not a second kernel.
 *   - `assemble` is the caller sum of morsel partials. There is no index copy.
 *
 * A checksum other than the cross-engine pair, or a decoded row, fails the process. The action
 * publishes no batch and does not encode. The encode phase still expects one batch per source
 * partition.
 *
 * {{{
 * CROSS_WARMUP=5 CROSS_ROUNDS=30 \
 *   sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.FilterPhaseBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object FilterPhaseBench:

  private val rows = 1000000
  private val groups = 1024
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val width = envInt("CROSS_THREADS", 4)
  private val lcgSeed = 0x12345678
  private val countChecksum = 333036L
  private val sumChecksum = 166553391274L
  private val countByKeyChecksum = 10821571L
  private val countShape = "FilterValue(FilterKey(Scan))"
  private val sumShape = "Value(FilterValue(FilterKey(Scan)))"

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  private final case class Prepared(
      name: String,
      shape: String,
      plan: Plan[_],
      compile: () => Int,
      encode: () => Vector[ColumnBatch],
      encodeParallel: () => Vector[ColumnBatch],
      batches: Vector[ColumnBatch],
      morsels: Vector[Morsel],
      partials: Vector[Long],
      expected: Long,
      evaluate: Morsel => Long,
      run: () => Long,
  )

  private final case class Reading(query: String, phase: String, threads: Int, p50Ms: Double)

  private final case class Alloc(query: String, threads: Int, p50Mb: Double)

  def main(args: Array[String]): Unit =
    val warmup = envInt("CROSS_WARMUP", 5)
    val rounds = envInt("CROSS_ROUNDS", 30)
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(width)
    var status = 0
    try
      if (warmup < 0 || rounds <= 0 || width < 1) then
        refuse("warmup must be >= 0, rounds must be > 0, and threads must be >= 1")
      configure(savedConf)
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val prepared = prepare()
      preamble(warmup, rounds, prepared)
      val readings = prepared.flatMap(one => timeAll(one, warmup, rounds))
      val guard = timeGuard(warmup, rounds)
      val allocs = measureAlloc()
      System.err.println(table(readings ++ guard))
      System.err.println(allocTable(allocs))
    catch
      case _: HarnessRefusal => status = 1
      case NonFatal(thrown) =>
        thrown.printStackTrace(System.err)
        status = 1
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def prepare(): Vector[Prepared] =
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    val filtered = DistCollection
      .ofIntPairs(keys, values, sourcePartitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val countPlan = filtered.plan
    val sumPlan = filtered.values.plan
    val countOp = lowered(countPlan, countShape)
    val sumOp = lowered(sumPlan, sumShape)
    val filterOp = sumOp match
      case PhysicalOp.Column(child, 1) if child.kind == PhysicalOp.Kind.Pairs => child
      case other => refuse(s"sum child ${other.shape} is not an int-pair value column")
    oracle(keys, values)
    val count = countPrepared(countPlan, countOp, () => filtered.count())
    val sum = sumPrepared(sumPlan, countPlan, filterOp, () => filtered.values.sum)
    Vector(count, sum)

  private def countPrepared(
      plan: Plan[_],
      op: PhysicalOp,
      run: () => Long,
  ): Prepared =
    val steps = countSteps(plan, op)
    val ordinals = ColumnUse.ordinals(plan, op)
    if (ordinals != Set(0, 1)) then refuse(s"count ordinals $ordinals, expected both columns")
    val partitions = scanPartitions(plan, op)
    val evaluate: Morsel => Long = morsel =>
      Selection.count(morsel.batch, morsel.start, morsel.length, steps)
    build(
      "filter-count",
      countShape,
      plan,
      () => compileCount(plan, op),
      () => SourceEncode.encodePairs(partitions, ordinals),
      () =>
        scheduleEncode(
          partitions,
          partition => SourceEncode.encodePairs(Seq(partition), ordinals),
        ),
      evaluate,
      countChecksum,
      run,
    )

  private def sumPrepared(
      valuesPlan: Plan[_],
      filterPlan: Plan[_],
      filterOp: PhysicalOp,
      run: () => Long,
  ): Prepared =
    val steps = sumSteps(filterPlan, filterOp)
    val partitions = scanPartitions(filterPlan, filterOp)
    val evaluate: Morsel => Long = morsel =>
      Selection.sumInt32(morsel.batch, morsel.start, morsel.length, steps, 1)
    build(
      "filter-sum",
      sumShape,
      valuesPlan,
      () => compileSum(filterPlan, filterOp),
      () => SourceEncode.encodeGroupedPairs(partitions),
      () =>
        scheduleEncode(
          partitions,
          partition => SourceEncode.encodeGroupedPairs(Seq(partition)),
        ),
      evaluate,
      sumChecksum,
      run,
    )

  private def build(
      name: String,
      shape: String,
      plan: Plan[_],
      compile: () => Int,
      encode: () => Vector[ColumnBatch],
      encodeParallel: () => Vector[ColumnBatch],
      evaluate: Morsel => Long,
      expected: Long,
      run: () => Long,
  ): Prepared =
    val batches = encode()
    acceptRows(name, batches, rows)
    val morsels = batches.flatMap(batch => Morsels.of(batch, batchSize))
    if (morsels.length <= sourcePartitions) then
      refuse(s"$name cut ${morsels.length} morsels from $sourcePartitions partitions")
    val partials = MorselScheduler.map(morsels, width)((_, morsel) => evaluate(morsel))
    val folded = partials.foldLeft(0L)(_ + _)
    if (folded != expected) then refuse(s"$name walk $folded != $expected")
    acceptAction(name, expected, morsels.length, run)
    if (compile() != 2) then refuse(s"$name compiled ${compile()} steps")
    Prepared(
      name,
      shape,
      plan,
      compile,
      encode,
      encodeParallel,
      batches,
      morsels,
      partials,
      expected,
      evaluate,
      run,
    )

  private def timeAll(prepared: Prepared, warmup: Int, rounds: Int): Vector[Reading] =
    Vector(
      time(prepared.name, "action", width, warmup, rounds)(clearShuffle())(prepared.run()) {
        (got, sink) =>
          acceptResult(prepared.name, "action", got, prepared.expected)
          acceptActionNotes(prepared.name, sink, prepared.morsels.length)
      },
      time(prepared.name, "lower", 1, warmup, rounds)(ready())(
        lower(prepared.plan, prepared.shape),
      ) { (shape, _) =>
        if (shape != prepared.shape) then
          refuse(s"${prepared.name} lower $shape != ${prepared.shape}")
      },
      time(prepared.name, "compile", 1, warmup, rounds)(ready())(prepared.compile()) {
        (steps, _) =>
          if (steps != 2) then refuse(s"${prepared.name} compile produced $steps steps")
      },
      time(prepared.name, "encode", 1, warmup, rounds)(ready())(prepared.encode()) {
        (batches, sink) =>
          acceptRows(prepared.name, batches, rows)
          acceptEncodeNotes(prepared.name, sink)
      },
      time(prepared.name, "encode-parallel", width, warmup, rounds)(ready())(
        prepared.encodeParallel(),
      ) { (batches, sink) =>
        acceptRows(prepared.name, batches, rows)
        acceptEncodeNotes(prepared.name, sink)
      },
      time(prepared.name, "dispatch", width, warmup, rounds)(ready())(dispatch(prepared.morsels)) {
        (lengths, sink) =>
          acceptDispatch(prepared.name, lengths, prepared.morsels.length, sink)
      },
      time(prepared.name, "filter", width, warmup, rounds)(ready())(filter(prepared)) {
        (partials, sink) =>
          acceptPartials(prepared, "filter", partials, sink)
      },
      time(prepared.name, "kernel", width, warmup, rounds)(ready())(kernel(prepared)) {
        (got, sink) =>
          acceptResult(prepared.name, "kernel", got, prepared.expected)
          acceptVisits(prepared.name, "kernel", sink, prepared.morsels.length)
      },
      time(prepared.name, "assemble", 1, warmup, rounds)(ready()) {
        prepared.partials.foldLeft(0L)(_ + _)
      } { (got, _) =>
        acceptResult(prepared.name, "assemble", got, prepared.expected)
      },
    )

  /**
   * One batch per partition, scheduled across the pool. [[SourceEncode.encodePairs]] on one
   * partition does not schedule again.
   */
  private def scheduleEncode(
      partitions: Seq[Partition[_]],
      encodeOne: Partition[_] => Vector[ColumnBatch],
  ): Vector[ColumnBatch] =
    MorselScheduler.map(partitions.toVector, width) { (_, partition) =>
      val batches = encodeOne(partition)
      if (batches.length != 1) then
        refuse(s"parallel encode produced ${batches.length} batches for one partition")
      batches.headOption.getOrElse(refuse("parallel encode produced no batch"))
    }

  private def dispatch(morsels: Vector[Morsel]): Vector[Long] =
    MorselScheduler.map(morsels, width)((_, morsel) => morsel.length.toLong)

  private def filter(prepared: Prepared): Vector[Long] =
    MorselScheduler.map(prepared.morsels, width)((_, morsel) => prepared.evaluate(morsel))

  /** Same statements as `ColumnarPlanner.foldMorsels`. That method is private to the planner. */
  private def kernel(prepared: Prepared): Long =
    val morsels = prepared.batches.flatMap(batch => Morsels.of(batch, batchSize))
    val partials = MorselScheduler.map(morsels, width)((_, morsel) => prepared.evaluate(morsel))
    partials.foldLeft(0L)(_ + _)

  /** Runs outside the timer. The action clears the shuffle here, as [[CrossEngineBench]] does. */
  private def clearShuffle(): Unit =
    ScarletRuntime.get.shuffle.clear()

  private def ready(): Unit = ()

  private def time[A](
      query: String,
      phase: String,
      threads: Int,
      warmup: Int,
      rounds: Int,
  )(before: => Unit)(body: => A)(check: (A, CountingSink) => Unit): Reading =
    System.err.println(s"sample $query $phase threads=$threads")
    val p50 = sample(warmup, rounds)(before)(body)(check)
    val reading = Reading(query, phase, threads, p50)
    println(line(query, reading))
    reading

  private def sample[A](warmup: Int, rounds: Int)(before: => Unit)(body: => A)(
      check: (A, CountingSink) => Unit,
  ): Double =
    var round = 0
    while (round < warmup) do
      val _ = observe(before)(body)(check)
      round += 1
    val samples = new Array[Double](rounds)
    round = 0
    while (round < rounds) do
      samples(round) = observe(before)(body)(check)
      round += 1
    median(samples)

  private def observe[A](before: => Unit)(body: => A)(check: (A, CountingSink) => Unit): Double =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      before
      val started = System.nanoTime()
      val produced = body
      val elapsed = (System.nanoTime() - started) / 1_000_000.0
      check(produced, sink)
      elapsed
    finally ColumnMetrics.restore(previous)

  private def lower(plan: Plan[_], shape: String): String =
    ColumnarPlanner.lower(plan) match
      case Some(op) => op.shape
      case None => refuse(s"lower refused, expected $shape")

  private def lowered(plan: Plan[_], shape: String): PhysicalOp =
    ColumnarPlanner.lower(plan) match
      case Some(op) if op.shape == shape => op
      case Some(op) => refuse(s"lower shape ${op.shape} != $shape")
      case None => refuse(s"lower refused, expected $shape")

  private def countSteps(plan: Plan[_], op: PhysicalOp): List[Selection.Step] =
    val compiled =
      PredicatePlan.compile(plan, op).getOrElse(refuse("count filters did not compile"))
    val steps = PredicatePlan
      .selectionSteps(PredicatePlan.encoded(compiled, ColumnUse.ordinals(plan, op)))
      .getOrElse(refuse("count filters have no selection steps"))
      .toList
    if (steps.length != 2) then refuse(s"count compiled ${steps.length} steps")
    steps

  private def sumSteps(plan: Plan[_], op: PhysicalOp): List[Selection.Step] =
    val compiled = PredicatePlan.compile(plan, op).getOrElse(refuse("sum filters did not compile"))
    val steps = PredicatePlan
      .selectionSteps(compiled)
      .getOrElse(refuse("sum filters have no selection steps"))
      .toList
    if (steps.length != 2) then refuse(s"sum compiled ${steps.length} steps")
    steps

  private def compileCount(plan: Plan[_], op: PhysicalOp): Int =
    countSteps(plan, op).length

  private def compileSum(plan: Plan[_], op: PhysicalOp): Int =
    sumSteps(plan, op).length

  private def scanPartitions(plan: Plan[_], op: PhysicalOp): Seq[Partition[_]] =
    PredicatePlan.childOf(plan, op) match
      case (_, PhysicalOp.Scan(PhysicalOp.Kind.Pairs, partitions)) =>
        if (partitions.length != sourcePartitions) then
          refuse(s"${partitions.length} source partitions, expected $sourcePartitions")
        partitions
      case (_, other) => refuse(s"expected a pair scan, found ${other.shape}")

  private def acceptAction(
      name: String,
      expected: Long,
      morsels: Int,
      run: () => Long,
  ): Unit =
    ScarletRuntime.get.shuffle.clear()
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    val got =
      try run()
      finally ColumnMetrics.restore(previous)
    acceptResult(name, "setup", got, expected)
    acceptActionNotes(name, sink, morsels)

  private def acceptActionNotes(name: String, sink: CountingSink, morsels: Int): Unit =
    if (sink.decodedValueCount != 0L) then refuse(s"$name decoded ${sink.decodedValueCount} rows")
    if (sink.primitiveEncodeCount != 0L) then
      refuse(s"$name encoded ${sink.primitiveEncodeCount} batches, expected 0")
    if (sink.batchCount != 0L) then
      refuse(s"$name published ${sink.batchCount} batches, expected 0")
    acceptVisits(name, "action", sink, morsels)

  private def acceptEncodeNotes(name: String, sink: CountingSink): Unit =
    if (sink.decodedValueCount != 0L) then
      refuse(s"$name encode decoded ${sink.decodedValueCount} rows")
    if (sink.primitiveEncodeCount != sourcePartitions.toLong) then
      refuse(s"$name encode notes ${sink.primitiveEncodeCount}, expected $sourcePartitions")
    if (sink.batchCount != sourcePartitions.toLong) then
      refuse(s"$name encode published ${sink.batchCount} batches, expected $sourcePartitions")

  private def acceptDispatch(
      name: String,
      lengths: Vector[Long],
      morsels: Int,
      sink: CountingSink,
  ): Unit =
    if (lengths.length != morsels) then
      refuse(s"$name dispatch returned ${lengths.length} lengths, expected $morsels")
    val total = lengths.foldLeft(0L)(_ + _)
    if (total != rows.toLong) then refuse(s"$name dispatch length $total != $rows")
    acceptVisits(name, "dispatch", sink, morsels)

  private def acceptPartials(
      prepared: Prepared,
      phase: String,
      partials: Vector[Long],
      sink: CountingSink,
  ): Unit =
    if (partials.length != prepared.morsels.length) then
      refuse(s"${prepared.name} $phase returned ${partials.length} partials")
    val total = partials.foldLeft(0L)(_ + _)
    acceptResult(prepared.name, phase, total, prepared.expected)
    acceptVisits(prepared.name, phase, sink, prepared.morsels.length)

  private def acceptVisits(name: String, phase: String, sink: CountingSink, morsels: Int): Unit =
    if (sink.visitCount != morsels.toLong) then
      refuse(s"$name $phase visited ${sink.visitCount} morsels, expected $morsels")

  private def acceptRows(name: String, batches: Vector[ColumnBatch], expected: Int): Unit =
    val total = batches.foldLeft(0L)((sum, batch) => sum + batch.length.toLong)
    if (total != expected.toLong) then refuse(s"$name encoded $total rows, expected $expected")
    if (batches.length != sourcePartitions) then
      refuse(s"$name encoded ${batches.length} batches, expected $sourcePartitions")

  private def acceptResult(name: String, phase: String, got: Long, expected: Long): Unit =
    if (got != expected) then refuse(s"$name $phase $got != $expected")

  private def oracle(keys: Array[Int], values: Array[Int]): Unit =
    var row = 0
    var counted = 0L
    var summed = 0L
    while (row < rows) do
      val key = keys(row)
      val value = values(row)
      if (key % 3 != 0 && value >= 0) then
        counted += 1L
        summed += value.toLong
      row += 1
    if (counted != countChecksum) then refuse(s"oracle count $counted != $countChecksum")
    if (summed != sumChecksum) then refuse(s"oracle sum $summed != $sumChecksum")

  private def lcgValues(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = lcgSeed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = (mixed % 2000001) - 1000000
      index += 1
    generated

  /** Upper median. Thirty rounds use index 15, the same slot as [[CrossEngineBench]]. */
  private def median(samples: Array[Double]): Double =
    Arrays.sort(samples)
    samples(samples.length / 2)

  private def configure(saved: ScarletConf): Unit =
    ScarletConf.set(
      saved.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = width,
        spillEnabled = true,
        threadPoolSize = width,
      ),
    )
    if (ColumnarPlanner.batchSize != batchSize) then
      refuse(s"batch size ${ColumnarPlanner.batchSize} != $batchSize")
    if (ColumnarPlanner.parallelism != width) then
      refuse(s"parallelism ${ColumnarPlanner.parallelism} != $width")

  private def preamble(warmup: Int, rounds: Int, prepared: Vector[Prepared]): Unit =
    val runtime = ManagementFactory.getRuntimeMXBean
    val morsels = prepared.headOption.map(_.morsels.length).getOrElse(0)
    System.err.println(
      s"filter-phase rows=$rows morsels=$morsels batch=$batchSize pool=$width " +
        s"warmup=$warmup rounds=$rounds spill=true p50=upper-median " +
        s"count=$countChecksum sum=$sumChecksum " +
        s"java=${runtime.getVmName} ${runtime.getVmVersion}",
    )

  private def table(readings: Vector[Reading]): String =
    val header = Vector(
      "",
      "| query | phase | threads | p50 ms |",
      "| --- | --- | ---: | ---: |",
    )
    val body = readings.map { reading =>
      s"| ${reading.query} | ${reading.phase} | ${reading.threads} | ${millis(reading.p50Ms)} |"
    }
    (header ++ body).mkString("\n")

  private def line(query: String, reading: Reading): String =
    "{" +
      """"schema":"filter-phase-1",""" +
      s""""query":"$query",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""rows":$rows,""" +
      s""""p50_ms":${millis(reading.p50Ms)},""" +
      s""""checksum":${checksum(query)}}"""

  /** Same two filters as the phase queries. Built outside the timer. */
  private def filteredPairs(): DistCollection[(Int, Int)] =
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    DistCollection
      .ofIntPairs(keys, values, sourcePartitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)

  private def timeGuard(warmup: Int, rounds: Int): Vector[Reading] =
    val filtered = filteredPairs()
    Vector(
      time("filter-countbykey", "action", width, warmup, rounds)(clearShuffle()) {
        FilterCountChecksum.collected(filtered.countByKey.collect(), groups)
      } { (got, _) =>
        if (got != countByKeyChecksum) then
          refuse(s"filter-countbykey $got != $countByKeyChecksum")
      },
    )

  private def measureAlloc(): Vector[Alloc] =
    val bean = threadBean.getOrElse(refuse("thread allocation counter is unavailable"))
    bean.setThreadAllocatedMemoryEnabled(true)
    if (!bean.isThreadAllocatedMemoryEnabled) then
      refuse("thread allocation counter did not enable")
    val filtered = filteredPairs()
    Vector(
      sampleAlloc("filter-count", countChecksum, bean)(filtered.count()),
      sampleAlloc("filter-sum", sumChecksum, bean)(filtered.values.sum),
      sampleAlloc("filter-countbykey", countByKeyChecksum, bean) {
        FilterCountChecksum.collected(filtered.countByKey.collect(), groups)
      },
    )

  private def sampleAlloc(query: String, expected: Long, bean: ThreadMXBean)(
      body: => Long,
  ): Alloc =
    var warm = 0
    while (warm < 2) {
      clearShuffle()
      val got = body
      if (got != expected) then refuse(s"$query alloc warmup $got != $expected")
      warm += 1
    }
    val samples = new Array[Double](allocRounds)
    var round = 0
    while (round < allocRounds) {
      clearShuffle()
      val (got, bytes) = accounted(bean)(body)
      if (got != expected) then refuse(s"$query alloc $got != $expected")
      samples(round) = bytes.toDouble / 1000000.0
      round += 1
    }
    val reading = Alloc(query, width, median(samples))
    println(allocLine(reading))
    System.err.println(s"alloc $query threads=$width p50_mb=${millis(reading.p50Mb)}")
    reading

  private def accounted[A](bean: ThreadMXBean)(body: => A): (A, Long) =
    val beforeIds = new Array[Long](allocSlots)
    val beforeBytes = new Array[Long](allocSlots)
    val before = snapshot(bean, beforeIds, beforeBytes)
    val value = body
    val afterIds = new Array[Long](allocSlots)
    val afterBytes = new Array[Long](allocSlots)
    val after = snapshot(bean, afterIds, afterBytes)
    (value, byteDelta(before, beforeIds, beforeBytes, after, afterIds, afterBytes))

  private def snapshot(bean: ThreadMXBean, ids: Array[Long], bytes: Array[Long]): Int =
    val live = bean.getAllThreadIds
    if (live.length > ids.length) then
      refuse(s"alloc probe saw ${live.length} threads, capacity ${ids.length}")
    var index = 0
    while (index < live.length) {
      ids(index) = live(index)
      bytes(index) = bean.getThreadAllocatedBytes(live(index))
      index += 1
    }
    live.length

  private def byteDelta(
      before: Int,
      beforeIds: Array[Long],
      beforeBytes: Array[Long],
      after: Int,
      afterIds: Array[Long],
      afterBytes: Array[Long],
  ): Long =
    var total = 0L
    var index = 0
    while (index < after) {
      val grew = afterBytes(index) - baselineBytes(afterIds(index), before, beforeIds, beforeBytes)
      if (grew > 0L) then total += grew
      index += 1
    }
    total

  private def baselineBytes(
      id: Long,
      count: Int,
      ids: Array[Long],
      bytes: Array[Long],
  ): Long =
    var index = 0
    var found = 0L
    while (index < count) {
      if (ids(index) == id) then found = bytes(index)
      index += 1
    }
    found

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  private def allocTable(allocs: Vector[Alloc]): String =
    val header = Vector(
      "",
      "| query | threads | p50 alloc MB |",
      "| --- | ---: | ---: |",
    )
    val body = allocs.map { alloc =>
      s"| ${alloc.query} | ${alloc.threads} | ${millis(alloc.p50Mb)} |"
    }
    (header ++ body).mkString("\n")

  private def allocLine(alloc: Alloc): String =
    "{" +
      """"schema":"filter-phase-alloc-1",""" +
      s""""query":"${alloc.query}",""" +
      s""""threads":${alloc.threads},""" +
      s""""rows":$rows,""" +
      s""""p50_alloc_mb":${millis(alloc.p50Mb)},""" +
      s""""checksum":${checksum(alloc.query)}}"""

  private val allocRounds = 5
  private val allocSlots = 512

  private def checksum(query: String): Long =
    query match
      case "filter-count" => countChecksum
      case "filter-sum" => sumChecksum
      case "filter-countbykey" => countByKeyChecksum
      case other => refuse(s"no checksum for $other")

  private def millis(value: Double): String =
    String.format(Locale.ENGLISH, "%.3f", value)

  private def envInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) => value
          case None => refuse(s"$name must be an integer, found $text")

  private def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)
