package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.{Arrays, Locale}

import scala.util.control.NonFatal

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.columnar.JoinCountTrace.Probe
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.ScarletRuntime.RuntimeComponents
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Phase split for cross-engine `filter-join`. Not a test, and not part of `make test`.
 *
 * The input matches [[CrossEngineBench]]: 1e6 int pairs, keys `row % 1024`, the same LCG values,
 * two source partitions, batch 8192, shuffle width 4, two filters (`key % 3 != 0`, `value >= 0`),
 * joined to 10240 rows (10 copies of `0 until 1024`). The action is `count`. Spill stays on. The
 * build fits the budget, so the join stays in memory.
 *
 * The filtered side is the left input, and the shuffle hash builds that side. The right input is
 * the probe and is not filtered. `probe-selection` is [[FilterChain.selected]] on that scan, which
 * returns no index. `build-selection` is the two filters on the left, morsels then the id copy.
 * `exchange` scatters the survivor index and the dense probe. `insert`, `probe-walk`, and `append`
 * are one partition each of the in-memory table: insert, the match count, and the per-row write.
 * `append-grouped` is [[JoinWrite]]. `kernel` is [[HashJoin.innerInt32]]. `count` sums lengths of
 * an already written join. `action` is the real `count`, including encode.
 *
 * Each line is its own nearest-rank p50 over `JOIN_PHASE_ROUNDS` rounds (default 40) after
 * `JOIN_PHASE_WARMUP` warmups (default 5). `steady_ms` is the upper median of the last 10 rounds,
 * with the same `ok` / `compilation` / `machine` rule as [[CrossEngineBench]]. Phases are not
 * subtracted. `build-selection` still times [[Selection.applySelected]] and the id copy. The
 * production count does not: an owned int-pair slot stack is folded into the encode, and that cost
 * is inside `action`. Four threads run before one thread, so the morsel pool is sized at 4.
 *
 * `JOIN_PHASE_ROWS` selects the filtered side (default 1e6). The right side stays 10240 rows. At
 * 1e7 the match count is about 33.3e6 and the output-row guard scales with that.
 *
 * `JOIN_PHASE_THREADS`, when set, is the only pool width, and it is installed before the first
 * dispatch so the morsel pool is that size. Unset keeps the historical four-then-one run, which
 * sizes the pool at four. `JOIN_PHASE_ONLY=action` times the real `count` and the phases
 * [[JoinCountTrace]] records on that call. It does not time the hash-join replicas. The count
 * multiplies key tallies. It does not exchange. Submits are `pool.execute` calls on the caller.
 *
 * `JOIN_PHASE_ONLY=joinsum` does not time the count. [[JoinSumPhase]] splits `filter-joinsum`:
 * encode, filters, exchange, insert, the writer the kernel selects, and the value-pair sum.
 *
 * {{{
 * JOIN_PHASE_ROWS=10000000 JOIN_PHASE_WARMUP=80 JOIN_PHASE_ROUNDS=30 \
 *   sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.JoinPhaseBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object JoinPhaseBench:

  private val rows = readInt("JOIN_PHASE_ROWS", 1000000)
  private val groups = 1024
  private val joinRows = 10 * groups
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val width = 4
  private val warmup = readInt("JOIN_PHASE_WARMUP", 5)
  private val rounds = readInt("JOIN_PHASE_ROUNDS", 40)
  private val requestedThreads = readOptionalInt("JOIN_PHASE_THREADS")
  private val actionOnly = sys.env.get("JOIN_PHASE_ONLY").contains("action")
  private val actionPhases = Array[String](
    "build-open",
    "build-classify",
    "build-setup",
    "build-scan",
    "build-merge",
    "probe-open",
    "probe-classify",
    "probe-setup",
    "probe-scan",
    "probe-merge",
    "product",
  )
  private val lcgSeed = 0x12345678

  private val keepKey: ColumnKernel.Int32Predicate = (key: Int) => key % 3 != 0
  private val keepValue: ColumnKernel.Int32Predicate = (value: Int) => value >= 0
  private val steps: List[Selection.Step] = List(
    Selection.Step.Int32(0, keepKey),
    Selection.Step.Int32(1, keepValue),
  )

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  private def refuse(message: String): Nothing =
    throw new HarnessRefusal(message)

  private final case class Owned(keys: Array[Int], values: Array[Int])

  private final case class Part(build: ColumnBatch, probe: ColumnBatch, table: JoinPhaseOps.Table)

  private final case class Prepared(
      ownedLeft: Vector[Owned],
      ownedRight: Vector[Owned],
      leftBatches: Vector[ColumnBatch],
      rightBatches: Vector[ColumnBatch],
      leftHits: Vector[Vector[Selected]],
      assembled: Vector[Selected],
      buildBuckets: Vector[ColumnBatch],
      probeBuckets: Vector[ColumnBatch],
      parts: Vector[Part],
      reserves: Vector[Int],
      joined: Vector[ColumnBatch],
      outputRows: Long,
      fold: Long,
      action: () => Long,
      probePlan: PlanRef,
  )

  /** Logical and physical scan, so probe selection is the real `None` path. */
  private final case class PlanRef(plan: Plan[_], op: PhysicalOp)

  private final case class Reading(
      phase: String,
      threads: Int,
      p50Ms: Double,
      steadyMs: Double,
      verdict: String,
      submits: Option[Long],
      visits: Option[Long],
  )

  def main(args: Array[String]): Unit =
    if (sys.env.get("JOIN_PHASE_ONLY").contains("joinsum")) then JoinSumPhase.run()
    else countMain()

  private def countMain(): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val lead = requestedThreads.getOrElse(width)
    val schedulers = schedulersFor(lead)
    var status = 0
    try
      configure(savedConf, lead)
      ScarletRuntime.set(savedRuntime.copy(scheduler = schedulers(lead)))
      val prepared = prepare()
      accept(prepared)
      preamble(prepared)
      val readings = requestedThreads match
        case Some(threads) =>
          runWidths(savedConf, savedRuntime, schedulers, prepared, Vector(threads))
        case None => runWidths(savedConf, savedRuntime, schedulers, prepared, Vector(width, 1))
      System.err.println(table(readings))
    catch
      case refused: HarnessRefusal =>
        System.err.println(refused.getMessage)
        status = 1
      case NonFatal(thrown) =>
        thrown.printStackTrace(System.err)
        status = 1
    finally
      try schedulers.values.foreach(_.shutdown().unsafeRunSync())
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def prepare(): Prepared =
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    val joinKeys = Array.tabulate(joinRows)(row => row % groups)
    val joinValues = Array.tabulate(joinRows)(row => row % 100)
    val ownedLeft = owned(keys, values)
    val ownedRight = owned(joinKeys, joinValues)
    val leftBatches = encode(ownedLeft)
    val rightBatches = encode(ownedRight)
    val leftMorsels = leftBatches.flatMap(batch => Morsels.of(batch, batchSize))
    val leftHits = groupHits(leftBatches, selectMorsels(leftMorsels, width))
    val assembled = leftHits.zip(leftBatches).map { (hits, batch) =>
      assemble(batch, hits)
    }
    val buildBuckets = HashExchange.partitionAllSelected(assembled, 0, width, width)
    val probeBuckets = HashExchange.partitionAll(rightBatches, 0, width, width)
    val tables = buildBuckets.map(JoinPhaseOps.insert)
    val parts = buildBuckets.zip(probeBuckets).zip(tables).map { case ((build, probe), table) =>
      Part(build, probe, table)
    }
    val reserves = parts.map(part => JoinPhaseOps.walk(part.table, part.probe))
    val written = parts.zip(reserves).map { (part, reserved) =>
      JoinPhaseOps.append(part.table, part.build, part.probe, math.max(reserved, 1))
    }
    val grouped = parts.zip(reserves).map { (part, reserved) =>
      JoinPhaseOps.appendGrouped(part.table, part.build, part.probe, math.max(reserved, 1))
    }
    val kernel = parts.map(part => JoinPhaseOps.kernel(part.build, part.probe))
    val outputRows = ColumnReduce.count(kernel)
    if (outputRows != ColumnReduce.count(written) || outputRows != ColumnReduce.count(grouped))
    then refuse("writer row count drifted from the kernel")
    if (kernel.zip(written).exists((left, right) => !JoinPhaseOps.same(left, right))) then
      refuse("replica bytes drifted from the kernel")
    if (kernel.zip(grouped).exists((left, right) => !JoinPhaseOps.same(left, right))) then
      refuse("grouped bytes drifted from the kernel")
    val fold = foldOf(kernel)
    val left = DistCollection
      .ofIntPairs(keys, values, sourcePartitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val right = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val joined = left.join(right)
    val probePlan = scanOf(right.plan)
    Prepared(
      ownedLeft,
      ownedRight,
      leftBatches,
      rightBatches,
      leftHits,
      assembled,
      buildBuckets,
      probeBuckets,
      parts,
      reserves,
      kernel,
      outputRows,
      fold,
      () => joined.count(),
      probePlan,
    )

  private def accept(prepared: Prepared): Unit =
    val low = rows.toLong * 3L
    val high = rows.toLong * 36L / 10L
    if (prepared.outputRows < low || prepared.outputRows > high) then
      refuse(s"output rows ${prepared.outputRows} are outside $low until $high")
    val selected = prepared.assembled.foldLeft(0L)((sum, one) => sum + one.count.toLong)
    if (selected * 10L != prepared.outputRows) then
      refuse(s"survivors $selected * 10 != ${prepared.outputRows}")
    if (!prepared.parts.forall(part => JoinPhaseOps.liveValues(part.build))) then
      refuse("build bucket has a null")
    if (!prepared.parts.forall(part => JoinPhaseOps.liveValues(part.probe))) then
      refuse("probe bucket has a null")
    ScarletRuntime.get.shuffle.clear()
    val counted = prepared.action()
    if (counted != prepared.outputRows) then
      refuse(s"action count $counted != ${prepared.outputRows}")

  private def schedulersFor(lead: Int): Map[Int, LocalTaskScheduler] =
    Vector(lead, width, 1).distinct
      .map(threads => threads -> new LocalTaskScheduler(threads))
      .toMap

  private def runWidths(
      saved: ScarletConf,
      runtime: RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      prepared: Prepared,
      widths: Vector[Int],
  ): Vector[Reading] =
    widths.flatMap { threads =>
      val traced = timeActionSplit(saved, runtime, schedulers, prepared, threads)
      if (actionOnly) then traced
      else timeAll(saved, runtime, schedulers, prepared, threads) ++ traced
    }

  /**
   * One sampling loop for the real count. Leaf phases come from [[JoinCountTrace]] on that same
   * call, so they are not a second reconstruction. Submits and visits must be the same on every
   * round. A shuffle width of 32 must submit the same tasks: the count does not read it.
   */
  private def timeActionSplit(
      saved: ScarletConf,
      runtime: RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      prepared: Prepared,
      threads: Int,
  ): Vector[Reading] =
    configure(saved, threads)
    ScarletRuntime.set(runtime.copy(scheduler = schedulers(threads)))
    System.err.println(s"sample action-split threads=$threads")
    val probe = new PhaseProbe
    val previous = JoinCountTrace.install(probe)
    val phaseSamples = Array.tabulate(actionPhases.length)(_ => new Array[Double](rounds))
    val phaseSubmits = Array.tabulate(actionPhases.length)(_ => new Array[Long](rounds))
    val actionSamples = new Array[Double](rounds)
    val visitSamples = new Array[Long](rounds)
    try
      var round = 0
      while (round < warmup) {
        probe.clear()
        val counted = prepared.action()
        if (counted != prepared.outputRows) then refuse(s"warmup count $counted")
        round += 1
      }
      round = 0
      while (round < rounds) {
        probe.clear()
        val sink = new CountingSink
        val previousSink = ColumnMetrics.install(sink)
        val started = System.nanoTime()
        val counted =
          try prepared.action()
          finally ColumnMetrics.restore(previousSink)
        val elapsed = (System.nanoTime() - started) / 1_000_000.0
        if (counted != prepared.outputRows) then refuse(s"action count $counted")
        actionSamples(round) = elapsed
        visitSamples(round) = sink.visitCount
        val seen = probe.snapshot()
        val names = Array.tabulate(seen.length)(index => seen(index)._1)
        if (!names.sameElements(actionPhases)) then
          refuse(
            s"action phases ${names.mkString(",")} expected ${actionPhases.mkString(",")}",
          )
        var index = 0
        while (index < actionPhases.length) {
          val (_, nanos, submits) = seen(index)
          phaseSamples(index)(round) = nanos / 1_000_000.0
          phaseSubmits(index)(round) = submits
          index += 1
        }
        round += 1
      }
    finally JoinCountTrace.restore(previous)
    val visits = stableLong(visitSamples, "visits")
    val leaves = actionPhases.indices.map { index =>
      val submits = stableLong(phaseSubmits(index), actionPhases(index))
      val steady = steadyOf(phaseSamples(index))
      val reading = Reading(
        actionPhases(index),
        threads,
        nearest(phaseSamples(index)),
        steady.ms,
        steady.verdict,
        Some(submits),
        None,
      )
      println(parallelLine(reading))
      reading
    }.toVector
    val actionSteady = steadyOf(actionSamples)
    val actionSubmits = leaves.foldLeft(0L)((sum, reading) => sum + reading.submits.getOrElse(0L))
    val action = Reading(
      "action",
      threads,
      nearest(actionSamples),
      actionSteady.ms,
      actionSteady.verdict,
      Some(actionSubmits),
      Some(visits),
    )
    println(parallelLine(action))
    checkShuffleWidth(prepared, threads, actionSubmits)
    leaves :+ action

  /** The count's submit total must not move when the shuffle width does. */
  private def checkShuffleWidth(prepared: Prepared, threads: Int, expected: Long): Unit =
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(defaultShufflePartitions = 32))
    try
      val before = MorselScheduler.submittedOnCaller
      val counted = prepared.action()
      val submits = MorselScheduler.submittedOnCaller - before
      if (counted != prepared.outputRows) then refuse(s"wide shuffle count $counted")
      if (submits != expected) then
        refuse(s"shuffle width 32 submitted $submits, the timed width submitted $expected")
      println(
        "{" +
          """"schema":"join-parallel-1",""" +
          """"phase":"shuffle-width",""" +
          s""""threads":$threads,""" +
          """"width":32,""" +
          s""""submits":$submits,""" +
          """"same":true}""",
      )
    finally ScarletConf.set(saved)

  private def stableLong(values: Array[Long], label: String): Long =
    val first = values(0)
    var index = 1
    while (index < values.length) {
      if (values(index) != first) then
        refuse(s"$label changed from $first to ${values(index)} on round $index")
      index += 1
    }
    first

  private final class PhaseProbe extends Probe:
    private val names = new Array[String](16)
    private val nanos = new Array[Long](16)
    private val submits = new Array[Long](16)
    private val used = new Array[Int](1)

    def add(phase: String, elapsed: Long, submitted: Long): Unit =
      val index = used(0)
      if (index >= names.length) then
        throw new IllegalStateException(s"join count recorded more than ${names.length} phases")
      names(index) = phase
      nanos(index) = elapsed
      submits(index) = submitted
      used(0) = index + 1

    def snapshot(): Array[(String, Long, Long)] =
      Array.tabulate(used(0))(index => (names(index), nanos(index), submits(index)))

    def clear(): Unit =
      used(0) = 0

  private def timeAll(
      saved: ScarletConf,
      runtime: RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      prepared: Prepared,
      threads: Int,
  ): Vector[Reading] =
    configure(saved, threads)
    ScarletRuntime.set(runtime.copy(scheduler = schedulers(threads)))
    Vector(
      time("encode", threads)(encode(prepared.ownedLeft) ++ encode(prepared.ownedRight)) {
        batches =>
          val total = rowCount(batches)
          if (total != rows.toLong + joinRows.toLong) then
            refuse(s"encoded $total rows, expected ${rows + joinRows}")
      },
      time("build-selection", threads) {
        val morsels = prepared.leftBatches.flatMap(batch => Morsels.of(batch, batchSize))
        val hits = groupHits(prepared.leftBatches, selectMorsels(morsels, threads))
        hits.zip(prepared.leftBatches).map { (group, batch) =>
          assemble(batch, group)
        }
      } { selected =>
        val total = selected.foldLeft(0L)((sum, one) => sum + one.count.toLong)
        if (total * 10L != prepared.outputRows) then refuse(s"selected $total survivors")
      },
      time("probe-selection", threads) {
        FilterChain.selected(
          prepared.probePlan.plan,
          prepared.probePlan.op,
          None,
          preserveAbsence = true,
          Set(0, 1),
        )
      } { selected =>
        if (selected.nonEmpty) then refuse("probe scan produced a selection")
      },
      time("exchange", threads) {
        val build = HashExchange.partitionAllSelected(prepared.assembled, 0, width, threads)
        val probe = HashExchange.partitionAll(prepared.rightBatches, 0, width, threads)
        (build, probe)
      } { case (build, probe) =>
        if (rowCount(build) * 10L != prepared.outputRows) then
          refuse("exchange dropped a survivor")
        if (rowCount(probe) != joinRows.toLong) then refuse("exchange dropped a probe row")
      },
      time("insert", threads) {
        MorselScheduler.map(prepared.buildBuckets, threads)((_, batch) =>
          JoinPhaseOps.insert(batch),
        )
      } { tables =>
        val built = tables.foldLeft(0L)((sum, table) => sum + table.rows.toLong)
        if (built * 10L != prepared.outputRows) then refuse(s"insert built $built rows")
      },
      time("probe-walk", threads) {
        MorselScheduler.map(prepared.parts, threads) { (_, part) =>
          JoinPhaseOps.walk(part.table, part.probe)
        }
      } { counts =>
        val matched = counts.foldLeft(0L)((sum, count) => sum + count.toLong)
        if (matched != prepared.outputRows) then refuse(s"walk counted $matched")
      },
      time("append", threads) {
        MorselScheduler.map(prepared.parts.zip(prepared.reserves), threads) { (_, item) =>
          val (part, reserved) = item
          JoinPhaseOps.append(part.table, part.build, part.probe, math.max(reserved, 1))
        }
      } { batches =>
        if (ColumnReduce.count(batches) != prepared.outputRows) then refuse("append row count")
        if (foldOf(batches) != prepared.fold) then refuse("append checksum")
      },
      time("append-grouped", threads) {
        MorselScheduler.map(prepared.parts.zip(prepared.reserves), threads) { (_, item) =>
          val (part, reserved) = item
          JoinPhaseOps.appendGrouped(part.table, part.build, part.probe, math.max(reserved, 1))
        }
      } { batches =>
        if (ColumnReduce.count(batches) != prepared.outputRows) then refuse("grouped row count")
        if (foldOf(batches) != prepared.fold) then refuse("grouped checksum")
      },
      time("kernel", threads) {
        MorselScheduler.map(prepared.parts, threads) { (_, part) =>
          JoinPhaseOps.kernel(part.build, part.probe)
        }
      } { batches =>
        if (ColumnReduce.count(batches) != prepared.outputRows) then refuse("kernel row count")
        if (foldOf(batches) != prepared.fold) then refuse("kernel checksum")
      },
      time("count", threads)(ColumnReduce.count(prepared.joined)) { counted =>
        if (counted != prepared.outputRows) then refuse(s"count $counted")
      },
      time("action", threads)(prepared.action()) { counted =>
        if (counted != prepared.outputRows) then refuse(s"action $counted")
      },
    )

  private def time[A](phase: String, threads: Int)(body: => A)(check: A => Unit): Reading =
    System.err.println(s"sample $phase threads=$threads")
    val (p50, steadyMs, verdict) = sample(body)(check)
    val reading = Reading(phase, threads, p50, steadyMs, verdict, None, None)
    println(line(reading))
    reading

  private def sample[A](body: => A)(check: A => Unit): (Double, Double, String) =
    var round = 0
    while (round < warmup) {
      check(body)
      round += 1
    }
    val samples = new Array[Double](rounds)
    round = 0
    while (round < rounds) {
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        val started = System.nanoTime()
        val produced = body
        val elapsed = (System.nanoTime() - started) / 1_000_000.0
        check(produced)
        samples(round) = elapsed
      finally ColumnMetrics.restore(previous)
      round += 1
    }
    val steady = steadyOf(samples)
    (nearest(samples), steady.ms, steady.verdict)

  /** Last-10 upper median. Same verdicts as [[CrossEngineBench.steadyOf]]. */
  private def steadyOf(samples: Array[Double]): Steady =
    val n = samples.length
    val tailN = math.min(10, n)
    val headN = math.min(10, n)
    val tail = medianRange(samples, n - tailN, n)
    val head = medianRange(samples, 0, headN)
    if (n < 20) then Steady(tail, head, "short")
    else
      val mid = n - tailN / 2
      val left = medianRange(samples, n - tailN, mid)
      val right = medianRange(samples, mid, n)
      val low = math.min(left, right)
      val high = math.max(left, right)
      val verdict =
        if (high > low * 1.10) then "machine"
        else if (head > tail * 1.10) then "compilation"
        else "ok"
      Steady(tail, head, verdict)

  private final case class Steady(ms: Double, head: Double, verdict: String)

  private def medianRange(samples: Array[Double], from: Int, until: Int): Double =
    val copy = Arrays.copyOfRange(samples, from, until)
    Arrays.sort(copy)
    copy(copy.length / 2)

  private def encode(ownedParts: Vector[Owned]): Vector[ColumnBatch] =
    ownedParts.map(part => BatchCodec.copiedIntPairs(part.keys, part.values))

  private def selectMorsels(morsels: Vector[Morsel], threads: Int): Vector[Selected] =
    MorselScheduler.map(morsels, threads) { (_, morsel) =>
      Selection.applySelected(morsel.batch, morsel.start, morsel.length, steps)
    }

  private def groupHits(
      batches: Vector[ColumnBatch],
      flat: Vector[Selected],
  ): Vector[Vector[Selected]] =
    val sizes = groupSizes(batches)
    val seed: (Vector[Selected], Vector[Vector[Selected]]) = (flat, Vector.empty)
    val (left, groups) = sizes.foldLeft(seed) { (state, count) =>
      val (rest, acc) = state
      val (group, tail) = rest.splitAt(count)
      if (group.length != count) then
        refuse(s"selection group has ${group.length}, expected $count")
      (tail, acc :+ group)
    }
    if (left.nonEmpty) then refuse(s"${left.length} selection morsels were not grouped")
    groups

  private def groupSizes(batches: Vector[ColumnBatch]): Vector[Int] =
    batches.map(batch => Morsels.of(batch, batchSize).length)

  private def assemble(batch: ColumnBatch, morsels: Vector[Selected]): Selected =
    val copied = new Array[Int](batch.length)
    val total = morsels.foldLeft(0) { (written, morsel) =>
      if (written + morsel.count > copied.length) then
        refuse(s"selection morsels exceed capacity ${copied.length}")
      System.arraycopy(morsel.rows, 0, copied, written, morsel.count)
      written + morsel.count
    }
    Selected(batch, copied, total)

  private def scanOf(plan: Plan[_]): PlanRef =
    plan match
      case source: Plan.Source[_] =>
        PlanRef(source, PhysicalOp.Scan(PhysicalOp.Kind.Pairs, source.partitions))
      case other => refuse(s"probe plan was ${other.getClass.getName}")

  private def owned(keys: Array[Int], values: Array[Int]): Vector[Owned] =
    val count = keys.length
    val base = count / sourcePartitions
    val extra = count % sourcePartitions
    Vector.tabulate(sourcePartitions) { index =>
      val start = index * base + math.min(index, extra)
      val end = start + base + (if (index < extra) then 1 else 0)
      Owned(Arrays.copyOfRange(keys, start, end), Arrays.copyOfRange(values, start, end))
    }

  private def lcgValues(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = lcgSeed
    while (index < count) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    generated

  private def foldOf(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((sum, batch) => sum + JoinPhaseOps.fold(batch))

  private def rowCount(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((sum, batch) => sum + batch.length.toLong)

  private def nearest(samples: Array[Double]): Double =
    Arrays.sort(samples)
    val count = samples.length
    val rank = (50 * count + 99) / 100
    samples(math.max(1, math.min(count, rank)) - 1)

  private def configure(saved: ScarletConf, threads: Int): Unit =
    ScarletConf.set(
      saved.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = width,
        spillEnabled = true,
        threadPoolSize = threads,
      ),
    )

  private def preamble(prepared: Prepared): Unit =
    val runtime = ManagementFactory.getRuntimeMXBean
    System.err.println(
      s"join-phase rows=$rows output=${prepared.outputRows} fold=${prepared.fold} " +
        s"batch=$batchSize width=$width warmup=$warmup rounds=$rounds spill=true " +
        s"threads=${requestedThreads.getOrElse(width)} onlyAction=$actionOnly " +
        s"${affinity()} p50=nearest-rank steady=last-10 " +
        s"java=${runtime.getVmName} ${runtime.getVmVersion}",
    )

  private def table(readings: Vector[Reading]): String =
    val header = Vector(
      "",
      "| phase | threads | p50 ms | steady ms | verdict | submits | visits |",
      "| --- | ---: | ---: | ---: | --- | ---: | ---: |",
    )
    val body = readings.map { reading =>
      val submits = reading.submits.fold("")(_.toString)
      val visits = reading.visits.fold("")(_.toString)
      s"| ${reading.phase} | ${reading.threads} | ${millis(reading.p50Ms)} | " +
        s"${millis(reading.steadyMs)} | ${reading.verdict} | $submits | $visits |"
    }
    (header ++ body).mkString("\n")

  private def line(reading: Reading): String =
    "{" +
      """"schema":"join-phase-1",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""rows":$rows,""" +
      s""""p50_ms":${millis(reading.p50Ms)},""" +
      s""""steady_ms":${millis(reading.steadyMs)},""" +
      s""""steady":"${reading.verdict}"}"""

  private def millis(value: Double): String =
    String.format(Locale.ENGLISH, "%.3f", value)

  private def parallelLine(reading: Reading): String =
    val submits = reading.submits.fold("null")(_.toString)
    val visits = reading.visits.fold("null")(_.toString)
    "{" +
      """"schema":"join-parallel-1",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""rows":$rows,""" +
      s""""p50_ms":${millis(reading.p50Ms)},""" +
      s""""steady_ms":${millis(reading.steadyMs)},""" +
      s""""steady":"${reading.verdict}",""" +
      s""""submits":$submits,""" +
      s""""visits":$visits}"""

  private def affinity(): String =
    val lines = java.nio.file.Files.readAllLines(java.nio.file.Path.of("/proc/self/status"))
    var found = "Cpus_allowed_list: unknown"
    val iterator = lines.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      if (row.startsWith("Cpus_allowed_list")) then found = row
    }
    found

  private def readOptionalInt(name: String): Option[Int] =
    if (sys.env.contains(name)) then Some(readInt(name, 1)) else None

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) if value > 0 => value
          case _ =>
            refuse(s"$name must be a positive integer, found $text")
