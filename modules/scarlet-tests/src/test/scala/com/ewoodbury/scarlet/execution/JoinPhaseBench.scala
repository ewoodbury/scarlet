package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.{Arrays, Locale}

import scala.util.control.NonFatal

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
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
 * Each line is its own p50, nearest-rank `ceil(50/100*n)` over 40 rounds after 5 warmups. Phases
 * are not subtracted. Four threads run before one thread, so the morsel pool is sized at 4.
 *
 * {{{
 * sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.JoinPhaseBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object JoinPhaseBench:

  private val rows = 1000000
  private val groups = 1024
  private val joinRows = 10 * groups
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val width = 4
  private val warmup = 5
  private val rounds = 40
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

  private final case class Reading(phase: String, threads: Int, p50Ms: Double)

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val schedulers = Map(1 -> new LocalTaskScheduler(1), width -> new LocalTaskScheduler(width))
    var status = 0
    try
      configure(savedConf, width)
      ScarletRuntime.set(savedRuntime.copy(scheduler = schedulers(width)))
      val prepared = prepare()
      accept(prepared)
      preamble(prepared)
      val four = timeAll(savedConf, savedRuntime, schedulers, prepared, width)
      val one = timeAll(savedConf, savedRuntime, schedulers, prepared, 1)
      System.err.println(table(four ++ one))
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
    if (
      outputRows != ColumnReduce.count(written) || outputRows != ColumnReduce.count(grouped)
    ) then refuse("writer row count drifted from the kernel")
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
      () => {
        ScarletRuntime.get.shuffle.clear()
        joined.count()
      },
      probePlan,
    )

  private def accept(prepared: Prepared): Unit =
    if (prepared.outputRows < 3000000L || prepared.outputRows > 3600000L) then
      refuse(s"output rows ${prepared.outputRows} are outside 3.0e6 until 3.6e6")
    val selected = prepared.assembled.foldLeft(0L)((sum, one) => sum + one.count.toLong)
    if (selected * 10L != prepared.outputRows) then
      refuse(s"survivors $selected * 10 != ${prepared.outputRows}")
    if (!prepared.parts.forall(part => JoinPhaseOps.liveValues(part.build))) then
      refuse("build bucket has a null")
    if (!prepared.parts.forall(part => JoinPhaseOps.liveValues(part.probe))) then
      refuse("probe bucket has a null")
    val counted = prepared.action()
    if (counted != prepared.outputRows) then
      refuse(s"action count $counted != ${prepared.outputRows}")

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
        if (rowCount(build) * 10L != prepared.outputRows) then refuse("exchange dropped a survivor")
        if (rowCount(probe) != joinRows.toLong) then refuse("exchange dropped a probe row")
      },
      time("insert", threads) {
        MorselScheduler.map(prepared.buildBuckets, threads)((_, batch) => JoinPhaseOps.insert(batch))
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
    val p50 = sample(body)(check)
    val reading = Reading(phase, threads, p50)
    println(line(reading))
    reading

  private def sample[A](body: => A)(check: A => Unit): Double =
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
    nearest(samples)

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
      if (group.length != count) then refuse(s"selection group has ${group.length}, expected $count")
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
        s"p50=nearest-rank java=${runtime.getVmName} ${runtime.getVmVersion}",
    )

  private def table(readings: Vector[Reading]): String =
    val header = Vector(
      "",
      "| phase | threads | p50 ms |",
      "| --- | ---: | ---: |",
    )
    val body = readings.map { reading =>
      s"| ${reading.phase} | ${reading.threads} | ${millis(reading.p50Ms)} |"
    }
    (header ++ body).mkString("\n")

  private def line(reading: Reading): String =
    "{" +
      """"schema":"join-phase-1",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""rows":$rows,""" +
      s""""p50_ms":${millis(reading.p50Ms)}}"""

  private def millis(value: Double): String =
    String.format(Locale.ENGLISH, "%.3f", value)
