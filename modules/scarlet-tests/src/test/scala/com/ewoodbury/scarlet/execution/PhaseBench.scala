package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.{Arrays, Locale}

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntArrays, IntPairArrays, Plan, ScarletConf}

/**
 * Phase split for the two actions A1 names. Not a test, and not part of `make test`. No kernel
 * change.
 *
 * The input is [[QueryBench]]'s: 1e6 rows, two source partitions, batch 8192. `int-narrow` is the
 * LCG ints, `filter(_ > 0).map(_ * 2)`. `filter-countbykey` is keys `row % 1024` and the same LCG
 * values, then `key % 3 != 0` and `value >= 0`. Spill stays off.
 *
 * Each line is one phase. The arrays already exist and the timer wraps that phase only. Phases
 * are not subtracted from each other or from an action total. p50 is nearest-rank
 * `ceil(p / 100 * n)` over 40 rounds after 5 warmups. A [[CountingSink]] is installed around the
 * timed call the way [[QueryBench.sample]] installs it, so a scheduled phase pays the same
 * per-morsel visit.
 *
 *   - `lower` is [[ColumnarPlanner.lower]] on that action's logical plan.
 *   - `encode` is the owned copy the action pays on the caller, before any worker.
 *     `int-narrow` is [[BatchCodec.copiedInts]] per partition. `filter-countbykey` is
 *     [[BatchCodec.copiedIntPairs]] per partition.
 *   - `selection` is [[MorselScheduler.map]] of [[Selection.applySelected]] over morsels of the
 *     filtered pair batches. That is the parallel half of `FilterChain.selectBatches`. It does
 *     not encode and it does not assemble.
 *   - `assemble` is the caller-side id copy in `FilterChain.assemble`: one `Array[Int]` of the
 *     batch length and `System.arraycopy` of each morsel's ids.
 *   - `dispatch` is one [[MorselScheduler.map]] at 4 threads whose body reads the morsel length.
 *   - `count` is [[MorselScheduler.map]] of [[Selection.count]] on the same two-filter stack.
 *   - `int32` is [[HashCount.int32]] on dense key morsels of the unfiltered pairs. The slice copy
 *     is outside the timer. `int32Selected` is [[HashCount.int32Selected]] on the assembled
 *     survivor index. Neither call merges, exchanges, or finishes.
 *
 * A row count that is not the oracle, an empty vector, or a scheduler that does not visit every
 * morsel fails the process before that round is recorded.
 *
 * {{{
 * sbt -J-Xmx2g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.PhaseBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object PhaseBench:

  private val schemaName = "phase-bench-1"
  private val rows = 1000000
  private val groups = 1024
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val shuffleWidth = 4
  private val warmup = 5
  private val rounds = 40
  private val threadCounts: Vector[Int] = Vector(1, 4)
  private val lcgSeed = 0x12345678
  private val narrowShape = "Pipeline2(Scan)"
  private val countShape = "CountByKey(Exchange(FilterValue(FilterKey(Scan))))"
  private val filterShape = "FilterValue(FilterKey(Scan))"

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  private final case class OwnedPairs(keys: Array[Int], values: Array[Int])

  private final case class Span(selected: Selected, from: Int, until: Int)

  private final case class Reading(
      query: String,
      phase: String,
      threads: Int,
      p50Ms: Double,
  )

  def main(args: Array[String]): Unit =
    val saved = ScarletConf.get
    var status = 0
    try
      configure(saved, threads = 1)
      run(saved)
    catch case _: HarnessRefusal => status = 1
    finally ScarletConf.set(saved)
    if (status != 0) then sys.exit(status)

  private def run(saved: ScarletConf): Unit =
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    val ints = DistCollection.ofInts(values, sourcePartitions)
    val narrow = ints.filter(_ > 0).map(_ * 2)
    val pairs = DistCollection.ofIntPairs(keys, values, sourcePartitions)
    val filtered = pairs
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val counted = filtered.countByKey
    val narrowParts = ownedInts(ints.plan)
    val pairParts = ownedPairs(pairs.plan)
    val survivors = survivorCount(keys, values)
    val narrowSum = expectNarrow(values)
    configure(saved, threads = 1)
    ColumnarPlanner.trySumInt(narrow.plan) match
      case Some(got) if got == narrowSum => ()
      case Some(got) => refuse(s"int-narrow sum $got != $narrowSum")
      case None => refuse("int-narrow did not sum on the columnar path")
    val steps = filterSteps(filtered.plan)
    val pairBatches = encodePairs(pairParts)
    acceptBatches("setup-encode", pairBatches, rows)
    val morsels = pairBatches.flatMap(batch => Morsels.of(batch, batchSize))
    if (morsels.length != morselCount(pairBatches)) then
      refuse(s"morsels ${morsels.length} != formula ${morselCount(pairBatches)}")
    if (morsels.length <= sourcePartitions) then
      refuse(s"only ${morsels.length} morsels for $sourcePartitions partitions")
    val dense = denseMorsels(pairBatches)
    acceptBatches("dense", dense, rows)
    val prepared = group(batchSizes(pairBatches), selectMorsels(morsels, steps, threads = 1))
    acceptSelected("prepared", prepared.flatten, survivors)
    if (prepared.length != pairBatches.length) then
      refuse(s"prepared groups ${prepared.length} != batches ${pairBatches.length}")
    val assembled = prepared.zip(pairBatches).map { (group, batch) =>
      assemble(batch, group)
    }
    acceptSelected("prepared-assemble", assembled, survivors)
    val spans = spansOf(assembled)
    acceptSpans(spans, survivors)
    preamble(survivors, morsels.length, dense.length, spans.length, narrowSum)
    val readings = Vector(
      timeLower(saved, "int-narrow", narrow.plan, narrowShape),
      timeLower(saved, "filter-countbykey", counted.plan, countShape),
      timeEncode(saved, "int-narrow", narrowParts.length, rows)(encodeInts(narrowParts)),
      timeEncode(saved, "filter-countbykey", pairParts.length, rows)(encodePairs(pairParts)),
      timeSelected(saved, "selection", morsels, steps, survivors),
      timeAssemble(saved, prepared, pairBatches, survivors),
      timeDispatch(saved, morsels),
      timeCount(saved, morsels, steps, survivors),
      timeInt32(saved, dense, rows),
      timeSelectedHash(saved, spans, survivors),
    ).flatten
    System.err.println(table(readings))

  private def timeLower(
      saved: ScarletConf,
      query: String,
      plan: Plan[_],
      shape: String,
  ): Vector[Reading] =
    Vector(record(saved, query, "lower", threads = 1)(lower(plan, shape))(acceptLower(shape)))

  private def timeEncode(
      saved: ScarletConf,
      query: String,
      partitions: Int,
      expectedRows: Int,
  )(encode: => Vector[ColumnBatch]): Vector[Reading] =
    Vector(
      record(saved, query, "encode", threads = 1)(encode) { batches =>
        acceptBatches(query + " encode", batches, expectedRows)
        if (batches.length != partitions) then
          refuse(s"$query encode produced ${batches.length} batches, expected $partitions")
        if (ColumnMetrics.primitiveEncodeCount != partitions.toLong) then
          refuse(
            s"$query encode notes ${ColumnMetrics.primitiveEncodeCount}, expected $partitions",
          )
      },
    )

  private def timeSelected(
      saved: ScarletConf,
      phase: String,
      morsels: Vector[Morsel],
      steps: List[Selection.Step],
      survivors: Long,
  ): Vector[Reading] =
    threadCounts.map { threads =>
      record(saved, "filter-countbykey", phase, threads) {
        selectMorsels(morsels, steps, threads)
      } { selected =>
        acceptSelected(phase, selected, survivors)
        acceptVisits(phase, morsels.length)
      }
    }

  private def timeAssemble(
      saved: ScarletConf,
      groups: Vector[Vector[Selected]],
      batches: Vector[ColumnBatch],
      survivors: Long,
  ): Vector[Reading] =
    Vector(
      record(saved, "filter-countbykey", "assemble", threads = 1) {
        groups.zip(batches).map { (group, batch) =>
          assemble(batch, group)
        }
      } { selected =>
        acceptSelected("assemble", selected, survivors)
      },
    )

  private def timeDispatch(saved: ScarletConf, morsels: Vector[Morsel]): Vector[Reading] =
    Vector(
      record(saved, "filter-countbykey", "dispatch", threads = 4) {
        MorselScheduler.map(morsels, 4)((_, morsel) => morsel.length.toLong)
      } { lengths =>
        val total = lengths.foldLeft(0L)(_ + _)
        if (lengths.length != morsels.length) then
          refuse(s"dispatch returned ${lengths.length} lengths, expected ${morsels.length}")
        if (total != rows.toLong) then refuse(s"dispatch length sum $total != $rows")
        acceptVisits("dispatch", morsels.length)
      },
    )

  private def timeCount(
      saved: ScarletConf,
      morsels: Vector[Morsel],
      steps: List[Selection.Step],
      survivors: Long,
  ): Vector[Reading] =
    threadCounts.map { threads =>
      record(saved, "filter-countbykey", "count", threads) {
        MorselScheduler.map(morsels, threads) { (_, morsel) =>
          Selection.count(morsel.batch, morsel.start, morsel.length, steps)
        }
      } { partials =>
        val total = partials.foldLeft(0L)(_ + _)
        if (partials.length != morsels.length) then
          refuse(s"count returned ${partials.length} partials, expected ${morsels.length}")
        if (total != survivors) then refuse(s"count $total != filtered rows $survivors")
        acceptVisits("count", morsels.length)
      }
    }

  private def timeInt32(
      saved: ScarletConf,
      dense: Vector[ColumnBatch],
      expectedRows: Int,
  ): Vector[Reading] =
    threadCounts.map { threads =>
      record(saved, "countbykey", "int32", threads) {
        MorselScheduler.map(dense, threads)((_, batch) => HashCount.int32(batch))
      } { partials =>
        acceptCounts("int32", partials, dense.length, expectedRows.toLong)
        acceptVisits("int32", dense.length)
      }
    }

  private def timeSelectedHash(
      saved: ScarletConf,
      spans: Vector[Span],
      survivors: Long,
  ): Vector[Reading] =
    threadCounts.map { threads =>
      record(saved, "filter-countbykey", "int32Selected", threads) {
        MorselScheduler.map(spans, threads) { (_, span) =>
          HashCount.int32Selected(span.selected, span.from, span.until)
        }
      } { partials =>
        acceptCounts("int32Selected", partials, spans.length, survivors)
        acceptVisits("int32Selected", spans.length)
      }
    }

  private def record[A](
      saved: ScarletConf,
      query: String,
      phase: String,
      threads: Int,
  )(action: => A)(check: A => Unit): Reading =
    configure(saved, threads)
    System.err.println(s"sample $query $phase threads=$threads")
    val p50 = sample(action)(check)
    val reading = Reading(query, phase, threads, p50)
    println(line(reading))
    reading

  /**
   * The check runs after the timer stops and before the sample is stored. Warm rounds are
   * checked and discarded. The sink stays installed through the check, then the previous sink
   * is restored. [[QueryBench.sample]] installs [[CountingSink]] the same way, outside the
   * timer.
   */
  private def sample[A](action: => A)(check: A => Unit): Double =
    var round = 0
    while (round < warmup) do
      val elapsed = observeTimed(action)(check)
      if (elapsed < 0.0) then refuse(s"negative warmup sample $elapsed")
      round += 1
    val samples = new Array[Double](rounds)
    round = 0
    while (round < rounds) do
      samples(round) = observeTimed(action)(check)
      round += 1
    nearestP50(samples)

  private def observeTimed[A](action: => A)(check: A => Unit): Double =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      val started = System.nanoTime()
      val produced = action
      val elapsed = (System.nanoTime() - started) / 1_000_000.0
      check(produced)
      elapsed
    finally ColumnMetrics.restore(previous)

  /** 1-based rank `ceil(50 / 100 * n)`, no interpolation. Forty rounds use rank 20. */
  private def nearestP50(samples: Array[Double]): Double =
    Arrays.sort(samples)
    val count = samples.length
    val rank = (50 * count + 99) / 100
    val index = math.max(1, math.min(count, rank)) - 1
    samples(index)

  private def lower(plan: Plan[_], shape: String): String =
    ColumnarPlanner.lower(plan) match
      case Some(op) => op.shape
      case None => refuse(s"lower refused, expected $shape")

  private def acceptLower(shape: String)(got: String): Unit =
    if (got != shape) then refuse(s"lower shape $got != $shape")

  private def selectMorsels(
      morsels: Vector[Morsel],
      steps: List[Selection.Step],
      threads: Int,
  ): Vector[Selected] =
    MorselScheduler.map(morsels, threads) { (_, morsel) =>
      Selection.applySelected(morsel.batch, morsel.start, morsel.length, steps)
    }

  /** Caller-side id copy. Same statements as `FilterChain.assemble`. */
  private def assemble(batch: ColumnBatch, morsels: Vector[Selected]): Selected =
    val copied = new Array[Int](batch.length)
    val total = morsels.foldLeft(0) { (written, morsel) =>
      if (written + morsel.count > copied.length) then
        refuse(s"selection morsels exceed capacity ${copied.length}")
      System.arraycopy(morsel.rows, 0, copied, written, morsel.count)
      written + morsel.count
    }
    Selected(batch, copied, total)

  private def encodeInts(parts: Vector[Array[Int]]): Vector[ColumnBatch] =
    parts.map(values => BatchCodec.copiedInts(values))

  private def encodePairs(parts: Vector[OwnedPairs]): Vector[ColumnBatch] =
    parts.map(pairs => BatchCodec.copiedIntPairs(pairs.keys, pairs.values))

  private def denseMorsels(batches: Vector[ColumnBatch]): Vector[ColumnBatch] =
    batches
      .map(batch => ColumnBatches.takeColumn(batch, 0))
      .flatMap(batch => ColumnBatches.slice(batch, batchSize))

  private def spansOf(selected: Vector[Selected]): Vector[Span] =
    selected.flatMap { one =>
      ColumnBatches.cuts(one.count, batchSize).map { case (start, length) =>
        Span(one, start, start + length)
      }
    }

  private def filterSteps(plan: Plan[_]): List[Selection.Step] =
    val op = ColumnarPlanner.lower(plan) match
      case Some(lowered) if lowered.shape == filterShape => lowered
      case Some(lowered) => refuse(s"filter shape ${lowered.shape} != $filterShape")
      case None => refuse("filter plan did not lower")
    val compiled = PredicatePlan.compile(plan, op).getOrElse {
      refuse("filter stack did not compile")
    }
    val ordinals = ColumnUse.ordinals(plan, op)
    if (ordinals != Set(0, 1)) then refuse(s"filter ordinals $ordinals, expected both columns")
    val steps = PredicatePlan
      .selectionSteps(PredicatePlan.encoded(compiled, ordinals))
      .getOrElse(refuse("filter stack has no selection steps"))
    if (steps.length != 2) then refuse(s"expected 2 filter steps, got ${steps.length}")
    steps.toList

  private def ownedInts(plan: Plan[_]): Vector[Array[Int]] =
    plan match
      case Plan.Source(partitions, _) =>
        if (partitions.length != sourcePartitions) then
          refuse(s"int source has ${partitions.length} partitions, expected $sourcePartitions")
        partitions.toVector.map { partition =>
          partition.data match
            case owned: IntArrays =>
              if (owned.values.length <= 0) then refuse("empty int partition")
              owned.values
            case _ => refuse("int-narrow partition was not an owned int array")
        }
      case _ => refuse("int-narrow source was not a scan")

  private def ownedPairs(plan: Plan[_]): Vector[OwnedPairs] =
    plan match
      case Plan.Source(partitions, _) =>
        if (partitions.length != sourcePartitions) then
          refuse(s"pair source has ${partitions.length} partitions, expected $sourcePartitions")
        partitions.toVector.map { partition =>
          partition.data match
            case owned: IntPairArrays =>
              if (owned.keys.length <= 0) then refuse("empty pair partition")
              OwnedPairs(owned.keys, owned.values)
            case _ => refuse("pair partition was not an owned int-pair array")
        }
      case _ => refuse("pair source was not a scan")

  private def acceptBatches(phase: String, batches: Vector[ColumnBatch], expectedRows: Int): Unit =
    if (batches.isEmpty) then refuse(s"$phase returned an empty vector")
    val total = batches.foldLeft(0L)((sum, batch) => sum + batch.length.toLong)
    if (total != expectedRows.toLong) then refuse(s"$phase rows $total != $expectedRows")
    if (batches.exists(_.length <= 0)) then refuse(s"$phase has an empty batch")

  private def acceptSelected(phase: String, selected: Vector[Selected], survivors: Long): Unit =
    if (selected.isEmpty) then refuse(s"$phase returned an empty selection")
    val total = selected.foldLeft(0L)((sum, one) => sum + one.count.toLong)
    if (total != survivors) then refuse(s"$phase survivors $total != $survivors")
    if (selected.forall(_.count <= 0)) then refuse(s"$phase kept no row")

  private def acceptCounts(
      phase: String,
      batches: Vector[ColumnBatch],
      expectedBatches: Int,
      expectedRows: Long,
  ): Unit =
    if (batches.length != expectedBatches) then
      refuse(s"$phase returned ${batches.length} batches, expected $expectedBatches")
    if (batches.exists(_.length <= 0)) then refuse(s"$phase produced an empty partial")
    val total = sumCounts(batches)
    if (total != expectedRows) then refuse(s"$phase counted $total rows, expected $expectedRows")

  private def acceptSpans(spans: Vector[Span], survivors: Long): Unit =
    if (spans.isEmpty) then refuse("int32Selected has no span")
    val total = spans.foldLeft(0L)((sum, span) => sum + (span.until - span.from).toLong)
    if (total != survivors) then refuse(s"selection spans cover $total rows, expected $survivors")
    if (spans.exists(span => span.until <= span.from)) then refuse("selection span is empty")

  private def acceptVisits(phase: String, expected: Int): Unit =
    val visits = ColumnMetrics.visitCount
    if (visits != expected.toLong) then refuse(s"$phase visited $visits morsels, expected $expected")

  private def sumCounts(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((total, batch) => total + countSum(batch))

  private def countSum(batch: ColumnBatch): Long =
    batch.columns.lift(1) match
      case Some(column: Int32Column) =>
        val values = column.values
        val limit = batch.length
        var row = 0
        var sum = 0L
        while (row < limit) do
          sum += values(row).toLong
          row += 1
        sum
      case _ => refuse(s"count batch of ${batch.length} rows has no Int32 count column")

  private def batchSizes(batches: Vector[ColumnBatch]): Vector[Int] =
    batches.map(batch => Morsels.of(batch, batchSize).length)

  private def morselCount(batches: Vector[ColumnBatch]): Int =
    batchSizes(batches).sum

  private def group(sizes: Vector[Int], flat: Vector[Selected]): Vector[Vector[Selected]] =
    val seed: (Vector[Selected], Vector[Vector[Selected]]) =
      (flat, Vector.empty[Vector[Selected]])
    val (left, groups) = sizes.foldLeft(seed) { (state, size) =>
      val (rest, acc) = state
      if (size <= 0) then refuse(s"morsel group size $size")
      val (grouped, tail) = rest.splitAt(size)
      if (grouped.length != size) then
        refuse(s"morsel group has ${grouped.length} selections, expected $size")
      (tail, acc :+ grouped)
    }
    if (left.nonEmpty) then refuse(s"${left.length} selections were not grouped")
    groups

  /**
   * Keys kept by `key % 3 != 0` over full cycles of `groups`, plus the tail. The value filter is
   * not in this count. Survivors are the same walk with `value >= 0`, and they have to be a
   * non-empty proper subset of the key count: the LCG takes negative values, so a missing value
   * filter would keep every key-filter row.
   */
  private def survivorCount(keys: Array[Int], values: Array[Int]): Long =
    val keyKept = keptByFormula(keys.length)
    var index = 0
    var keptKeys = 0
    var kept = 0
    while (index < keys.length) do
      val key = keys(index)
      if (key % groups != index % groups) then refuse(s"key at $index is $key, expected row % $groups")
      if (key % 3 != 0) then
        keptKeys += 1
        if (values(index) >= 0) then kept += 1
      index += 1
    if (keptKeys.toLong != keyKept) then refuse(s"key filter kept $keptKeys rows, formula $keyKept")
    if (kept <= 0 || kept.toLong >= keyKept) then
      refuse(s"survivors $kept are not a non-empty subset of key rows $keyKept")
    kept.toLong

  /** Full cycles of `groups`, then the tail keys `0 until rows % groups`. */
  private def keptByFormula(rowCount: Int): Long =
    val full = rowCount / groups
    val tail = rowCount % groups
    full.toLong * keptIn(groups).toLong + keptIn(tail).toLong

  private def keptIn(span: Int): Int =
    var key = 0
    var kept = 0
    while (key < span) do
      if (key % 3 != 0) then kept += 1
      key += 1
    kept

  /** Same LCG as [[QueryBench]] `generate`: seed `0x12345678`, modulus 2000001, centered at 0. */
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

  /** `int-narrow`: keep `value > 0`, then add `value * 2`. */
  private def expectNarrow(values: Array[Int]): Long =
    var index = 0
    var sum = 0L
    while (index < values.length) do
      val value = values(index)
      if (value > 0) then sum += (value * 2).toLong
      index += 1
    if (sum == 0L) then refuse("int-narrow sum is zero")
    sum

  private def configure(saved: ScarletConf, threads: Int): Unit =
    ScarletConf.set(
      saved.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shuffleWidth,
        spillEnabled = false,
        threadPoolSize = threads,
      ),
    )

  private def preamble(
      survivors: Long,
      morsels: Int,
      dense: Int,
      spans: Int,
      narrowSum: Long,
  ): Unit =
    val runtime = ManagementFactory.getRuntimeMXBean
    System.err.println(
      s"phase-bench rows=$rows survivors=$survivors morsels=$morsels dense=$dense " +
        s"spans=$spans narrow_sum=$narrowSum batch=$batchSize partitions=$sourcePartitions " +
        s"warmup=$warmup rounds=$rounds threads=${threadCounts.mkString(",")} spill=false " +
        s"p50=nearest-rank java=${runtime.getVmName} ${runtime.getVmVersion}",
    )

  private def table(readings: Vector[Reading]): String =
    val header = Vector(
      "",
      "| query | phase | threads | rows | p50 ms |",
      "| --- | --- | ---: | ---: | ---: |",
    )
    val body = readings.map { reading =>
      s"| ${reading.query} | ${reading.phase} | ${reading.threads} | $rows | ${millis(reading.p50Ms)} |"
    }
    (header ++ body).mkString("\n")

  private def line(reading: Reading): String =
    "{" +
      s""""schema":"$schemaName",""" +
      s""""query":"${reading.query}",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""rows":$rows,""" +
      s""""warmup":$warmup,""" +
      s""""rounds":$rounds,""" +
      s""""p50_ms":${millis(reading.p50Ms)}}"""

  private def millis(value: Double): String =
    String.format(Locale.ENGLISH, "%.3f", value)

  private def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)
