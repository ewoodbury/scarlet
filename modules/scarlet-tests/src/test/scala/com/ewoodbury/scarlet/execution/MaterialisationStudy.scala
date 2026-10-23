package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntArrays, IntPairArrays, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Measures the columnar materialisation boundary. Not a test, and not part of `make test`.
 *
 * F4 is four `map(_ + 1)` calls and `sum`: one element pipeline, one encode, no output column. P5
 * is five maps and `sum`: `Project(Pipeline4(Scan))`, so the pipeline writes a column, the morsels
 * are concatenated, and the sum slices that column again. RAW is a hand-written loop of four adds
 * over the generator array. It does not call the planner. GROUPED is `groupedSum` at 1e7 rows and
 * 1024 groups. Its stage rows replay the public calls `tryGroupedSum` makes. They are not timers
 * inside that call.
 *
 * The oracle is computed once. Each sample's returned long is compared with it outside the timed
 * region. `alloc_bytes` on query and stage rows is a sampled lower bound: a probe reads live
 * threads while the action runs, because a morsel worker has exited by the time the action
 * returns. A worker that starts and exits between samples is missing. Unrelated live threads are
 * included. The probe's own bytes are left out. At one thread, `MorselScheduler` runs the morsel
 * on the caller, so that caller is still alive at the final read. Caller probes use
 * `getThreadAllocatedBytes` on the calling thread only. They are separate calls, not a split of a
 * query total. `ColumnMetrics` is armed during a timed sample and reports batches, primitive
 * encodes, and visits. Arm and disarm sit outside the timer.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.MaterialisationStudy"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object MaterialisationStudy:

  private val warmup = 5
  private val measure = 11
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val shuffleWidth = 4
  private val elementRows = 1000000
  private val groupedRows = 10000000
  private val groups = 1024
  private val threadCounts = Vector(1, 4)

  private val partialSum: ColumnBatch => ColumnBatch =
    morsel => HashAggregate.sumInt32(morsel, keyOrdinal = 0, valueOrdinal = 1)

  private val mergeSum: ColumnBatch => ColumnBatch =
    batch => HashAggregate.sumInt64(batch, keyOrdinal = 0, valueOrdinal = 1)

  private val pipeline: ColumnBatch => ColumnBatch =
    morsel => Int32Pipeline.PPPP(morsel, PlusOne, PlusOne, PlusOne, PlusOne)

  private object PlusOne extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value + 1

  private final case class Sample(
      elapsedNs: Long,
      allocBytes: Long,
      batches: Long,
      encodes: Long,
      visits: Long,
  )

  private final case class Row(
      kind: String,
      name: String,
      threads: Int,
      shape: String,
      rowCount: Int,
      p50Ns: Long,
      p95Ns: Long,
      allocBytes: Long,
      allocKind: String,
      batches: Long,
      encodes: Long,
      visits: Long,
      gcMs: Long,
      checksum: Long,
  )

  private final case class Setup(
      values: Array[Int],
      element: DistCollection[Int],
      fused: DistCollection[Int],
      projected: DistCollection[Int],
      grouped: DistCollection[(Int, Int)],
      fusedSum: Long,
      projectedSum: Long,
      groupedSum: Long,
      morsels: Int,
      groupedMorsels: Int,
  )

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    ExecutionModuleInit.initialize()
    val probe = startProbe()
    try execute(probe, savedConf)
    finally
      ColumnMetrics.disarm()
      probe.foreach(_.stop())
      ScarletConf.set(savedConf)
      ScarletRuntime.set(savedRuntime)

  private def execute(probe: Option[AllocProbe], savedConf: ScarletConf): Unit =
    val bean = threadBean
    ScarletConf.set(
      savedConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shuffleWidth,
        threadPoolSize = 1,
      ),
    )
    val setup = prepare()
    println(header(probe.isDefined, bean.isDefined, setup))
    val measured = threadCounts.flatMap { threads =>
      installThreads(threads)
      val floor = new RawFloor(setup.values, threads)
      try
        require(floor.sum() == setup.fusedSum, s"raw floor drifted at $threads threads")
        measureThread(probe, setup, threads, floor)
      finally floor.stop()
    }
    val probes = measureProbes(bean, setup)
    println()
    printTable("Queries", measured.filter(row => sameText(row.kind, "query")))
    printTable("Grouped stages", measured.filter(row => sameText(row.kind, "stage")))
    printTable("Caller probes", probes)
    printThresholds(measured, probes)
    printBytes(measured, probes, setup)

  private def prepare(): Setup =
    val values = generate(elementRows)
    val element = DistCollection.ofInts(values, sourcePartitions)
    val fused = (0 until 4).foldLeft(element)((current, _) => current.map(_ + 1))
    val projected = fused.map(_ + 1)
    val keys = Array.tabulate(groupedRows)(row => row % groups)
    val groupValues = Array.tabulate(groupedRows)(row => row)
    val grouped = DistCollection.ofIntPairs(keys, groupValues, sourcePartitions)
    val fusedSum = expectPlus(values, steps = 4)
    val projectedSum = expectPlus(values, steps = 5)
    val groupedSum = groupedRows.toLong * (groupedRows.toLong - 1L) / 2L
    val fusedShape = shapeOf(fused)
    val projectedShape = shapeOf(projected)
    val groupedShape = ColumnarPlanner.groupedSumPeel(grouped.plan).getOrElse {
      throw new IllegalStateException("grouped sum did not peel")
    }
    require(sameText(fusedShape, "Pipeline4(Scan)"), fusedShape)
    require(sameText(projectedShape, "Project(Pipeline4(Scan))"), projectedShape)
    require(sameText(groupedShape, "GroupedSum(Scan)"), groupedShape)
    val morsels = morselCount(elementRows, sourcePartitions, batchSize)
    val groupedMorsels = morselCount(groupedRows, sourcePartitions, batchSize)
    require(morsels > sourcePartitions, "element partitions are not sliced more than once")
    require(groupedMorsels > sourcePartitions, "grouped partitions are not sliced more than once")
    require(rawRange(values, 0, values.length) == fusedSum, "raw loop disagrees with the oracle")
    Setup(
      values,
      element,
      fused,
      projected,
      grouped,
      fusedSum,
      projectedSum,
      groupedSum,
      morsels,
      groupedMorsels,
    )

  private def measureThread(
      probe: Option[AllocProbe],
      setup: Setup,
      threads: Int,
      floor: RawFloor,
  ): Vector[Row] =
    val queries = Vector(
      benchLong(
        probe,
        s"F4-t$threads",
        "query",
        "F4",
        threads,
        "Pipeline4(Scan)",
        elementRows,
        setup.fusedSum,
        sampledKind,
      )(setup.fused.sum),
      benchLong(
        probe,
        s"P5-t$threads",
        "query",
        "P5",
        threads,
        "Project(Pipeline4(Scan))",
        elementRows,
        setup.projectedSum,
        sampledKind,
      )(setup.projected.sum),
      benchLong(
        probe,
        s"RAW-t$threads",
        "query",
        "RAW",
        threads,
        "hand-written-four-adds",
        elementRows,
        setup.fusedSum,
        sampledKind,
      )(floor.sum()),
      benchLong(
        probe,
        s"GROUPED-t$threads",
        "query",
        "GROUPED",
        threads,
        "GroupedSum(Scan)",
        groupedRows,
        setup.groupedSum,
        sampledKind,
      )(setup.grouped.groupedSum),
    )
    queries ++ benchStages(probe, setup, threads)

  private def benchStages(
      probe: Option[AllocProbe],
      setup: Setup,
      threads: Int,
  ): Vector[Row] =
    val names = Vector(
      ("encode", "copiedIntPairs", groupedRows),
      ("partial", "slice+sumInt32", groupedRows),
      ("merge", "concat+sumInt64", groupedRows),
      ("exchange", "partitionAll", groupedRows),
      ("final", "sumInt64+ColumnReduce", groupedRows),
    )
    System.err.println(s"run stages-t$threads")
    (0 until warmup).foreach { _ =>
      clearShuffle()
      val got = groupedOnce(setup.grouped, threads)
      require(got == setup.groupedSum, s"stages-t$threads warmup $got != ${setup.groupedSum}")
    }
    val elapsed = Array.fill(names.length)(new Array[Long](measure))
    val allocated = Array.fill(names.length)(new Array[Long](measure))
    val batches = Array.fill(names.length)(new Array[Long](measure))
    val encodes = Array.fill(names.length)(new Array[Long](measure))
    val visits = Array.fill(names.length)(new Array[Long](measure))
    val gcBefore = gcMillis
    (0 until measure).foreach { sample =>
      clearShuffle()
      val (encoded, encodeSample) = timeAction(probe)(encodeOwnedPairs(setup.grouped))
      val (split, partialSample) = timeAction(probe) {
        PartialCombine.partialMorsels(encoded, batchSize, threads, partialSum)
      }
      val (merged, mergeSample) = timeAction(probe) {
        PartialCombine.mergeMorsels(split._1, split._2, threads, mergeSum)
      }
      val (buckets, exchangeSample) = timeAction(probe) {
        HashExchange.partitionAll(
          merged,
          keyOrdinal = 0,
          numPartitions = shuffleWidth,
          parallelism = threads,
        )
      }
      val (got, finalSample) = timeAction(probe) {
        val summed = MorselScheduler.map(buckets, threads)((_, bucket) => mergeSum(bucket))
        ColumnReduce.sumInt64(summed, ordinal = 1)
      }
      require(got == setup.groupedSum, s"stages-t$threads $got != ${setup.groupedSum}")
      require(lengthOf(encoded) == groupedRows, "grouped encode dropped rows")
      val taken = Vector(encodeSample, partialSample, mergeSample, exchangeSample, finalSample)
      taken.zipWithIndex.foreach { (stage, index) =>
        assign(
          stage,
          sample,
          elapsed(index),
          allocated(index),
          batches(index),
          encodes(index),
          visits(index),
        )
      }
    }
    val gcAfter = gcMillis
    val gc = gcDelta(gcBefore, gcAfter)
    names.zipWithIndex.map { (described, index) =>
      val (name, shape, rows) = described
      emit(
        rowFrom(
          "stage",
          name,
          threads,
          shape,
          rows,
          setup.groupedSum,
          sampledKind,
          elapsed(index),
          allocated(index),
          batches(index),
          encodes(index),
          visits(index),
          gc,
        ),
      )
    }

  private def groupedOnce(source: DistCollection[(Int, Int)], threads: Int): Long =
    val encoded = encodeOwnedPairs(source)
    val (sizes, partials) = PartialCombine.partialMorsels(encoded, batchSize, threads, partialSum)
    val merged = PartialCombine.mergeMorsels(sizes, partials, threads, mergeSum)
    val buckets = HashExchange.partitionAll(
      merged,
      keyOrdinal = 0,
      numPartitions = shuffleWidth,
      parallelism = threads,
    )
    val summed = MorselScheduler.map(buckets, threads)((_, bucket) => mergeSum(bucket))
    ColumnReduce.sumInt64(summed, ordinal = 1)

  private def measureProbes(bean: Option[ThreadMXBean], setup: Setup): Vector[Row] =
    val owned = ownedInts(setup.element)
    val encoded = owned.map(values => BatchCodec.copiedInts(values))
    val groups = encoded.map(batch => ColumnBatches.slice(batch, batchSize))
    val written = groups.map(group => group.map(pipeline))
    val concatenated = written.map(group => ColumnBatches.concat(group))
    require(lengthOf(encoded) == elementRows, "probe setup encode dropped rows")
    require(lengthOfNested(groups) == elementRows, "probe setup slice dropped rows")
    Vector(
      benchCaller(bean, "probe-encode", "encode", "copiedInts", elementRows, 0L, lengthOf) {
        owned.map(values => BatchCodec.copiedInts(values))
      },
      benchCaller(
        bean,
        "probe-slice",
        "slice",
        "ColumnBatches.slice",
        elementRows,
        0L,
        lengthOfNested,
      ) {
        encoded.map(batch => ColumnBatches.slice(batch, batchSize))
      },
      benchCaller(
        bean,
        "probe-output",
        "output",
        "Int32Pipeline.PPPP",
        elementRows,
        0L,
        lengthOfNested,
      ) {
        groups.map(group => group.map(pipeline))
      },
      benchCaller(
        bean,
        "probe-concat",
        "concat",
        "ColumnBatches.concat",
        elementRows,
        0L,
        lengthOf,
      ) {
        written.map(group => ColumnBatches.concat(group))
      },
      benchCaller(
        bean,
        "probe-reslice",
        "reslice",
        "ColumnBatches.slice",
        elementRows,
        0L,
        lengthOfNested,
      ) {
        concatenated.map(batch => ColumnBatches.slice(batch, batchSize))
      },
    )

  private def benchLong(
      probe: Option[AllocProbe],
      title: String,
      kind: String,
      name: String,
      threads: Int,
      shape: String,
      rowCount: Int,
      checksum: Long,
      allocKind: String,
  )(action: => Long): Row =
    System.err.println(s"run $title")
    (0 until warmup).foreach { _ =>
      clearShuffle()
      val got = action
      require(got == checksum, s"$title warmup $got != $checksum")
    }
    val elapsed = new Array[Long](measure)
    val allocated = new Array[Long](measure)
    val batches = new Array[Long](measure)
    val encodes = new Array[Long](measure)
    val visits = new Array[Long](measure)
    val gcBefore = gcMillis
    (0 until measure).foreach { sample =>
      clearShuffle()
      val (got, taken) = timeAction(probe)(action)
      require(got == checksum, s"$title $got != $checksum")
      assign(taken, sample, elapsed, allocated, batches, encodes, visits)
    }
    val gc = gcDelta(gcBefore, gcMillis)
    emit(
      rowFrom(
        kind,
        name,
        threads,
        shape,
        rowCount,
        checksum,
        allocKind,
        elapsed,
        allocated,
        batches,
        encodes,
        visits,
        gc,
      ),
    )

  private def benchCaller[A](
      bean: Option[ThreadMXBean],
      title: String,
      name: String,
      shape: String,
      rowCount: Int,
      checksum: Long,
      rowsOf: A => Int,
  )(action: => A): Row =
    System.err.println(s"run $title")
    (0 until warmup).foreach { _ =>
      val got = rowsOf(action)
      require(got == rowCount, s"$title warmup rows $got != $rowCount")
    }
    val elapsed = new Array[Long](measure)
    val allocated = new Array[Long](measure)
    val batches = new Array[Long](measure)
    val encodes = new Array[Long](measure)
    val visits = new Array[Long](measure)
    val gcBefore = gcMillis
    (0 until measure).foreach { sample =>
      val (produced, taken) = timeCaller(bean)(action)
      val got = rowsOf(produced)
      require(got == rowCount, s"$title rows $got != $rowCount")
      assign(taken, sample, elapsed, allocated, batches, encodes, visits)
    }
    val gc = gcDelta(gcBefore, gcMillis)
    emit(
      rowFrom(
        "probe",
        name,
        1,
        shape,
        rowCount,
        checksum,
        callerKind,
        elapsed,
        allocated,
        batches,
        encodes,
        visits,
        gc,
      ),
    )

  private def timeAction[A](probe: Option[AllocProbe])(body: => A): (A, Sample) =
    ColumnMetrics.arm()
    try
      val (value, elapsed, bytes) = measure(probe)(body)
      val sample = Sample(
        elapsed,
        bytes,
        ColumnMetrics.batchCount,
        ColumnMetrics.primitiveEncodeCount,
        ColumnMetrics.visitCount,
      )
      (value, sample)
    finally ColumnMetrics.disarm()

  private def timeCaller[A](bean: Option[ThreadMXBean])(body: => A): (A, Sample) =
    ColumnMetrics.arm()
    try
      val (value, elapsed, bytes) = bean match
        case Some(active) => callerAlloc(active)(body)
        case None =>
          val start = System.nanoTime()
          val value = body
          (value, System.nanoTime() - start, -1L)
      val sample = Sample(
        elapsed,
        bytes,
        ColumnMetrics.batchCount,
        ColumnMetrics.primitiveEncodeCount,
        ColumnMetrics.visitCount,
      )
      (value, sample)
    finally ColumnMetrics.disarm()

  private def callerAlloc[A](bean: ThreadMXBean)(body: => A): (A, Long, Long) =
    val id = Thread.currentThread().getId
    val before = bean.getThreadAllocatedBytes(id)
    val start = System.nanoTime()
    val value = body
    val elapsed = System.nanoTime() - start
    val after = bean.getThreadAllocatedBytes(id)
    val bytes = if (before < 0L || after < 0L) then -1L else after - before
    (value, elapsed, bytes)

  private def rowFrom(
      kind: String,
      name: String,
      threads: Int,
      shape: String,
      rowCount: Int,
      checksum: Long,
      allocKind: String,
      elapsed: Array[Long],
      allocated: Array[Long],
      batches: Array[Long],
      encodes: Array[Long],
      visits: Array[Long],
      gcMs: Long,
  ): Row =
    Row(
      kind,
      name,
      threads,
      shape,
      rowCount,
      percentile(elapsed, 50),
      percentile(elapsed, 95),
      mean(allocated),
      allocKind,
      same(batches, s"$kind $name batches"),
      same(encodes, s"$kind $name encodes"),
      same(visits, s"$kind $name visits"),
      gcMs,
      checksum,
    )

  private def assign(
      sample: Sample,
      at: Int,
      elapsed: Array[Long],
      allocated: Array[Long],
      batches: Array[Long],
      encodes: Array[Long],
      visits: Array[Long],
  ): Unit =
    elapsed(at) = sample.elapsedNs
    allocated(at) = sample.allocBytes
    batches(at) = sample.batches
    encodes(at) = sample.encodes
    visits(at) = sample.visits

  private def emit(row: Row): Row =
    println(tsv(row))
    row

  private def printTable(title: String, rows: Vector[Row]): Unit =
    println(title)
    println(
      "| kind | case | threads | shape | p50 | p95 | rows/s | alloc | alloc kind | batches | encodes | visits | gc_ms |",
    )
    println(
      "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |",
    )
    rows.foreach(row => println(markdown(row)))
    println()

  private def printThresholds(measured: Vector[Row], probes: Vector[Row]): Unit =
    println("Thresholds")
    println(
      "Comparisons use p50. The one-third line is one third of P5. The 2× line is twice RAW. " +
        "The 15% line is 15% of P5. Nothing here chooses a redesign.",
    )
    threadCounts.foreach { threads =>
      val fused = findRow(measured, "query", "F4", threads)
      val projected = findRow(measured, "query", "P5", threads)
      val raw = findRow(measured, "query", "RAW", threads)
      val grouped = findRow(measured, "query", "GROUPED", threads)
      val delta = projected.p50Ns - fused.p50Ns
      val third = projected.p50Ns / 3L
      val fifteen = (projected.p50Ns * 15L) / 100L
      println(
        s"threads=$threads F4 ${ms(fused.p50Ns)} ms, P5 ${ms(projected.p50Ns)} ms, " +
          s"RAW ${ms(raw.p50Ns)} ms, GROUPED ${ms(grouped.p50Ns)} ms.",
      )
      println(
        s"threads=$threads P5-F4 is ${ms(delta)} ms (${percent(delta, projected.p50Ns)} of P5, " +
          s"${percent(delta, fused.p50Ns)} of F4). One third of P5 is ${ms(third)} ms, and the " +
          s"delta is ${multiple(delta, third)}× that line. The 15% line is ${ms(fifteen)} ms.",
      )
      println(
        s"threads=$threads F4 is ${multiple(fused.p50Ns, raw.p50Ns)}× RAW. P5 is " +
          s"${multiple(projected.p50Ns, raw.p50Ns)}× RAW. The 2× line is " +
          s"${ms(raw.p50Ns * 2L)} ms. F4 over RAW is the fused path against the four-add loop. " +
          "P5 over F4 is the extra written column, concat, and second slice.",
      )
      println(
        s"threads=$threads GROUPED is ${nsPerRow(grouped.p50Ns, groupedRows)} ns/row at " +
          s"$groupedRows rows. RAW is ${nsPerRow(raw.p50Ns, elementRows)} ns/row at " +
          s"$elementRows rows. RAW is not a grouped kernel, so that pair is not a boundary ratio.",
      )
      val stageNames = Vector("encode", "partial", "merge", "exchange", "final")
      val stages = stageNames.map(name => findRow(measured, "stage", name, threads))
      val described = stages.map(row => s"${row.name} ${ms(row.p50Ns)} ms").mkString(", ")
      val slowest = slowestStage(stages)
      println(
        s"threads=$threads stage p50: $described. Largest stage p50 is ${slowest.name} at " +
          s"${ms(slowest.p50Ns)} ms. Sum of stage p50 is ${ms(stages.map(_.p50Ns).sum)} ms " +
          s"against GROUPED p50 ${ms(grouped.p50Ns)} ms. The stage sum is the replay, and " +
          "GROUPED is `groupedSum`.",
      )
      val stageVisits = stages.map(_.visits).sum
      println(
        s"threads=$threads GROUPED batches=${grouped.batches} encodes=${grouped.encodes} " +
          s"visits=${grouped.visits}. Stage visits sum to $stageVisits. Stage batches sum to " +
          s"${stages.map(_.batches).sum}.",
      )
    }
    val output = findRow(probes, "probe", "output", 1)
    val concat = findRow(probes, "probe", "concat", 1)
    val reslice = findRow(probes, "probe", "reslice", 1)
    val extra = output.allocBytes + concat.allocBytes + reslice.allocBytes
    println(
      s"Caller probes of the P5 extra passes: output ${bytes(output.allocBytes)}, " +
        s"concat ${bytes(concat.allocBytes)}, reslice ${bytes(reslice.allocBytes)}, " +
        s"sum ${bytes(extra)}. These are caller-thread totals, not a split of P5-F4.",
    )
    println()

  private def printBytes(measured: Vector[Row], probes: Vector[Row], setup: Setup): Unit =
    println("Bytes")
    println(
      "Query and stage `alloc_bytes` are sampled lower bounds. Caller probes are the calling " +
        "thread only. A per-query split into encode copy, slice arraycopy, concat append, and " +
        "pipeline output column is not instrumented, so the probes are not subtracted from the " +
        "query totals.",
    )
    val encode = findRow(probes, "probe", "encode", 1)
    val slice = findRow(probes, "probe", "slice", 1)
    val output = findRow(probes, "probe", "output", 1)
    val concat = findRow(probes, "probe", "concat", 1)
    val reslice = findRow(probes, "probe", "reslice", 1)
    println(
      s"Probe batches: encode ${encode.batches}, slice ${slice.batches}, output ${output.batches}, " +
        s"concat ${concat.batches}, reslice ${reslice.batches}. Probe encodes: encode " +
        s"${encode.encodes}, and the other probes ${slice.encodes}, ${output.encodes}, " +
        s"${concat.encodes}, ${reslice.encodes}.",
    )
    threadCounts.foreach { threads =>
      val fused = findRow(measured, "query", "F4", threads)
      val projected = findRow(measured, "query", "P5", threads)
      val raw = findRow(measured, "query", "RAW", threads)
      val grouped = findRow(measured, "query", "GROUPED", threads)
      val fusedAccount = encode.batches + slice.batches
      val projectedAccount =
        encode.batches + slice.batches + output.batches + concat.batches + reslice.batches
      println(
        s"F4 t$threads shape ${fused.shape}. Sampled alloc ${bytes(fused.allocBytes)}. " +
          s"Batches ${fused.batches} against encode+slice probe batches $fusedAccount " +
          s"(${relation(fused.batches, fusedAccount)}). Encodes ${fused.encodes}. Visits " +
          s"${fused.visits} against ${setup.morsels} sliced morsels " +
          s"(${relation(fused.visits, setup.morsels.toLong)}). The sum path folds " +
          "`ElementSum` into the sliced scan and does not write an output column or concat.",
      )
      println(
        s"P5 t$threads shape ${projected.shape}. Sampled alloc ${bytes(projected.allocBytes)}. " +
          s"P5-F4 alloc delta ${signedBytes(projected.allocBytes - fused.allocBytes)}. Batches " +
          s"${projected.batches} against encode+slice+output+concat+reslice probe batches " +
          s"$projectedAccount (${relation(projected.batches, projectedAccount)}). Encodes " +
          s"${projected.encodes}. Visits ${projected.visits}. The extra passes are the pipeline " +
          "output column, the concat back to a partition, and the second slice. Probe bytes for " +
          s"those three are ${bytes(output.allocBytes)}, ${bytes(concat.allocBytes)}, and " +
          s"${bytes(reslice.allocBytes)}. The output probe reuses one `Int32Map`. The query " +
          "builds a mapper per morsel. That allocation is not in the probe.",
      )
      println(
        s"RAW t$threads alloc ${bytes(raw.allocBytes)}, batches ${raw.batches}, encodes " +
          s"${raw.encodes}, visits ${raw.visits}. The loop is four dependent adds on the " +
          "generator array. It does not encode, slice, concat, or write a column. The JIT may " +
          "fold the adds. At 4 threads the pre-started workers are woken inside the timer. " +
          "Their arithmetic does not allocate.",
      )
      val stageNames = Vector("encode", "partial", "merge", "exchange", "final")
      val stages = stageNames.map(name => findRow(measured, "stage", name, threads))
      val listed = stages
        .map { row =>
          s"${row.name} alloc ${bytes(row.allocBytes)} batches ${row.batches} " +
            s"encodes ${row.encodes} visits ${row.visits}"
        }
        .mkString("; ")
      println(
        s"GROUPED t$threads shape ${grouped.shape}. Sampled alloc ${bytes(grouped.allocBytes)}. " +
          s"Batches ${grouped.batches}, encodes ${grouped.encodes}, visits ${grouped.visits}. " +
          "The action's bytes are not tagged by stage. The replay rows are separate sampled " +
          s"lower bounds: $listed. Partial includes the input slice. Merge includes the concat " +
          "of morsel partials. Exchange includes the bucket scatter and the bucket concat. " +
          "Final includes the per-bucket `sumInt64` and the driver `ColumnReduce`. Stage " +
          "`gc_ms` is one total for the stage loop, repeated on each stage row.",
      )
    }
    println(
      s"Element morsels ${setup.morsels} (${elementRows / sourcePartitions} rows per partition, " +
        s"batch $batchSize). Grouped morsels ${setup.groupedMorsels}.",
    )

  private def sameText(left: String, right: String): Boolean =
    left.compareTo(right) == 0

  private def relation(left: Long, right: Long): String =
    if (left == right) then "matches" else "does not match"

  /** The row scheduler is constructed once. Swap it so the thread count is the one just set. */
  private def installThreads(threads: Int): Unit =
    val conf = ScarletConf.get
    ScarletConf.set(conf.copy(threadPoolSize = threads))
    val runtime = ScarletRuntime.get
    ScarletRuntime.set(runtime.copy(scheduler = new LocalTaskScheduler(threads)))

  private def shapeOf(collection: DistCollection[?]): String =
    ColumnarPlanner.lower(collection.plan).map(_.shape).getOrElse {
      throw new IllegalStateException("plan stayed on the row path")
    }

  private def ownedInts(collection: DistCollection[Int]): Vector[Array[Int]] =
    collection.plan match
      case Plan.Source(partitions, _) =>
        partitions.toVector.map { partition =>
          partition.data match
            case owned: IntArrays => owned.values
            case _ =>
              throw new IllegalStateException("ofInts partition was not an owned int array")
        }
      case _ =>
        throw new IllegalStateException("ofInts plan was not a source")

  private def encodeOwnedPairs(collection: DistCollection[(Int, Int)]): Vector[ColumnBatch] =
    collection.plan match
      case Plan.Source(partitions, _) =>
        partitions.toVector.map { partition =>
          partition.data match
            case owned: IntPairArrays => BatchCodec.copiedIntPairs(owned.keys, owned.values)
            case _ =>
              throw new IllegalStateException("ofIntPairs partition was not an owned pair array")
        }
      case _ =>
        throw new IllegalStateException("ofIntPairs plan was not a source")

  private def lengthOf(batches: Vector[ColumnBatch]): Int =
    batches.foldLeft(0)((total, batch) => total + batch.length)

  private def lengthOfNested(groups: Vector[Vector[ColumnBatch]]): Int =
    groups.foldLeft(0)((total, group) => total + lengthOf(group))

  private def morselCount(rows: Int, parts: Int, size: Int): Int =
    val perPartition = rows / parts
    parts * ((perPartition + size - 1) / size)

  private def slowestStage(stages: Vector[Row]): Row =
    stages match
      case head +: rest =>
        rest.foldLeft(head) { (best, row) =>
          if (row.p50Ns > best.p50Ns) then row else best
        }
      case _ =>
        throw new IllegalStateException("no grouped stages")

  private def findRow(rows: Vector[Row], kind: String, name: String, threads: Int): Row =
    rows
      .find(row => sameText(row.kind, kind) && sameText(row.name, name) && row.threads == threads)
      .getOrElse {
        throw new IllegalStateException(s"missing $kind $name threads=$threads")
      }

  private def header(allocOn: Boolean, callerOn: Boolean, setup: Setup): String =
    val runtime = ManagementFactory.getRuntimeMXBean
    val alloc = if (allocOn) then sampledKind else "unavailable"
    val caller = if (callerOn) then callerKind else "unavailable"
    s"materialisation study warmup=$warmup measure=$measure batch=$batchSize " +
      s"source_partitions=$sourcePartitions shuffle=$shuffleWidth alloc_bytes=$alloc " +
      s"caller_probes=$caller\n" +
      s"F4 rows=$elementRows checksum=${setup.fusedSum} shape=Pipeline4(Scan) " +
      s"P5 checksum=${setup.projectedSum} shape=Project(Pipeline4(Scan)) " +
      s"RAW checksum=${setup.fusedSum}\n" +
      s"GROUPED rows=$groupedRows groups=$groups checksum=${setup.groupedSum} " +
      s"shape=GroupedSum(Scan) element_morsels=${setup.morsels} " +
      s"grouped_morsels=${setup.groupedMorsels}\n" +
      s"java=${runtime.getVmName} ${runtime.getVmVersion} args=${runtime.getInputArguments}"

  private def tsv(row: Row): String =
    s"${row.kind}\t${row.name}\tt${row.threads}\tshape=${row.shape}\trows=${row.rowCount}\t" +
      s"p50_ns=${row.p50Ns}\tp95_ns=${row.p95Ns}\trows_per_s=${rowsPerSecond(row.rowCount, row.p50Ns)}\t" +
      s"alloc_bytes=${row.allocBytes}\talloc_kind=${row.allocKind}\tbatches=${row.batches}\t" +
      s"encodes=${row.encodes}\tvisits=${row.visits}\tgc_ms=${row.gcMs}\tchecksum=${row.checksum}"

  private def markdown(row: Row): String =
    s"| ${row.kind} | ${row.name} | ${row.threads} | ${row.shape} | ${ms(row.p50Ns)} ms | " +
      s"${ms(row.p95Ns)} ms | ${rowsPerSecond(row.rowCount, row.p50Ns)} | ${bytes(row.allocBytes)} | " +
      s"${row.allocKind} | ${row.batches} | ${row.encodes} | ${row.visits} | ${row.gcMs} |"

  private val sampledKind = "sampled-lower-bound"
  private val callerKind = "caller-thread"

  private def rowsPerSecond(rows: Int, p50Ns: Long): Long =
    if (p50Ns <= 0L) then 0L else (rows.toLong * 1000000000L) / p50Ns

  private def nsPerRow(p50Ns: Long, rows: Int): String =
    if (rows <= 0) then "n/a"
    else String.format(Locale.ENGLISH, "%.2f", p50Ns.toDouble / rows.toDouble)

  private def ms(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000.0)

  private def bytes(value: Long): String =
    if (value < 0L) then "n/a" else signedBytes(value)

  private def signedBytes(value: Long): String =
    String.format(Locale.ENGLISH, "%.2f MB", value.toDouble / 1000000.0)

  private def multiple(numerator: Long, denominator: Long): String =
    if (denominator == 0L) then "n/a"
    else String.format(Locale.ENGLISH, "%.2f", numerator.toDouble / denominator.toDouble)

  private def percent(numerator: Long, denominator: Long): String =
    if (denominator == 0L) then "n/a"
    else
      String.format(Locale.ENGLISH, "%.1f%%", (100.0 * numerator.toDouble) / denominator.toDouble)

  private def percentile(samples: Array[Long], percent: Int): Long =
    val sorted = samples.clone()
    java.util.Arrays.sort(sorted)
    val index =
      if (percent <= 0) then 0
      else if (percent >= 100) then sorted.length - 1
      else (percent * (sorted.length - 1)) / 100
    sorted(index)

  private def mean(values: Array[Long]): Long =
    var index = 0
    var total = 0L
    var negative = false
    while (index < values.length) {
      val value = values(index)
      if (value < 0L) then negative = true
      else total += value
      index += 1
    }
    if (negative) -1L else total / values.length.toLong

  private def same(values: Array[Long], label: String): Long =
    val first = values(0)
    var index = 1
    while (index < values.length) {
      if (values(index) != first) then
        throw new IllegalStateException(s"$label varied: ${values.mkString(",")}")
      index += 1
    }
    first

  private def expectPlus(values: Array[Int], steps: Int): Long =
    var index = 0
    var sum = 0L
    val extra = steps.toLong
    while (index < values.length) {
      sum += values(index).toLong + extra
      index += 1
    }
    sum

  private def rawRange(values: Array[Int], start: Int, end: Int): Long =
    var total = 0L
    var row = start
    while (row < end) {
      val first = values(row) + 1
      val second = first + 1
      val third = second + 1
      val fourth = third + 1
      total += fourth.toLong
      row += 1
    }
    total

  private def generate(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = 0x12345678
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  private def clearShuffle(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ()

  private def measure[A](probe: Option[AllocProbe])(body: => A): (A, Long, Long) =
    probe match
      case Some(active) => active.sample(body)
      case None =>
        val start = System.nanoTime()
        val value = body
        (value, System.nanoTime() - start, -1L)

  private def startProbe(): Option[AllocProbe] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
    }

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  /**
   * Samples live threads while an action runs. Morsel workers are gone when the action returns, so
   * a before/after pair on the caller misses their bytes. The probe's own counter is excluded.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class AllocProbe(bean: ThreadMXBean):
    private val capacity = 1024
    private val ids = new Array[Long](capacity)
    private val baseline = new Array[Long](capacity)
    private val high = new Array[Long](capacity)
    private var count = 0
    @volatile private var running = true
    @volatile private var probeId = 0L
    private val thread = new Thread(() => watch(), "scarlet-alloc-probe")

    thread.setDaemon(true)
    thread.start()
    while (probeId == 0L) LockSupport.parkNanos(10000L)

    def stop(): Unit =
      running = false
      LockSupport.unpark(thread)
      thread.join(1000L)
      ()

    def sample[A](body: => A): (A, Long, Long) =
      open()
      val start = System.nanoTime()
      val value = body
      val elapsed = System.nanoTime() - start
      (value, elapsed, close())

    private def watch(): Unit =
      probeId = Thread.currentThread().getId
      while (running) {
        observe(setBaseline = false)
        LockSupport.parkNanos(200000L)
      }

    private def open(): Unit =
      this.synchronized {
        count = 0
        observe(setBaseline = true)
      }

    private def close(): Long =
      this.synchronized {
        observe(setBaseline = false)
        var index = 0
        var total = 0L
        while (index < count) {
          if (ids(index) != probeId) then
            val base = if (baseline(index) < 0L) 0L else baseline(index)
            val delta = high(index) - base
            if (delta > 0L) then total += delta
          index += 1
        }
        total
      }

    private def observe(setBaseline: Boolean): Unit = this.synchronized {
      val live = bean.getAllThreadIds
      var index = 0
      while (index < live.length) {
        val id = live(index)
        val bytes = bean.getThreadAllocatedBytes(id)
        if (bytes >= 0L) then note(id, bytes, setBaseline)
        index += 1
      }
    }

    private def note(id: Long, bytes: Long, setBaseline: Boolean): Unit =
      val at = slot(id)
      if (at >= 0) then
        if (bytes > high(at)) then high(at) = bytes
        if (setBaseline) then baseline(at) = bytes

    private def slot(id: Long): Int =
      val existing = indexOf(id)
      if (existing >= 0) then existing
      else if (count >= capacity) then -1
      else
        val placed = count
        ids(placed) = id
        baseline(placed) = -1L
        high(placed) = 0L
        count = placed + 1
        placed

    private def indexOf(id: Long): Int =
      var index = 0
      var found = -1
      while (index < count && found < 0) {
        if (ids(index) == id) then found = index
        index += 1
      }
      found

  /**
   * Four adds over contiguous ranges. Workers are started once. A sample bumps an epoch and wakes
   * them. The arithmetic itself allocates nothing. One thread runs on the caller.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class RawFloor(values: Array[Int], workers: Int):
    private val width = workers
    private val starts = new Array[Int](math.max(width, 1))
    private val ends = new Array[Int](math.max(width, 1))
    private val partials = new Array[Long](math.max(width, 1))
    private val ready = new AtomicInteger(0)
    private val published = new AtomicInteger(0)
    @volatile private var open = true
    @volatile private var epoch = 0
    private val threads: Array[Thread] =
      if (width <= 1) then Array.empty[Thread]
      else Array.tabulate(width)(id => new Thread(() => worker(id), s"scarlet-raw-$id"))

    require(width >= 1, s"raw floor needs a positive thread count, got $width")
    if (width > 1) then
      fillRanges(values.length, starts, ends)
      threads.foreach { thread =>
        thread.setDaemon(true)
        thread.start()
      }
      awaitCount(ready, width)

    def stop(): Unit =
      if (width > 1) then
        open = false
        epoch = epoch + 1
        threads.foreach(thread => LockSupport.unpark(thread))
        threads.foreach(_.join(1000L))
        ()

    def sum(): Long =
      if (width <= 1) then rawRange(values, 0, values.length)
      else
        val ticket = epoch + 1
        published.set(0)
        epoch = ticket
        threads.foreach(thread => LockSupport.unpark(thread))
        awaitCount(published, width)
        var total = 0L
        var index = 0
        while (index < width) {
          total += partials(index)
          index += 1
        }
        total

    private def worker(id: Int): Unit =
      val from = starts(id)
      val until = ends(id)
      ready.incrementAndGet()
      var seen = 0
      while (open) {
        val ticket = epoch
        if (open && ticket != seen) then
          partials(id) = rawRange(values, from, until)
          seen = ticket
          published.incrementAndGet()
        else if (open) then LockSupport.parkNanos(200000L)
      }

    private def fillRanges(length: Int, intoStart: Array[Int], intoEnd: Array[Int]): Unit =
      val count = width
      val base = length / count
      val extra = length % count
      var index = 0
      var cursor = 0
      while (index < count) {
        val size = base + (if (index < extra) then 1 else 0)
        intoStart(index) = cursor
        cursor += size
        intoEnd(index) = cursor
        index += 1
      }
      require(cursor == length, s"split $cursor != $length")

    private def awaitCount(counter: AtomicInteger, expected: Int): Unit =
      val deadline = System.nanoTime() + 30000000000L
      while (counter.get() != expected) {
        if (System.nanoTime() > deadline) then
          throw new IllegalStateException(
            s"raw floor waited for $expected workers, saw ${counter.get()}",
          )
        LockSupport.parkNanos(10000L)
      }

  private def gcDelta(before: Long, after: Long): Long =
    if (before < 0L || after < 0L) then -1L else after - before

  private def gcMillis: Long =
    val beans = ManagementFactory.getGarbageCollectorMXBeans
    var index = 0
    var total = 0L
    var known = false
    while (index < beans.size) {
      val time = beans.get(index).getCollectionTime
      if (time >= 0L) then
        known = true
        total += time
      index += 1
    }
    if (known) total else -1L
