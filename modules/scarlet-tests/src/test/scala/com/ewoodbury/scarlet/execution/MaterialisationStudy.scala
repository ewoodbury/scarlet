package com.ewoodbury.scarlet.execution

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntArrays, IntPairArrays, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

import MaterialisationStudySupport.*

/**
 * Measures the columnar materialisation boundary. Not a test, and not part of `make test`.
 *
 * F4 is `trySumInt` on `Pipeline4(Scan)`: four maps inside `ElementSum`, no output column. B4 is
 * the same four maps with one forced boundary: encode, slice, `ElementPipeline`, concat, reslice,
 * then `ColumnReduce.sumInt32`. S5 keeps that prefix and replaces the plain sum with
 * `sumProjected` of one more `+1`. P5 is `trySumInt` on `Project(Pipeline4(Scan))`: the trailing
 * map is folded into the sum, so the pipeline column is not written. The boundary cost is B4
 * minus F4. The fifth-map cost on that forced boundary is S5 minus B4. P5 minus F4 is the folded
 * fifth map.
 *
 * RAW is four dependent `+1` adds, a folded-loop floor. RAWD is four data-dependent updates.
 * Both create and join their workers inside the sample when the width is above one. GROUPED is
 * `tryGroupedSum`. REPLAY times the five stage calls as one action. Stage percentiles are not
 * added together.
 *
 * Timed F4, P5, and GROUPED fail if the planner returns `None`, so a row-path fallback is not
 * recorded as a columnar result. `tryRun` is asserted in `prepare` for the int source, F4, and P5.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.MaterialisationStudy"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object MaterialisationStudy:

  private val plusA: Int => Int = value => value + 1
  private val plusB: Int => Int = value => value + 1
  private val plusC: Int => Int = value => value + 1
  private val plusD: Int => Int = value => value + 1
  private val plusE: Int => Int = value => value + 1

  private val fourSteps: List[ColumnarPlanner.ElementStep] =
    List(
      ColumnarPlanner.ElementStep.Project(plusA),
      ColumnarPlanner.ElementStep.Project(plusB),
      ColumnarPlanner.ElementStep.Project(plusC),
      ColumnarPlanner.ElementStep.Project(plusD),
    )

  private val stagePlan: Vector[(String, String)] =
    Vector(
      ("encode", "copiedIntPairs"),
      ("partial", "range+sumInt32"),
      ("merge", "mergeSumPartials"),
      ("exchange", "partitionAll"),
      ("final", "sumInt64+ColumnReduce"),
    )

  private val mergeSum: ColumnBatch => ColumnBatch =
    batch => HashAggregate.sumInt64(batch, keyOrdinal = 0, valueOrdinal = 1)

  private object PlusOne extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value + 1

  private val pipeline: ColumnBatch => ColumnBatch =
    morsel => Int32Pipeline.PPPP(morsel, PlusOne, PlusOne, PlusOne, PlusOne)

  private final case class Setup(
      values: Array[Int],
      element: DistCollection[Int],
      fused: DistCollection[Int],
      projected: DistCollection[Int],
      grouped: DistCollection[(Int, Int)],
      fusedSum: Long,
      projectedSum: Long,
      dependentSum: Long,
      groupedSum: Long,
      morsels: Int,
      groupedMorsels: Int,
  )

  private final class Series:
    val elapsed: Array[Long] = new Array[Long](measure)
    val batches: Array[Long] = new Array[Long](measure)
    val encodes: Array[Long] = new Array[Long](measure)
    val visits: Array[Long] = new Array[Long](measure)
    val gc: Array[Long] = new Array[Long](measure)
    val allocEstimate: Array[Long] = new Array[Long](allocSamples)
    val allocCaller: Array[Long] = new Array[Long](allocSamples)

    java.util.Arrays.fill(allocEstimate, -1L)
    java.util.Arrays.fill(allocCaller, -1L)

  private final class Query(
      val kind: String,
      val name: String,
      val shape: String,
      val rows: Int,
      val checksum: Long,
      val raw: Boolean,
      val dependent: Boolean,
      val body: Int => Long,
  ):
    val series: Array[Series] = Array.fill(threadCounts.length)(new Series)

  private trait Meter[A]:
    def apply[B](body: => B): (B, A)

  def main(args: Array[String]): Unit =
    checkPercentileConvention()
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    ExecutionModuleInit.initialize()
    val sampler = startSampler()
    try execute(sampler, savedConf)
    finally
      ColumnMetrics.restore(NoOpSink)
      sampler.foreach(_.stop())
      ScarletConf.set(savedConf)
      ScarletRuntime.set(savedRuntime)

  private def execute(sampler: Option[AllocSampler], savedConf: ScarletConf): Unit =
    ScarletConf.set(
      savedConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shuffleWidth,
        threadPoolSize = 1,
      ),
    )
    val setup = prepare()
    val queries = buildQueries(setup)
    val stages = Array.fill(stagePlan.length)(Array.fill(threadCounts.length)(new Series))
    System.err.println("warm")
    rounds(warmup, record = false, queries, stages, setup)
    System.err.println("measure")
    rounds(measure, record = true, queries, stages, setup)
    sampler.foreach { active =>
      System.err.println("allocate")
      allocate(active, queries, stages, setup)
    }
    installThreads(1)
    val probes = measureProbes(sampler.map(_.bean), setup)
    val facts = MaterialisationStudyReport.Facts(
      setup.fusedSum,
      setup.projectedSum,
      setup.dependentSum,
      setup.groupedSum,
      setup.morsels,
      setup.groupedMorsels,
      sampler.isDefined,
    )
    MaterialisationStudyReport.printAll(
      queries.flatMap(query => threadCounts.map(threads => toQueryRow(query, threads, facts))),
      stageRows(stages, facts),
      probes,
      facts,
    )

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
    val dependentSum = rawDependent(values, 0, values.length)
    val groupedSum = groupedRows.toLong * (groupedRows.toLong - 1L) / 2L
    require(rawFolded(values, 0, values.length) == fusedSum, "folded loop disagrees with the oracle")
    require(dependentSum != fusedSum, "dependent floor collapsed to the folded sum")
    require(sameText(shapeOf(fused), "Pipeline4(Scan)"), shapeOf(fused))
    require(sameText(shapeOf(projected), "Project(Pipeline4(Scan))"), shapeOf(projected))
    val groupedShape = ColumnarPlanner.groupedSumPeel(grouped.plan).getOrElse {
      throw new IllegalStateException("grouped sum did not peel")
    }
    require(sameText(groupedShape, "GroupedSum(Scan)"), groupedShape)
    requireSum(fused.plan, fusedSum, "F4")
    requireSum(projected.plan, projectedSum, "P5")
    requireGrouped(grouped.plan, groupedSum)
    requireRun(element.plan, elementRows, "source")
    requireRun(fused.plan, elementRows, "F4")
    requireRun(projected.plan, elementRows, "P5")
    val morsels = morselCount(elementRows, sourcePartitions, batchSize)
    val groupedMorsels = morselCount(groupedRows, sourcePartitions, batchSize)
    require(morsels > sourcePartitions, "element partitions are not sliced more than once")
    require(groupedMorsels > sourcePartitions, "grouped partitions are not sliced more than once")
    Setup(
      values,
      element,
      fused,
      projected,
      grouped,
      fusedSum,
      projectedSum,
      dependentSum,
      groupedSum,
      morsels,
      groupedMorsels,
    )

  private def buildQueries(setup: Setup): Vector[Query] =
    Vector(
      action("query", "F4", "Pipeline4(Scan)+ElementSum", elementRows, setup.fusedSum) { _ =>
        acceptedSum(setup.fused.plan, "F4")
      },
      action(
        "query",
        "B4",
        "encode+slice+pipeline+concat+reslice+sumInt32",
        elementRows,
        setup.fusedSum,
      ) { threads =>
        materialised(setup.element, threads, fifth = false)
      },
      action(
        "query",
        "S5",
        "encode+slice+pipeline+concat+reslice+sumProjected",
        elementRows,
        setup.projectedSum,
      ) { threads =>
        materialised(setup.element, threads, fifth = true)
      },
      action(
        "query",
        "P5",
        "Project(Pipeline4(Scan))+foldedSum",
        elementRows,
        setup.projectedSum,
      ) { _ =>
        acceptedSum(setup.projected.plan, "P5")
      },
      rawQuery("RAW", "folded-four-adds", setup.fusedSum, dependent = false),
      rawQuery("RAWD", "data-dependent-four-updates", setup.dependentSum, dependent = true),
      action("query", "GROUPED", "GroupedSum(Scan)", groupedRows, setup.groupedSum) { _ =>
        acceptedGrouped(setup.grouped.plan)
      },
      action(
        "replay",
        "REPLAY",
        "encode+partial+merge+exchange+final",
        groupedRows,
        setup.groupedSum,
      ) { threads =>
        groupedOnce(setup.grouped, threads)
      },
    )

  private def rounds(
      count: Int,
      record: Boolean,
      queries: Vector[Query],
      stages: Array[Array[Series]],
      setup: Setup,
  ): Unit =
    var round = 0
    while (round < count) {
      val order = if (round % 2 == 0) then threadCounts else threadCounts.reverse
      val recorded = if (record) then Some(round) else None
      order.foreach { threads =>
        installThreads(threads)
        queries.foreach(query => runQuery(query, threads, recorded, setup.values))
        runStages(setup, threads, recorded, stages)
      }
      System.err.println(s"${if (record) then "measure" else "warm"} ${round + 1}/$count")
      round += 1
    }

  private def runQuery(
      query: Query,
      threads: Int,
      recorded: Option[Int],
      values: Array[Int],
  ): Unit =
    clearShuffle()
    val slot = recorded.map(index => (query.series(widthSlot(threads)), index))
    val label = s"${query.name}-t$threads"
    if (query.raw) then
      val floor = new RawFloor(values, threads)
      runChecked(label, query.checksum, slot)(floor.sum(query.dependent, None))
    else runChecked(label, query.checksum, slot)(query.body(threads))

  private def runStages(
      setup: Setup,
      threads: Int,
      recorded: Option[Int],
      stages: Array[Array[Series]],
  ): Unit =
    clearShuffle()
    val marks = stageChain(
      setup.grouped,
      threads,
      setup.groupedSum,
      new Meter[Timed]:
        def apply[B](body: => B): (B, Timed) = timeOnce(body),
    )
    recorded.foreach { index =>
      marks.zipWithIndex.foreach { (timed, stage) =>
        val series = stages(stage)(widthSlot(threads))
        series.elapsed(index) = timed.elapsedNs
        series.batches(index) = timed.batches
        series.encodes(index) = timed.encodes
        series.visits(index) = timed.visits
        series.gc(index) = timed.gcMs
      }
    }

  private def allocate(
      sampler: AllocSampler,
      queries: Vector[Query],
      stages: Array[Array[Series]],
      setup: Setup,
  ): Unit =
    var sample = 0
    while (sample < allocSamples) {
      val at = sample
      threadCounts.foreach { threads =>
        installThreads(threads)
        queries.foreach(query => allocateQuery(sampler, query, threads, at, setup.values))
        allocateStages(sampler, setup, threads, at, stages)
      }
      System.err.println(s"allocate ${at + 1}/$allocSamples")
      sample += 1
    }

  private def allocateQuery(
      sampler: AllocSampler,
      query: Query,
      threads: Int,
      sample: Int,
      values: Array[Int],
  ): Unit =
    clearShuffle()
    val account =
      if (query.raw) then
        val floor = new RawFloor(values, threads)
        sampler.account(floor.sum(query.dependent, Some(sampler.bean)))(floor.publishedTotal)
      else sampler.account(query.body(threads))(0L)
    require(account.value == query.checksum, s"${query.name}-t$threads alloc ${account.value}")
    val series = query.series(widthSlot(threads))
    series.allocCaller(sample) = account.callerBytes
    series.allocEstimate(sample) = account.estimateBytes

  private def allocateStages(
      sampler: AllocSampler,
      setup: Setup,
      threads: Int,
      sample: Int,
      stages: Array[Array[Series]],
  ): Unit =
    clearShuffle()
    val marks = stageChain(
      setup.grouped,
      threads,
      setup.groupedSum,
      new Meter[(Long, Long)]:
        def apply[B](body: => B): (B, (Long, Long)) =
          val account = sampler.account(body)(0L)
          (account.value, (account.callerBytes, account.estimateBytes)),
    )
    marks.zipWithIndex.foreach { (mark, stage) =>
      val (callerBytes, estimateBytes) = mark
      val series = stages(stage)(widthSlot(threads))
      series.allocCaller(sample) = callerBytes
      series.allocEstimate(sample) = estimateBytes
    }

  private def stageChain[A](
      grouped: DistCollection[(Int, Int)],
      threads: Int,
      checksum: Long,
      meter: Meter[A],
  ): Vector[A] =
    val (encoded, encodeMark) = meter(encodeOwnedPairs(grouped))
    require(lengthOf(encoded) == groupedRows, "grouped encode dropped rows")
    val (split, partialMark) = meter {
      GroupedSumKernel.partialMorsels(
        encoded,
        keyOrdinal = 0,
        valueOrdinal = 1,
        parallelism = threads,
        batchSize = batchSize,
        steps = Vector.empty[PairStep],
      )
    }
    // Two input batches, each with many morsels. The fold publishes one partial per batch: 2
    // batches and 2 visits. Concatenation used to add one batch per input batch (4 batches).
    val (merged, mergeMark) = meter {
      GroupedSumKernel.mergePartials(split._1, split._2, threads)
    }
    val (buckets, exchangeMark) = meter {
      HashExchange.partitionAll(
        merged,
        keyOrdinal = 0,
        numPartitions = shuffleWidth,
        parallelism = threads,
      )
    }
    val (got, finalMark) = meter {
      val summed = MorselScheduler.map(buckets, threads)((_, bucket) => mergeSum(bucket))
      ColumnReduce.sumInt64(summed, ordinal = 1)
    }
    require(got == checksum, s"stages-t$threads $got != $checksum")
    Vector(encodeMark, partialMark, mergeMark, exchangeMark, finalMark)

  private def groupedOnce(source: DistCollection[(Int, Int)], threads: Int): Long =
    val encoded = encodeOwnedPairs(source)
    val (sizes, partials) = GroupedSumKernel.partialMorsels(
      encoded,
      keyOrdinal = 0,
      valueOrdinal = 1,
      parallelism = threads,
      batchSize = batchSize,
      steps = Vector.empty[PairStep],
    )
    val merged = GroupedSumKernel.mergePartials(sizes, partials, threads)
    val buckets = HashExchange.partitionAll(
      merged,
      keyOrdinal = 0,
      numPartitions = shuffleWidth,
      parallelism = threads,
    )
    val summed = MorselScheduler.map(buckets, threads)((_, bucket) => mergeSum(bucket))
    ColumnReduce.sumInt64(summed, ordinal = 1)

  private def materialised(element: DistCollection[Int], threads: Int, fifth: Boolean): Long =
    val encoded = ownedInts(element).map(values => BatchCodec.copiedInts(values))
    val sliced = encoded.map(batch => ColumnBatches.slice(batch, batchSize))
    val counts = sliced.map(_.length)
    val written = MorselScheduler.map(sliced.flatten, threads) { (_, morsel) =>
      ElementPipeline.ints(morsel, fourSteps)
    }
    val concatenated = regroup(counts, written)
    val resliced = concatenated.flatMap(batch => ColumnBatches.slice(batch, batchSize))
    val partials =
      if (fifth) then
        MorselScheduler.map(resliced, threads) { (_, morsel) =>
          val step = plusE
          Int32Pipeline.sumProjected(morsel, value => step(value))
        }
      else MorselScheduler.map(resliced, threads)((_, morsel) => ColumnReduce.sumInt32At(morsel, 0))
    partials.foldLeft(0L)(_ + _)

  private def measureProbes(bean: Option[ThreadMXBean], setup: Setup): Vector[Row] =
    val owned = ownedInts(setup.element)
    val encoded = owned.map(values => BatchCodec.copiedInts(values))
    val slicedGroups = encoded.map(batch => ColumnBatches.slice(batch, batchSize))
    val written = slicedGroups.map(group => group.map(pipeline))
    val concatenated = written.map(group => ColumnBatches.concat(group))
    require(lengthOf(encoded) == elementRows, "probe setup encode dropped rows")
    require(lengthOfNested(slicedGroups) == elementRows, "probe setup slice dropped rows")
    Vector(
      benchCaller(bean, "encode", "copiedInts", elementRows, lengthOf) {
        owned.map(values => BatchCodec.copiedInts(values))
      },
      benchCaller(bean, "slice", "ColumnBatches.slice", elementRows, lengthOfNested) {
        encoded.map(batch => ColumnBatches.slice(batch, batchSize))
      },
      benchCaller(bean, "output", "Int32Pipeline.PPPP", elementRows, lengthOfNested) {
        slicedGroups.map(group => group.map(pipeline))
      },
      benchCaller(bean, "concat", "ColumnBatches.concat", elementRows, lengthOf) {
        written.map(group => ColumnBatches.concat(group))
      },
      benchCaller(bean, "reslice", "ColumnBatches.slice", elementRows, lengthOfNested) {
        concatenated.map(batch => ColumnBatches.slice(batch, batchSize))
      },
    )

  private def benchCaller[A](
      bean: Option[ThreadMXBean],
      name: String,
      shape: String,
      rowCount: Int,
      rowsOf: A => Int,
  )(
      action: => A,
  ): Row =
    System.err.println(s"probe $name")
    val elapsed = new Array[Long](measure)
    val batches = new Array[Long](measure)
    val encodes = new Array[Long](measure)
    val visits = new Array[Long](measure)
    val gc = new Array[Long](measure)
    val alloc = new Array[Long](allocSamples)
    var sample = 0
    while (sample < warmup) {
      val (produced, _) = timeOnce(action)
      require(rowsOf(produced) == rowCount, s"$name warmup rows")
      sample += 1
    }
    sample = 0
    while (sample < measure) {
      val (produced, timed) = timeOnce(action)
      require(rowsOf(produced) == rowCount, s"$name rows")
      elapsed(sample) = timed.elapsedNs
      batches(sample) = timed.batches
      encodes(sample) = timed.encodes
      visits(sample) = timed.visits
      gc(sample) = timed.gcMs
      sample += 1
    }
    sample = 0
    while (sample < allocSamples) {
      val account = callerAccount(bean)(action)
      require(rowsOf(account.value) == rowCount, s"$name alloc rows")
      alloc(sample) = account.callerBytes
      sample += 1
    }
    val bytesMean = mean(alloc)
    rowOf(
      "probe",
      name,
      1,
      shape,
      rowCount,
      0L,
      if (bytesMean < 0L) then unavailableKind else callerKind,
      elapsed,
      batches,
      encodes,
      visits,
      gc,
      bytesMean,
      bytesMean,
      -1L,
    )

  private def toQueryRow(query: Query, threads: Int, facts: MaterialisationStudyReport.Facts): Row =
    val series = query.series(widthSlot(threads))
    rowOf(
      query.kind,
      query.name,
      threads,
      query.shape,
      query.rows,
      query.checksum,
      if (facts.allocOn) then estimatedKind else unavailableKind,
      series.elapsed,
      series.batches,
      series.encodes,
      series.visits,
      series.gc,
      mean(series.allocEstimate),
      mean(series.allocCaller),
      orderSpread(series.elapsed, threads == 1),
    )

  private def stageRows(stages: Array[Array[Series]], facts: MaterialisationStudyReport.Facts): Vector[Row] =
    stagePlan.zipWithIndex.flatMap { (described, stage) =>
      val (name, shape) = described
      threadCounts.map { threads =>
        val series = stages(stage)(widthSlot(threads))
        rowOf(
          "stage",
          name,
          threads,
          shape,
          groupedRows,
          0L,
          if (facts.allocOn) then estimatedKind else unavailableKind,
          series.elapsed,
          series.batches,
          series.encodes,
          series.visits,
          series.gc,
          mean(series.allocEstimate),
          mean(series.allocCaller),
          orderSpread(series.elapsed, threads == 1),
        )
      }
    }

  private def runChecked(label: String, checksum: Long, slot: Option[(Series, Int)])(
      body: => Long,
  ): Unit =
    val (got, timed) = timeOnce(body)
    require(got == checksum, s"$label $got != $checksum")
    slot.foreach { (series, index) =>
      series.elapsed(index) = timed.elapsedNs
      series.batches(index) = timed.batches
      series.encodes(index) = timed.encodes
      series.visits(index) = timed.visits
      series.gc(index) = timed.gcMs
    }

  private def action(kind: String, name: String, shape: String, rows: Int, checksum: Long)(
      body: Int => Long,
  ): Query =
    new Query(kind, name, shape, rows, checksum, raw = false, dependent = false, body)

  private def rawQuery(name: String, shape: String, checksum: Long, dependent: Boolean): Query =
    new Query(
      "query",
      name,
      shape,
      elementRows,
      checksum,
      raw = true,
      dependent = dependent,
      _ => throw new IllegalStateException(s"$name has no engine body"),
    )

  private def acceptedSum(plan: Plan[Int], label: String): Long =
    ColumnarPlanner.trySumInt(plan).getOrElse {
      throw new IllegalStateException(s"$label trySumInt refused")
    }

  private def acceptedGrouped(plan: Plan[(Int, Int)]): Long =
    ColumnarPlanner.tryGroupedSum(plan).getOrElse {
      throw new IllegalStateException("GROUPED tryGroupedSum refused")
    }

  private def requireSum(plan: Plan[Int], expected: Long, label: String): Unit =
    ColumnarPlanner.trySumInt(plan) match
      case Some(got) => require(got == expected, s"$label trySumInt $got != $expected")
      case None => throw new IllegalStateException(s"$label trySumInt refused")

  private def requireGrouped(plan: Plan[(Int, Int)], expected: Long): Unit =
    ColumnarPlanner.tryGroupedSum(plan) match
      case Some(got) => require(got == expected, s"GROUPED tryGroupedSum $got != $expected")
      case None => throw new IllegalStateException("GROUPED tryGroupedSum refused")

  private def requireRun[A](plan: Plan[A], rows: Int, label: String): Unit =
    ColumnarPlanner.tryRun(plan) match
      case None => throw new IllegalStateException(s"$label tryRun refused")
      case Some(parts) =>
        val count = parts.foldLeft(0)((total, part) => total + part.data.size)
        require(count == rows, s"$label tryRun rows $count != $rows")

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

  private def regroup(counts: Vector[Int], flat: Vector[ColumnBatch]): Vector[ColumnBatch] =
    val emptyBatches = Vector.empty[ColumnBatch]
    val seed: (Vector[ColumnBatch], Vector[ColumnBatch]) = (flat, emptyBatches)
    val (_, grouped) = counts.foldLeft(seed) { (state, count) =>
      val (rest, acc) = state
      val (group, tail) = rest.splitAt(count)
      (tail, acc :+ ColumnBatches.concat(group))
    }
    grouped

  private def lengthOf(batches: Vector[ColumnBatch]): Int =
    batches.foldLeft(0)((total, batch) => total + batch.length)

  private def lengthOfNested(groupsOf: Vector[Vector[ColumnBatch]]): Int =
    groupsOf.foldLeft(0)((total, group) => total + lengthOf(group))

  private def morselCount(rows: Int, parts: Int, size: Int): Int =
    val perPartition = rows / parts
    parts * ((perPartition + size - 1) / size)

  private def widthSlot(threads: Int): Int =
    val index = threadCounts.indexOf(threads)
    if (index < 0) then throw new IllegalArgumentException(s"unexpected width $threads") else index

  private def clearShuffle(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ()

  private def sameText(left: String, right: String): Boolean =
    left.compareTo(right) == 0
