package com.ewoodbury.scarlet.execution

import java.util.{Arrays, Locale}

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Separates map-side combine, exchange, and reduce-side add for int `countByKey`. Not a test, and
 * not part of `make test`.
 *
 * The input is 1e6 int pairs, keys `row % 1024`, values the row index, with the key filter
 * `key % 3 != 0` applied to the arrays. Survivors are split into two source batches. Each phase
 * then follows [[HashCount.byKey]] without calling it and without calling `finish`: take the key
 * column, `PartialCombine.perBatch` with [[HashCount.int32]] and [[HashCount.addCounts]],
 * `HashExchange.partitionAll` at key ordinal 0, then [[HashCount.addCounts]] on each bucket. A
 * filter on a real `countByKey` lowers to `byKeySelected`. This bench does not call that path.
 *
 * Spill stays off, so the add is the in-memory branch `finish` takes when no bucket is over the
 * budget. p50 is nearest-rank `ceil(p / 100 * n)` over 40 rounds after 5 warmups, at 1 and 4
 * threads. Partition width is the shuffle default, 4. Allocation is not measured. An empty vector,
 * a partial that does not shrink the filtered rows, or a bucket-count sum that disagrees with the
 * filtered row count fails the process.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.PartialExchangeBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object PartialExchangeBench:

  private val rows = 1000000
  private val groups = 1024
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val width = 4
  private val warmup = 5
  private val rounds = 40
  private val threadCounts: Vector[Int] = Vector(1, 4)

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  def main(args: Array[String]): Unit =
    val saved = ScarletConf.get
    var status = 0
    try
      ScarletConf.set(
        saved.copy(
          columnarExecution = true,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = width,
          spillEnabled = false,
          threadPoolSize = 1,
        ),
      )
      run()
    catch case _: HarnessRefusal => status = 1
    finally ScarletConf.set(saved)
    if (status != 0) then sys.exit(status)

  private def run(): Unit =
    val (keysOnly, filteredRows) = filteredKeys()
    val partials = accept("partial", combine(keysOnly, 1))
    val partialRows = rowCount(partials)
    if (partialRows <= 0L || partialRows >= filteredRows) then
      refuse(s"partial rows $partialRows did not reduce filtered rows $filteredRows")
    val buckets = acceptWidth("exchange", exchange(partials, 1))
    val partitionedRows = rowCount(buckets)
    if (partitionedRows != partialRows) then
      refuse(s"partitioned rows $partitionedRows != partial rows $partialRows")
    val counted = sumCounts(acceptWidth("addCounts", reduce(buckets, 1)))
    if (counted != filteredRows) then
      refuse(s"bucket counts $counted != filtered rows $filteredRows")
    preamble(filteredRows, partialRows, partitionedRows)
    threadCounts.foreach { threads =>
      val partialMs = sample(combine(keysOnly, threads)) { got =>
        val produced = rowCount(accept("partial", got))
        if (produced != partialRows) then refuse(s"partial rows $produced != $partialRows")
      }
      val exchangeMs = sample(exchange(partials, threads)) { got =>
        val produced = rowCount(acceptWidth("exchange", got))
        if (produced != partitionedRows) then
          refuse(s"partitioned rows $produced != $partitionedRows")
      }
      val addMs = sample(reduce(buckets, threads)) { got =>
        val sum = sumCounts(acceptWidth("addCounts", got))
        if (sum != filteredRows) then refuse(s"bucket counts $sum != filtered rows $filteredRows")
      }
      println(line("partial", threads, partialMs, filteredRows, partialRows, partitionedRows))
      println(line("exchange", threads, exchangeMs, filteredRows, partialRows, partitionedRows))
      println(line("addCounts", threads, addMs, filteredRows, partialRows, partitionedRows))
    }

  /** [[HashCount.byKey]] drops the value column, then partial-counts with these two functions. */
  private def combine(keysOnly: Vector[ColumnBatch], threads: Int): Vector[ColumnBatch] =
    PartialCombine.perBatch(
      keysOnly,
      batchSize,
      threads,
      batch => HashCount.int32(batch),
      batch => HashCount.addCounts(batch),
    )

  /** [[HashCount]] `finish` exchanges at key ordinal 0. This bench does not call `finish`. */
  private def exchange(partials: Vector[ColumnBatch], threads: Int): Vector[ColumnBatch] =
    HashExchange.partitionAll(
      partials,
      keyOrdinal = 0,
      numPartitions = width,
      parallelism = threads,
    )

  /** In-memory reduce-side add. One call per bucket, the same worker `finish` uses. */
  private def reduce(buckets: Vector[ColumnBatch], threads: Int): Vector[ColumnBatch] =
    MorselScheduler.map(buckets, threads)((_, bucket) => HashCount.addCounts(bucket))

  private def filteredKeys(): (Vector[ColumnBatch], Long) =
    val keptKeys = new Array[Int](rows)
    val keptValues = new Array[Int](rows)
    var row = 0
    var kept = 0
    while (row < rows) do
      val key = row % groups
      if (key % 3 != 0) then
        keptKeys(kept) = key
        keptValues(kept) = row
        kept += 1
      row += 1
    val expected = keptByFormula(rows)
    if (kept.toLong != expected) then refuse(s"key filter kept $kept rows, formula $expected")
    val keys = Arrays.copyOf(keptKeys, kept)
    val values = Arrays.copyOf(keptValues, kept)
    val pairs = pairBatches(keys, values)
    val keysOnly = pairs.map(batch => ColumnBatches.takeColumn(batch, 0))
    if (keysOnly.isEmpty) then refuse("key filter produced no batch")
    (keysOnly, kept.toLong)

  private def pairBatches(keys: Array[Int], values: Array[Int]): Vector[ColumnBatch] =
    val length = keys.length
    val base = length / sourcePartitions
    val extra = length % sourcePartitions
    Vector.tabulate(sourcePartitions) { index =>
      val start = index * base + math.min(index, extra)
      val end = start + base + (if (index < extra) then 1 else 0)
      if (end <= start) then refuse(s"source partition $index is empty")
      BatchCodec.copiedIntPairs(
        Arrays.copyOfRange(keys, start, end),
        Arrays.copyOfRange(values, start, end),
      )
    }

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

  private def sample(action: => Vector[ColumnBatch])(check: Vector[ColumnBatch] => Unit): Double =
    var round = 0
    while (round < warmup) do
      check(action)
      round += 1
    val samples = new Array[Double](rounds)
    round = 0
    while (round < rounds) do
      val started = System.nanoTime()
      val produced = action
      val elapsed = (System.nanoTime() - started) / 1_000_000.0
      check(produced)
      samples(round) = elapsed
      round += 1
    nearestP50(samples)

  /** 1-based rank `ceil(50 / 100 * n)`, no interpolation. Forty rounds use rank 20. */
  private def nearestP50(samples: Array[Double]): Double =
    Arrays.sort(samples)
    val count = samples.length
    val rank = (50 * count + 99) / 100
    val index = math.max(1, math.min(count, rank)) - 1
    samples(index)

  private def accept(phase: String, batches: Vector[ColumnBatch]): Vector[ColumnBatch] =
    if (batches.isEmpty) then refuse(s"$phase returned an empty vector")
    else batches

  private def acceptWidth(phase: String, batches: Vector[ColumnBatch]): Vector[ColumnBatch] =
    val accepted = accept(phase, batches)
    if (accepted.length != width) then
      refuse(s"$phase returned ${accepted.length} batches, expected $width")
    else accepted

  private def rowCount(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((total, batch) => total + batch.length.toLong)

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
      case _ =>
        refuse(s"addCounts batch of ${batch.length} rows has no Int32 count column")

  private def preamble(filteredRows: Long, partialRows: Long, partitionedRows: Long): Unit =
    val runtime = java.lang.management.ManagementFactory.getRuntimeMXBean
    System.err.println(
      s"partial-exchange rows=$rows filtered_rows=$filteredRows partial_rows=$partialRows " +
        s"partitioned_rows=$partitionedRows width=$width batch=$batchSize warmup=$warmup " +
        s"rounds=$rounds threads=${threadCounts.mkString(",")} key_filter=key%3!=0 " +
        "spill=false p50=nearest-rank alloc=not-measured " +
        s"java=${runtime.getVmName} ${runtime.getVmVersion}",
    )

  private def line(
      phase: String,
      threads: Int,
      p50Ms: Double,
      filteredRows: Long,
      partialRows: Long,
      partitionedRows: Long,
  ): String =
    val p50 = String.format(Locale.ENGLISH, "%.3f", p50Ms)
    "{" +
      """"schema":"partial-exchange-1",""" +
      s""""phase":"$phase",""" +
      s""""threads":$threads,""" +
      s""""width":$width,""" +
      s""""rows":$rows,""" +
      s""""filtered_rows":$filteredRows,""" +
      s""""partial_rows":$partialRows,""" +
      s""""partitioned_rows":$partitionedRows,""" +
      s""""batch":$batchSize,""" +
      s""""warmup":$warmup,""" +
      s""""rounds":$rounds,""" +
      s""""p50_ms":$p50}"""

  private def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)
