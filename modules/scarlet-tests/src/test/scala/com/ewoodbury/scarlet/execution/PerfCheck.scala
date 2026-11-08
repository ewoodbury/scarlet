package com.ewoodbury.scarlet.execution

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Lightweight CI performance and correctness check. Not part of `make test`.
 *
 * Runs three filter queries on 200k rows in both layouts and compares checksums. It fails when a
 * checksum differs, the published batches are wrong, or the columnar p50 is more than twice the
 * row p50. A columnar sample must publish a batch and a row sample must publish none. The
 * threshold is deliberately loose: a shared CI runner is noisy, and this is a smoke test for a
 * clear regression, not a benchmark.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object PerfCheck:

  private val rows = 200_000
  private val groups = 256
  private val threads = 4
  private val batchSize = 8192
  private val warmup = 2
  private val rounds = 5
  private val maxRatio = 2.0

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = threads,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = 4,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val failures = run()
      if failures.nonEmpty then
        System.err.println(s"perf check failed: ${failures.mkString("; ")}")
        sys.exit(1)
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private def run(): List[String] =
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    val sortKeys = permutation(rows)
    val pairs = DistCollection.ofIntPairs(keys, values, 2)
    val filtered = pairs
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val sorted = DistCollection
      .ofIntPairs(sortKeys, values, 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .sortByIntKey

    val cases = List(
      ("filter-count", () => filtered.count()),
      (
        "filter-reducebykey",
        () =>
          filtered
            .reduceByKey[Int, Int](_ + _)
            .collect()
            .map { case (key, value) => key * 31L + value }
            .sum,
      ),
      (
        "filter-sort",
        () => sorted.collect().map { case (key, value) => key * 31L + value }.sum,
      ),
    )

    cases.flatMap { case (name, action) =>
      val columnar = measure(name, columnar = true, action)
      val row = measure(name, columnar = false, action)
      val ratio = columnar.millis / math.max(row.millis, 0.001)
      println(
        f"PERF $name%-20s columnar=${columnar.millis}%8.3f ms row=${row.millis}%8.3f ms " +
          f"ratio=$ratio%5.2f checksum=${columnar.checksum}",
      )
      val problems = List(
        Option.when(columnar.checksum != row.checksum)(
          s"$name checksum ${columnar.checksum} != ${row.checksum}",
        ),
        Option.when(ratio > maxRatio)(
          f"$name columnar is $ratio%.2f times the row path",
        ),
        columnar.batchProblem,
        row.batchProblem,
      ).flatten
      problems
    }

  private final case class Measured(millis: Double, checksum: Long, batchProblem: Option[String])

  private def measure(name: String, columnar: Boolean, action: () => Any): Measured =
    val conf = ScarletConf.get
    ScarletConf.set(conf.copy(columnarExecution = columnar))
    var round = 0
    while (round < warmup) do
      action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksum = 0L
    var problem: Option[String] = None
    round = 0
    // Do not catch ColumnarFallback. A decline reruns on rows and publishes no batch.
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        val start = System.nanoTime()
        val value = action()
        samples(round) = (System.nanoTime() - start) / 1_000_000.0
        checksum = checksumOf(value)
        if problem.isEmpty then problem = publishedMiss(name, columnar, sink.batchCount)
      finally ColumnMetrics.restore(previous)
      round += 1
    java.util.Arrays.sort(samples)
    Measured(samples(samples.length / 2), checksum, problem)

  private def publishedMiss(name: String, columnar: Boolean, batches: Long): Option[String] =
    if columnar then Option.when(batches == 0L)(s"$name columnar published no column batch")
    else Option.when(batches != 0L)(s"$name row published $batches column batches")

  private def checksumOf(value: Any): Long = value match
    case number: Long => number
    case number: Int => number.toLong
    case other => throw new IllegalStateException(s"unexpected checksum $other")

  private def lcgValues(count: Int): Array[Int] =
    val values = new Array[Int](count)
    var index = 0
    var state = 0x12345678
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    values

  private def permutation(count: Int): Array[Int] =
    val order = Array.tabulate(count)(identity)
    var state = 58L
    var index = count - 1
    while (index > 0) do
      state = (state * 1664525 + 1013904223) & 0xFFFFFFFFL
      val other = (state % (index + 1)).toInt
      val held = order(index)
      order(index) = order(other)
      order(other) = held
      index -= 1
    order
