package com.ewoodbury.scarlet.execution

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * One-off cross-engine comparison harness. Not a test, and not part of `make test`.
 *
 * Generates the same 1e6-row LCG dataset as `cross_engine_duckdb.py`, runs the same six queries,
 * and prints one JSON object with p50 milliseconds and a checksum per query. The checksums are
 * the ones the Python harness computes, so a mismatch is a semantic difference, not a timing
 * difference.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object CrossEngineBench:

  private val rows = 1_000_000
  private val groups = 1024
  private val seed = 0x12345678
  private val shuffleSeed = 58L
  private val threads = 4
  private val batchSize = 8192
  private val warmup = 3
  private val rounds = 10

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = threads,
          columnarExecution = true,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = 4,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      run()
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private def run(): Unit =
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    val sortKeys = permutation(rows)
    val joinRows = 10 * groups
    val joinKeys = Array.tabulate(joinRows)(row => row % groups)
    val joinValues = Array.tabulate(joinRows)(row => row % 100)

    val pairs = DistCollection.ofIntPairs(keys, values, 2)
    val filtered = pairs
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val unique = DistCollection.ofIntPairs(joinKeys, joinValues, 2)
    val sorted = DistCollection
      .ofIntPairs(sortKeys, values, 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .sortByIntKey

    val results = List(
      measure("filter-count", () => filtered.count()),
      measure("filter-sum", () => filtered.values.sum),
      measure(
        "filter-groupbykey",
        () => filtered.groupByKey.collect().map { case (key, vs) => key * 31L + vs.size }.sum,
      ),
      measure(
        "filter-reducebykey",
        () =>
          filtered
            .reduceByKey[Int, Int](_ + _)
            .collect()
            .map { case (key, value) => key * 31L + value }
            .sum,
      ),
      measure("filter-join", () => filtered.join(unique).count()),
      measure(
        "filter-sort",
        () => sorted.collect().map { case (key, value) => key * 31L + value }.sum,
      ),
    )

    val body = results
      .map { case (name, ms, checksum) =>
        s"""    "$name": {"p50_ms": $ms, "checksum": $checksum}"""
      }
      .mkString(",\n")
    println(s"""{
  "engine": "scarlet",
  "rows": $rows,
  "threads": $threads,
  "warmup": $warmup,
  "rounds": $rounds,
  "results": {
$body
  }
}""")

  private def measure(name: String, action: () => Any): (String, Double, Long) =
    var round = 0
    while (round < warmup) do
      action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksum = 0L
    round = 0
    while (round < rounds) do
      val start = System.nanoTime()
      val value = action()
      val elapsed = (System.nanoTime() - start) / 1_000_000.0
      samples(round) = elapsed
      checksum = checksumOf(value)
      round += 1
    java.util.Arrays.sort(samples)
    val p50 = samples(samples.length / 2)
    (name, p50, checksum)

  private def checksumOf(value: Any): Long = value match
    case number: Long => number
    case number: Int => number.toLong
    case other => throw new IllegalStateException(s"unexpected checksum $other")

  private def lcgValues(count: Int): Array[Int] =
    val values = new Array[Int](count)
    var index = 0
    var state = seed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    values

  private def permutation(count: Int): Array[Int] =
    val order = Array.tabulate(count)(identity)
    var state = shuffleSeed
    var index = count - 1
    while (index > 0) do
      state = (state * 1664525 + 1013904223) & 0xFFFFFFFFL
      val other = (state % (index + 1)).toInt
      val held = order(index)
      order(index) = order(other)
      order(other) = held
      index -= 1
    order
