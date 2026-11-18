package com.ewoodbury.scarlet.execution

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session timing of a filtered int join, a generic `(Int, Long)` map, and `reduceByKey`
 * addition. Not `sumJoinLeft`. Not part of `make test`.
 *
 * 1e6 left rows, warmup 80, rounds 30, four threads. The checksum is
 * [[CrossEngineShapes.sumChecksum]].
 *
 * {{{
 * sbt -batch -J-Xmx4g \
 *   "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.GenericJoinMapBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object GenericJoinMapBench:

  private val rows = 1000000
  private val groups = 1024
  private val seed = 0x12345678
  private val warmup = 80
  private val rounds = 30
  private val threads = 4

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    try
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val action = build()
      val row = time(action, columnar = false)
      val column = time(action, columnar = true)
      val op = ColumnarPlanner.lower(action.plan)
      val shape = op.map(_.shape).getOrElse("row")
      val ratio = row.p50 / column.p50
      println(
        s"row_p50_ms=${fmt(row.p50)} col_p50_ms=${fmt(column.p50)} ratio=${fmt(ratio)} " +
          s"row_checksum=${row.checksum} col_checksum=${column.checksum} " +
          s"row_steady_ms=${fmt(row.steady)} col_steady_ms=${fmt(column.steady)} arm=$shape",
      )
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private final class Timed(val p50: Double, val steady: Double, val checksum: Long)

  private def build(): DistCollection[(Int, Long)] =
    val width = 10 * groups
    val rightKeys = Array.tabulate(width)(row => row % groups)
    val rightValues = Array.tabulate(width)(row => row % 100)
    val leftKeys = Array.tabulate(rows)(row => row % groups)
    val leftValues = lcg(rows)
    val left = DistCollection.ofIntPairs(leftKeys, leftValues, 2)
    val right = DistCollection.ofIntPairs(rightKeys, rightValues, 2)
    val filteredLeft = left
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val filteredRight = right
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value < 50)
    filteredLeft
      .join(filteredRight)
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .reduceByKey((left: Long, right: Long) => left + right)

  private def time(action: DistCollection[(Int, Long)], columnar: Boolean): Timed =
    val saved = ScarletConf.get
    ScarletConf.set(
      saved.copy(
        threadPoolSize = threads,
        columnarExecution = columnar,
        columnarBatchSize = 8192,
        defaultShufflePartitions = 4,
        spillEnabled = true,
      ),
    )
    try
      var round = 0
      while (round < warmup) {
        ScarletRuntime.get.shuffle.clear()
        val _ = CrossEngineShapes.sumChecksum(action.collect())
        round += 1
      }
      val samples = new Array[Double](rounds)
      var checksum = 0L
      round = 0
      while (round < rounds) {
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        checksum = CrossEngineShapes.sumChecksum(action.collect())
        samples(round) = (System.nanoTime() - started) / 1_000_000.0
        round += 1
      }
      new Timed(median(samples, 0, rounds), median(samples, rounds - 10, rounds), checksum)
    finally ScarletConf.set(saved)

  /** Upper median of `[from, until)`, the same slot as [[CrossEngineBench]] p50. */
  private def median(samples: Array[Double], from: Int, until: Int): Double =
    val copy = java.util.Arrays.copyOfRange(samples, from, until)
    java.util.Arrays.sort(copy)
    copy(copy.length / 2)

  private def lcg(count: Int): Array[Int] =
    val values = new Array[Int](count)
    var index = 0
    var state = seed
    while (index < count) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  private def fmt(value: Double): String =
    val scaled = math.round(value * 1000.0)
    val whole = scaled / 1000
    val frac = math.abs((scaled % 1000).toInt)
    val digits = frac.toString
    s"$whole.${"000".substring(digits.length)}$digits"
