package com.ewoodbury.scarlet.execution

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session pair for `broadcastJoin.sumJoinLeft` on the `joinAggQuery` inputs.
 *
 * Not part of `make test`. The left table is `CROSS_ROWS` pairs, keys `row % 1024`, the shared LCG
 * values. The right table is `10 * 1024` pairs, keys `row % 1024`, values `row % 100`. Two
 * filters. The action is `filteredLeft.broadcastJoin(filteredRight).sumJoinLeft`.
 *
 * {{{
 * CROSS_ROWS=1000000 CROSS_THREADS=4 CROSS_WARMUP=10 CROSS_ROUNDS=15 \
 *   sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.BroadcastSumBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
object BroadcastSumBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val sourcePartitions = 2

  def main(args: Array[String]): Unit =
    val rows = readInt("CROSS_ROWS", 1000000)
    val threads = readInt("CROSS_THREADS", 4)
    val warmup = readInt("CROSS_WARMUP", 8)
    val rounds = readInt("CROSS_ROUNDS", 11)
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = threads,
          columnarExecution = true,
          columnarBatchSize = 8192,
          defaultShufflePartitions = 4,
          spillEnabled = true,
          traceLowering = false,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val aggregated = plan(rows)
      val shape = ColumnarPlanner.lower(aggregated.plan).map(_.shape).getOrElse("row")
      println(s"ARM $shape")
      val columnar = time(aggregated, warmup, rounds, columnar = true)
      val row = time(aggregated, warmup, rounds, columnar = false)
      println(
        s"COLUMNAR p50_ms=${columnar.p50} checksum=${columnar.checksum} rounds=$rounds",
      )
      println(s"ROW p50_ms=${row.p50} checksum=${row.checksum} rounds=$rounds")
      val ratio = row.p50 / columnar.p50
      println(s"RATIO row/columnar=$ratio")
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private final case class Timed(p50: Double, checksum: Long)

  private def plan(rows: Int): DistCollection[(Int, Long)] =
    val width = 10 * groups
    val joinKeys = Array.tabulate(width)(row => row % groups)
    val joinValues = Array.tabulate(width)(row => row % 100)
    val left = DistCollection.ofIntPairs(rowKeys(rows), lcgValues(rows), sourcePartitions)
    val right = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val filteredLeft = left
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val filteredRight = right
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value < 50)
    filteredLeft.broadcastJoin(filteredRight).sumJoinLeft

  private def time(
      aggregated: DistCollection[(Int, Long)],
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Timed =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = columnar))
      var round = 0
      while (round < warmup) do
        ScarletRuntime.get.shuffle.clear()
        val _ = CrossEngineShapes.sumChecksum(aggregated.collect())
        round += 1
      val samples = new Array[Double](rounds)
      var checksum = 0L
      round = 0
      while (round < rounds) do
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        val value = CrossEngineShapes.sumChecksum(aggregated.collect())
        samples(round) = (System.nanoTime() - started) / 1000000.0
        checksum = value
        round += 1
      java.util.Arrays.sort(samples)
      Timed(samples(samples.length / 2), checksum)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def rowKeys(rows: Int): Array[Int] =
    Array.tabulate(rows)(row => row % groups)

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

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) if value > 0 => value
          case _ => throw new IllegalArgumentException(s"$name must be a positive integer")
