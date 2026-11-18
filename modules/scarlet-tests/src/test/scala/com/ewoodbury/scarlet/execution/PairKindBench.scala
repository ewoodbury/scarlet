package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Same-session row and columnar times for the pair maps admitted after the int-pair project.
 *
 * `long` is `(Long, Long) => (Long, Long)` with `key % 100`, then `countByKey`. `string` keeps
 * the key and adds one to the value, then `countByKey`, so the dictionary is shared. `sort` swaps
 * an int pair and then `sortByIntKey`. The checksum sorts count rows by key. The sort checksum
 * keeps collect order. Spill is on. The row arm runs first.
 *
 * {{{
 * CROSS_ROWS=1000000 CROSS_THREADS=4 CROSS_WARMUP=6 CROSS_ROUNDS=11 \
 *   java -Xmx4g -cp "$CP" com.ewoodbury.scarlet.execution.PairKindBench
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object PairKindBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val allocRounds = 5

  private final case class Arm(
      kind: String,
      name: String,
      p50Ms: Double,
      checksum: Long,
      allocMb: Double,
      batches: Long,
  )

  def main(args: Array[String]): Unit =
    val rows = readPositive("CROSS_ROWS", 1000000)
    val threads = readPositive("CROSS_THREADS", 4)
    val warmup = readPositive("CROSS_WARMUP", 6)
    val rounds = readPositive("CROSS_ROUNDS", 11)
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
          spillEnabled = true,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val longRows = longCollection(rows)
      val stringRows = stringCollection(rows)
      val sorted = sortCollection(rows)
      measure("long", longRows.plan, () => longChecksum(DistCollection(longRows.plan).collect()))(
        warmup,
        rounds,
      )
      measure(
        "string",
        stringRows.plan,
        () => stringChecksum(DistCollection(stringRows.plan).collect()),
      )(warmup, rounds)
      measure("sort", sorted.plan, () => intChecksum(DistCollection(sorted.plan).collect()))(
        warmup,
        rounds,
      )
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private def measure(
      kind: String,
      plan: com.ewoodbury.scarlet.core.Plan[_],
      action: () => Long,
  )(warmup: Int, rounds: Int): Unit =
    val shape = ColumnarPlanner.lower(plan).map(_.shape).getOrElse(refuse(s"$kind did not lower"))
    println(s"kind $kind shape $shape")
    val row = time(kind, "row", action, warmup, rounds, columnar = false)
    val columnar = time(kind, "columnar", action, warmup, rounds, columnar = true)
    if (row.checksum != columnar.checksum) then
      refuse(s"$kind checksums differ row=${row.checksum} columnar=${columnar.checksum}")
    if (row.batches != 0L) then refuse(s"$kind row published batches=${row.batches}")
    if (columnar.batches == 0L) then refuse(s"$kind columnar published no column batch")
    println(render(row))
    println(render(columnar))
    val ratio = row.p50Ms / columnar.p50Ms
    println(s"$kind ratio ${ratio}")

  private def longCollection(rows: Int): DistCollection[(Long, Int)] =
    val keys = Array.tabulate(rows)(row => (row % groups).toLong)
    val values = Array.tabulate(rows)(row => row.toLong)
    DistCollection
      .ofLongPairs(keys, values, sourcePartitions)
      .map((pair: (Long, Long)) => (pair._1 % 100L, pair._2))
      .countByKey

  private def stringCollection(rows: Int): DistCollection[(String, Int)] =
    val words = Array.tabulate(groups)(slot => "k" + slot.toString)
    val data = Vector.tabulate(rows)(row => (words(row & (groups - 1)), row))
    DistCollection(data, sourcePartitions)
      .map((pair: (String, Int)) => (pair._1, pair._2 + 1))
      .countByKey

  private def sortCollection(rows: Int): DistCollection[(Int, Int)] =
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcgValues(rows)
    DistCollection
      .ofIntPairs(keys, values, sourcePartitions)
      .map((pair: (Int, Int)) => (pair._2, pair._1))
      .sortByIntKey

  private def time(
      kind: String,
      name: String,
      action: () => Long,
      warmup: Int,
      rounds: Int,
      columnar: Boolean,
  ): Arm =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksum = 0L
    var batches = 0L
    round = 0
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        checksum = action()
        samples(round) = (System.nanoTime() - started) / 1_000_000.0
        batches = sink.batchCount
      finally ColumnMetrics.restore(previous)
      round += 1
    java.util.Arrays.sort(samples)
    Arm(kind, name, samples(samples.length / 2), checksum, sampleAlloc(action), batches)

  private def longChecksum(rows: Iterable[(Long, Int)]): Long =
    val ordered = rows.iterator.toArray.sortBy(_._1)
    var total = 0L
    var index = 0
    while (index < ordered.length) do
      val row = ordered(index)
      total += row._1 * 31L + row._2.toLong
      index += 1
    total

  private def stringChecksum(rows: Iterable[(String, Int)]): Long =
    val ordered = rows.iterator.toArray.sortBy(_._1)
    var total = 0L
    var index = 0
    while (index < ordered.length) do
      val row = ordered(index)
      total += row._1.hashCode.toLong * 31L + row._2.toLong
      index += 1
    total

  private def intChecksum(rows: Iterable[(Int, Int)]): Long =
    rows.iterator.foldLeft(0L)((total, row) => total + row._1.toLong * 31L + row._2.toLong)

  private def lcgValues(count: Int): Array[Int] =
    val values = new Array[Int](count)
    var index = 0
    var state = seed
    while (index < count) do
      state = state * 1664525 + 1013904223
      values(index) = ((state & 0x7fffffff) % 2000001) - 1000000
      index += 1
    values

  private def sampleAlloc(action: () => Long): Double =
    startProbe() match
      case None => Double.NaN
      case Some(probe) =>
        try
          val samples = new Array[Double](allocRounds)
          var round = 0
          while (round < allocRounds) do
            val (_, bytes) = probe.account {
              ScarletRuntime.get.shuffle.clear()
              val _ = action()
            }
            samples(round) = bytes.toDouble / 1_000_000.0
            round += 1
          java.util.Arrays.sort(samples)
          samples(samples.length / 2)
        finally probe.stop()

  private def startProbe(): Option[AllocProbe] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
    }

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  private def render(arm: Arm): String =
    s"${arm.kind} ${arm.name} p50_ms=${arm.p50Ms} checksum=${arm.checksum} " +
      s"alloc_mb=${arm.allocMb} batches=${arm.batches}"

  private def readPositive(name: String, default: Int): Int =
    val value = sys.env.get(name).flatMap(_.toIntOption).getOrElse(default)
    if (value > 0) then value else refuse(s"$name must be positive, found $value")

  private def refuse(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)
