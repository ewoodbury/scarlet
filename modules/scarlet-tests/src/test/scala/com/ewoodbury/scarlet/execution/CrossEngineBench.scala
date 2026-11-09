package com.ewoodbury.scarlet.execution

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch, ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * One-off cross-engine comparison harness. Not a test, and not part of `make test`.
 *
 * Each query builds its own input and drops it before the next query starts. `encode_ms` times one
 * encode of that input before warmup and is not subtracted from the action. The action timer still
 * includes the scan encode. A measured action that publishes no column batch fails the process.
 * The row path is not run here.
 *
 * Owned int arrays use [[BatchCodec.intPairs]]. Owned long arrays use
 * [[BatchCodec.copiedLongPairs]], the scan's array encode. A null long value is timed with
 * [[BatchCodec.longPairs]], the boxed scan encoder. A null int value is timed with
 * [[BatchCodec.intPairsPreservingAbsence]], and the boxed int-pair scan uses that same encoder.
 * [[BatchCodec.nullableIntPairs]] takes `Option` pairs and is not that path. `groupByKey` still
 * refuses a null value.
 *
 * Spill is enabled for this process only. The `spillEnabled` default stays false, and the spill
 * budget stays the 64 MiB default.
 *
 * {{{
 * CROSS_ROWS=10000000 CROSS_QUERY=filter-count \
 *   sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object CrossEngineBench:

  private val groups = 1024
  private val seed = 0x12345678
  private val shuffleSeed = 58L
  private val threads = 4
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val nullStride = 4096
  private val queryNames = Vector(
    "filter-count",
    "filter-sum",
    "filter-groupbykey",
    "filter-reducebykey",
    "filter-join",
    "filter-sort",
  )

  private final case class Settings(
      rows: Int,
      filters: Int,
      nulls: Boolean,
      query: Option[String],
      warmup: Int,
      rounds: Int,
  )

  private final case class Built(action: () => Any, encodeMs: Double)

  private final case class Sample(
      p50Ms: Double,
      checksum: Long,
      encodeMs: Double,
      batches: Long,
  )

  private final case class Observed(value: Any, elapsedMs: Double, batches: Long)

  /** Printed before throw so `finally` can restore conf, then the process exits. */
  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  def main(args: Array[String]): Unit =
    val settings = readSettings()
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    var status = 0
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
      println(render(settings, run(settings)))
    catch case _: HarnessRefusal => status = 1
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def run(settings: Settings): Vector[(String, Sample)] =
    val names = settings.query match
      case Some(name) => Vector(name)
      case None => queryNames
    names.map { name =>
      name -> runQuery(name, settings)
    }

  private def runQuery(name: String, settings: Settings): Sample =
    val built = build(name, settings)
    measure(name, built, settings.warmup, settings.rounds)

  /** The action closes over the distributed collection, not the source arrays. */
  private def build(name: String, settings: Settings): Built =
    val rows = settings.rows
    val filters = settings.filters
    val nulls = settings.nulls
    name match
      case "filter-count" =>
        withInts(rows, filters, nulls)(_.count())
      case "filter-sum" =>
        withInts(rows, filters, nulls)(_.values.sum)
      case "filter-groupbykey" =>
        withInts(rows, filters, nulls) { data =>
          data.groupByKey.collect().map { case (key, vs) => key * 31L + vs.size }.sum
        }
      case "filter-reducebykey" =>
        withLongs(rows, filters, nulls) { data =>
          data.reduceByKey[Long, Long](_ + _).collect().map { case (key, value) =>
            key * 31L + value
          }.sum
        }
      case "filter-join" =>
        joinQuery(rows, filters, nulls)
      case "filter-sort" =>
        sortQuery(rows, filters, nulls)
      case other =>
        refuse(s"unknown query $other")

  private def withInts(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  )(action: DistCollection[(Int, Int)] => Any): Built =
    val (data, encodeMs) = filteredInts(rows, filters, nulls)
    Built(() => action(data), encodeMs)

  private def withLongs(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  )(action: DistCollection[(Long, Long)] => Any): Built =
    val (data, encodeMs) = filteredLongs(rows, filters, nulls)
    Built(() => action(data), encodeMs)

  private def filteredInts(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  ): (DistCollection[(Int, Int)], Double) =
    if (nulls) then
      val pairs = nullableInts(rows, rowKeys(rows))
      val encodeMs = timed(BatchCodec.intPairsPreservingAbsence(pairs))
      (filterInts(DistCollection(pairs, sourcePartitions), filters), encodeMs)
    else
      val keys = rowKeys(rows)
      val values = lcgValues(rows)
      val encodeMs = timed(BatchCodec.intPairs(keys, values))
      val data = DistCollection.ofIntPairs(keys, values, sourcePartitions)
      (filterInts(data, filters), encodeMs)

  private def filteredLongs(
      rows: Int,
      filters: Int,
      nulls: Boolean,
  ): (DistCollection[(Long, Long)], Double) =
    if (nulls) then
      val pairs = nullableLongs(rows)
      val encodeMs = timed(BatchCodec.longPairs(pairs))
      (filterLongs(DistCollection(pairs, sourcePartitions), filters), encodeMs)
    else
      val keys = longKeys(rows)
      val values = lcgLongs(rows)
      val encodeMs = timed(BatchCodec.copiedLongPairs(keys, values))
      val data = DistCollection.ofLongPairs(keys, values, sourcePartitions)
      (filterLongs(data, filters), encodeMs)

  private def joinQuery(rows: Int, filters: Int, nulls: Boolean): Built =
    val width = 10 * groups
    val joinKeys = Array.tabulate(width)(row => row % groups)
    val joinValues = Array.tabulate(width)(row => row % 100)
    val side = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    val (filtered, encodeMs) =
      if (nulls) then
        val pairs = nullableInts(rows, rowKeys(rows))
        val elapsed = timedBoth(
          BatchCodec.intPairsPreservingAbsence(pairs),
          BatchCodec.intPairs(joinKeys, joinValues),
        )
        val data = DistCollection(pairs, sourcePartitions)
        (filterInts(data, filters), elapsed)
      else
        val keys = rowKeys(rows)
        val values = lcgValues(rows)
        val elapsed = timedBoth(
          BatchCodec.intPairs(keys, values),
          BatchCodec.intPairs(joinKeys, joinValues),
        )
        val data = DistCollection.ofIntPairs(keys, values, sourcePartitions)
        (filterInts(data, filters), elapsed)
    val joined = filtered.join(side)
    Built(() => joined.count(), encodeMs)

  private def sortQuery(rows: Int, filters: Int, nulls: Boolean): Built =
    val order = permutation(rows)
    val (sorted, encodeMs) =
      if (nulls) then
        val pairs = nullableInts(rows, order)
        val elapsed = timed(BatchCodec.intPairsPreservingAbsence(pairs))
        val data = DistCollection(pairs, sourcePartitions)
        (filterInts(data, filters).sortByIntKey, elapsed)
      else
        val values = lcgValues(rows)
        val elapsed = timed(BatchCodec.intPairs(order, values))
        val data = DistCollection.ofIntPairs(order, values, sourcePartitions)
        (filterInts(data, filters).sortByIntKey, elapsed)
    Built(() => sorted.collect().map { case (key, value) => key * 31L + value }.sum, encodeMs)

  private def filterInts(
      data: DistCollection[(Int, Int)],
      filters: Int,
  ): DistCollection[(Int, Int)] =
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    if (filters == 1) then keyed
    else keyed.filterValues((value: Int) => value >= 0)

  private def filterLongs(
      data: DistCollection[(Long, Long)],
      filters: Int,
  ): DistCollection[(Long, Long)] =
    val keyed = data.filterKeys((key: Long) => key % 3 != 0)
    if (filters == 1) then keyed
    else keyed.filterValues((value: Long) => value >= 0)

  private def measure(name: String, built: Built, warmup: Int, rounds: Int): Sample =
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = built.action()
      round += 1
    val samples = new Array[Double](rounds)
    var checksum = 0L
    var batches = 0L
    round = 0
    while (round < rounds) do
      val observed = observe(built.action)
      if (observed.batches == 0L) then refuse(s"$name published no column batch")
      samples(round) = observed.elapsedMs
      checksum = checksumOf(observed.value)
      batches = observed.batches
      round += 1
    java.util.Arrays.sort(samples)
    Sample(samples(samples.length / 2), checksum, built.encodeMs, batches)

  private def observe(action: () => Any): Observed =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      val value = action()
      val elapsedMs = (System.nanoTime() - started) / 1_000_000.0
      Observed(value, elapsedMs, sink.batchCount)
    finally ColumnMetrics.restore(previous)

  private def checksumOf(value: Any): Long = value match
    case number: Long => number
    case number: Int => number.toLong
    case other => throw new IllegalStateException(s"unexpected checksum $other")

  private def timed(encode: => ColumnBatch): Double =
    val started = System.nanoTime()
    val batch = encode
    val elapsed = (System.nanoTime() - started) / 1_000_000.0
    val _ = batch.length
    elapsed

  private def timedBoth(left: => ColumnBatch, right: => ColumnBatch): Double =
    val started = System.nanoTime()
    val first = left
    val second = right
    val elapsed = (System.nanoTime() - started) / 1_000_000.0
    val _ = first.length + second.length
    elapsed

  private def rowKeys(rows: Int): Array[Int] =
    Array.tabulate(rows)(row => row % groups)

  private def longKeys(rows: Int): Array[Long] =
    Array.tabulate(rows)(row => (row % groups).toLong)

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

  private def lcgLongs(count: Int): Array[Long] =
    val values = lcgValues(count)
    Array.tabulate(count)(index => values(index).toLong)

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

  /**
   * Value is null when `index` is 1 mod 4096. Row 0 stays a present pair so the source classifies.
   * A dense key is `index % 1024`, so that null row's key is 1 and the key filter keeps it. Sort
   * keys are the permutation, so a sort null stays only when the permuted key is not divisible by
   * 3. The key itself stays present.
   */
  private def nullableInts(rows: Int, keys: Array[Int]): IndexedSeq[(Int, Int)] =
    val values = lcgValues(rows)
    IndexedSeq.tabulate(rows) { index =>
      if (index % nullStride == 1) then intPair(boxedInt(keys(index)), absentInt)
      else keys(index) -> values(index)
    }

  private def nullableLongs(rows: Int): IndexedSeq[(Long, Long)] =
    val keys = longKeys(rows)
    val values = lcgLongs(rows)
    IndexedSeq.tabulate(rows) { index =>
      if (index % nullStride == 1) then longPair(boxedLong(keys(index)), absentLong)
      else keys(index) -> values(index)
    }

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  /** Named erasure boundary. `Tuple2` keeps a null `Long`; `(Long, Long)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentLong: java.lang.Long = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def boxedLong(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  private def render(settings: Settings, results: Vector[(String, Sample)]): String =
    val body = results.map { case (name, sample) => queryJson(name, sample) }.mkString(",\n")
    s"""{
  "engine": "scarlet",
  "rows": ${settings.rows},
  "threads": $threads,
  "warmup": ${settings.warmup},
  "rounds": ${settings.rounds},
  "filters": ${settings.filters},
  "nulls": ${settings.nulls},
  "spill": true,
  "results": {
$body
  }
}"""

  private def queryJson(name: String, sample: Sample): String =
    val head = s"""    "$name": {"p50_ms": ${sample.p50Ms}, "checksum": ${sample.checksum},"""
    val tail = s""" "encode_ms": ${sample.encodeMs}, "batches": ${sample.batches}}"""
    head + tail

  private def readSettings(): Settings =
    Settings(
      rows = readInt("CROSS_ROWS", 1_000_000),
      filters = readChoice("CROSS_FILTERS", 2, Set(1, 2)),
      nulls = readFlag("CROSS_NULLS", false),
      query = readQuery(),
      warmup = readInt("CROSS_WARMUP", 3),
      rounds = readInt("CROSS_ROUNDS", 10),
    )

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) => value
          case None => envError(s"$name must be an integer, found $text")

  private def readChoice(name: String, default: Int, allowed: Set[Int]): Int =
    val value = readInt(name, default)
    if (allowed.contains(value)) then value
    else envError(s"$name must be ${allowed.mkString(" or ")}, found $value")

  private def readFlag(name: String, default: Boolean): Boolean =
    sys.env.get(name) match
      case None => default
      case Some("true") => true
      case Some("false") => false
      case Some(other) => envError(s"$name must be true or false, found $other")

  private def readQuery(): Option[String] =
    sys.env.get("CROSS_QUERY") match
      case None => None
      case Some(name) if queryNames.contains(name) => Some(name)
      case Some(name) =>
        envError(s"CROSS_QUERY must name one of ${queryNames.mkString(", ")}, found $name")

  private def envError(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)

  private def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)
