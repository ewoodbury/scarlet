package com.ewoodbury.scarlet.execution

import java.util.{Arrays, Locale}

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Simulates a parent that drops an int-to-long `mapValues`.
 *
 * `row` runs the whole plan with columnar execution off. `kept` decodes that widening child with
 * [[ColumnarPlanner.tryRun]] and applies the parent on those rows. The child runs once. Checksums
 * are outside the timer. Three warmup collects are discarded and nine are measured. The median is
 * sorted index 4.
 *
 * `mapIntKey` accepts only `(Int, Int)`, so `key-above` is the pair `map` that adds 1 to the key
 * of an `(Int, Long)`. A ratio above 2 is `GAP`. A checksum mismatch is `FAIL` and exits before
 * any sample.
 *
 * {{{
 * sbt -J-Xmx3g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.FoundationParents"
 * }}}
 */
object FoundationParents:

  private val largeRows = 1000000
  private val warmup = 3
  private val measure = 9
  private val p50Index = 4
  private val names: Vector[String] = Vector(
    "filter-above",
    "count-above",
    "key-above",
    "reduce-other",
  )

  /**
   * Checksums at `rows`. `count-above`, `filter-above`, and `reduce-other` lower. `key-above` must
   * not. A checksum mismatch throws.
   */
  def check(rows: Int): Unit =
    observe(rows).foreach { one =>
      if !one.matched then
        throw new IllegalStateException(
          s"${one.name} checksums row=${one.rowChecksum} kept=${one.keptChecksum}",
        )
    }

  /** `OK` at a ratio of 2 or below. Above 2 is `GAP`. */
  def judge(ratio: Double): String =
    if ratio > 2.0 then "GAP" else "OK"

  /** `1` when any verdict is `FAIL`. `GAP` stays `0`. */
  def exitCode(verdicts: Seq[String]): Int =
    if verdicts.contains("FAIL") then 1 else 0

  def main(args: Array[String]): Unit =
    if args.nonEmpty then
      System.err.println("usage: FoundationParents")
      System.exit(1)
    val found = observe(largeRows)
    if found.exists(one => !one.matched) then
      found.foreach(one => println(untimed(one)))
      System.exit(exitCode(found.map(one => if one.matched then "OK" else "FAIL")))
    val lines = names.map(timeOne)
    lines.foreach(line => println(line))
    val code = exitCode(lines.map(verdictOf))
    if code != 0 then System.exit(code)

  private final case class Observed(name: String, rowChecksum: Long, keptChecksum: Long):
    def matched: Boolean = keptChecksum == rowChecksum

  private def observe(rows: Int): Vector[Observed] =
    names.map(name => observeOne(name, rows))

  private def observeOne(name: String, rows: Int): Observed =
    name match
      case "filter-above" =>
        val data = filtered(rows)
        if ColumnarPlanner.lower(data.plan).isEmpty then
          throw new IllegalStateException(s"$name stayed on rows")
        val lowered = ColumnarPlanner
          .tryRun(data.plan)
          .getOrElse(throw new IllegalStateException(s"$name did not run columnar"))
        val kept = lowered.iterator.flatMap(_.data.iterator).toSeq
        watched(name, sumOrdered(collect(data)), sumOrdered(kept))
      case "count-above" =>
        val data = counted(rows)
        if ColumnarPlanner.lower(data.plan).isEmpty then
          throw new IllegalStateException(s"$name stayed on rows")
        watched(name, sumCounts(collect(data)), sumCounts(keptCount(rows)))
      case "key-above" =>
        val data = keyed(rows)
        refuse(name, data.plan)
        watched(name, sumOrdered(collect(data)), sumOrdered(keptKey(rows)))
      case "reduce-other" =>
        val data = reduced(rows)
        if ColumnarPlanner.lower(data.plan).isEmpty then
          throw new IllegalStateException(s"$name stayed on rows")
        val lowered = ColumnarPlanner
          .tryRun(data.plan)
          .getOrElse(throw new IllegalStateException(s"$name did not run columnar"))
        val kept = lowered.iterator.flatMap(_.data.iterator).toSeq
        watched(name, sumReduced(collect(data)), sumReduced(kept))
      case other =>
        throw new IllegalStateException(s"unknown query $other")

  private def watched(name: String, row: Long, kept: Long): Observed =
    Observed(name, row, kept)

  /** The whole parent does not run columnar. The widening child does lower. */
  private def refuse(name: String, plan: Plan[?]): Unit =
    if ColumnarPlanner.lower(plan).isDefined then throw new IllegalStateException(s"$name lowered")
    if ColumnarPlanner.tryRun(plan).isDefined then
      throw new IllegalStateException(s"$name ran columnar")

  private def timeOne(name: String): String =
    val rowNanos = p50Nanos(elapsedRow(name))
    if rowNanos == 0L then
      System.err.println(s"row p50 is 0 for $name")
      line(name, "0.00", "0.00", "na", "FAIL")
    else
      val keptNanos = p50Nanos(elapsedKept(name))
      val ratio = keptNanos.toDouble / rowNanos.toDouble
      line(name, ms(rowNanos), ms(keptNanos), msRatio(ratio), judge(ratio))

  private def elapsedRow(name: String): Long =
    name match
      case "filter-above" => elapsedCollect(filtered(largeRows))
      case "count-above" => elapsedCollect(counted(largeRows))
      case "key-above" => elapsedCollect(keyed(largeRows))
      case "reduce-other" => elapsedCollect(reduced(largeRows))
      case other => throw new IllegalStateException(s"unknown query $other")

  private def elapsedKept(name: String): Long =
    name match
      case "filter-above" => elapsedLowered(filtered(largeRows))
      case "count-above" => elapsedWork(keptCount(largeRows).size)
      case "key-above" => elapsedWork(keptKey(largeRows).size)
      case "reduce-other" => elapsedLowered(reduced(largeRows))
      case other => throw new IllegalStateException(s"unknown query $other")

  private def keptCount(rows: Int): Seq[(Int, Int)] =
    scheduled(Plan.CountByKeyOp(frozen(rows)))

  private def keptKey(rows: Int): Seq[(Int, Long)] =
    scheduled(Plan.MapOp(frozen(rows), (pair: (Int, Long)) => (pair._1 + 1, pair._2)))

  private def frozen(rows: Int): Plan[(Int, Long)] =
    val parts = decoded(widened(rows).plan).map(part => Partition(part))
    Plan.Source(parts)

  private def decoded[A](plan: Plan[A]): Seq[Vector[A]] =
    val parts = ColumnarPlanner
      .tryRun(plan)
      .getOrElse(throw new IllegalStateException("widening child did not decode"))
    parts.map(part => part.data.iterator.toVector)

  private def scheduled[A](plan: Plan[A]): Seq[A] =
    val runtime = ScarletRuntime.get
    val scheduler = new DAGScheduler(runtime.shuffle, runtime.scheduler, runtime.partitioner)
    scheduler.executePartitions(plan).iterator.flatMap(_.data.iterator).toSeq

  private def filtered(rows: Int): DistCollection[(Int, Long)] =
    widened(rows).filterValues[Int, Long]((value: Long) => value > 0L)

  private def counted(rows: Int): DistCollection[(Int, Int)] =
    widened(rows).countByKey[Int, Long]

  private def keyed(rows: Int): DistCollection[(Int, Long)] =
    widened(rows).map((pair: (Int, Long)) => (pair._1 + 1, pair._2))

  private def reduced(rows: Int): DistCollection[(Int, Long)] =
    widened(rows).reduceByKey[Int, Long]((left: Long, _: Long) => left)

  private def widened(rows: Int): DistCollection[(Int, Long)] =
    val data = intPairs(rows).mapValues[Int, Int, Long]((value: Int) => value.toLong)
    if ColumnarPlanner.lower(data.plan).isEmpty then
      throw new IllegalStateException("widening child did not lower")
    data

  private def collect[A](data: DistCollection[A]): Seq[A] =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      data.collect().toSeq
    finally ScarletConf.set(previous)

  private def p50Nanos(sample: => Long): Long =
    (0 until warmup).foreach(_ => discard(sample))
    val samples = Array.fill(measure)(sample)
    Arrays.sort(samples)
    samples(p50Index)

  private def discard(sample: Long): Unit =
    if sample < 0L then throw new IllegalStateException("negative sample")

  private def elapsedLowered[A](data: DistCollection[A]): Long =
    val started = System.nanoTime()
    val lowered = ColumnarPlanner
      .tryRun(data.plan)
      .getOrElse(throw new IllegalStateException("parent did not run columnar"))
    val taken = System.nanoTime() - started
    if lowered.iterator.flatMap(_.data.iterator).size < 0 then
      throw new IllegalStateException("negative collect")
    taken

  private def elapsedCollect[A](data: DistCollection[A]): Long =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      val size = data.collect().size
      val taken = System.nanoTime() - started
      if size < 0 then throw new IllegalStateException("negative collect")
      taken
    finally ScarletConf.set(previous)

  private def elapsedWork(size: => Int): Long =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = true))
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      val counted = size
      val taken = System.nanoTime() - started
      if counted < 0 then throw new IllegalStateException("negative collect")
      taken
    finally ScarletConf.set(previous)

  private def intPairs(rows: Int): DistCollection[(Int, Int)] =
    val keys = Array.tabulate(rows)(i => i % 1024)
    val values = Array.tabulate(rows)(i => i + 1)
    DistCollection.ofIntPairs(keys, values, 2)

  private def sumOrdered(rows: Seq[(Int, Long)]): Long =
    rows.iterator.zipWithIndex.foldLeft(0L) { (acc, item) =>
      acc + item._1._1.toLong * 31L + item._1._2 + item._2.toLong
    }

  private def sumCounts(rows: Seq[(Int, Int)]): Long =
    rows.sortBy(_._1).foldLeft(0L)((acc, row) => acc + row._1.toLong * 31L + row._2.toLong)

  private def sumReduced(rows: Seq[(Int, Long)]): Long =
    rows.sortBy(_._1).foldLeft(0L)((acc, row) => acc + row._1.toLong * 31L + row._2)

  private def ms(nanos: Long): String =
    String.format(Locale.US, "%.2f", Double.box(nanos.toDouble / 1000000.0))

  private def msRatio(ratio: Double): String =
    String.format(Locale.US, "%.2f", Double.box(ratio))

  private def untimed(one: Observed): String =
    line(one.name, "0.00", "0.00", "0.00", if one.matched then "OK" else "FAIL")

  private def line(
      name: String,
      rowMs: String,
      keptMs: String,
      ratio: String,
      verdict: String,
  ): String =
    s"PARENT query=$name rows=$largeRows row_p50_ms=$rowMs kept_p50_ms=$keptMs " +
      s"kept_ratio=$ratio verdict=$verdict"

  private def verdictOf(text: String): String =
    text.split("verdict=").lift(1).getOrElse("FAIL")
