package com.ewoodbury.scarlet.execution

import java.util.{Arrays, Locale}

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Simulates a boundary that decodes its child once.
 *
 * `boundary` is today's collect. `row` is the same plan with columnar execution off. `once`
 * decodes the child with [[ColumnarPlanner.tryRun]] and applies the parent function, with no
 * second child run and no cell compare. Checksums are outside the timer. Three warmup collects are
 * discarded and nine are measured. The median is sorted index 4.
 *
 * `to-string` and `flat-map` decide the next section. `once` within twice the row median is `OK`.
 * Above that is `GAP`. A checksum mismatch is `FAIL` and exits before any sample.
 *
 * {{{
 * sbt -J-Xmx3g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.FoundationSplit"
 * }}}
 */
object FoundationSplit:

  private val largeRows = 1000000
  private val warmup = 3
  private val measure = 9
  private val p50Index = 4
  private val names: Vector[String] = Vector(
    "to-string",
    "flat-map",
    "element-widen",
    "flat-map-values",
    "sort-by",
    "cogroup",
  )

  /** Checksums at `rows`. Throws when `once` or `boundary` disagrees with `row`. Does not time. */
  def check(rows: Int): Unit =
    observe(rows).foreach { one =>
      if !one.matched then
        throw new IllegalStateException(
          s"${one.name} checksums boundary=${one.boundaryChecksum} row=${one.rowChecksum} " +
            s"once=${one.onceChecksum}",
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
      System.err.println("usage: FoundationSplit")
      System.exit(1)
    val found = observe(largeRows)
    if found.exists(one => !one.matched) then
      found.foreach(one => println(untimed(one)))
      System.exit(exitCode(found.map(one => if one.matched then "OK" else "FAIL")))
    val lines = names.map(timeOne)
    lines.foreach(line => println(line))
    val code = exitCode(lines.map(verdictOf))
    if code != 0 then System.exit(code)

  private final case class Observed(
      name: String,
      boundaryChecksum: Long,
      rowChecksum: Long,
      onceChecksum: Long,
  ):
    def matched: Boolean =
      boundaryChecksum == rowChecksum && onceChecksum == rowChecksum

  private def observe(rows: Int): Vector[Observed] =
    names.map(name => observeOne(name, rows))

  private def observeOne(name: String, rows: Int): Observed =
    name match
      case "to-string" =>
        val child = intPairs(rows)
        val data = child.mapValues[Int, Int, String](value => value.toString)
        watched(
          name,
          sumPairsString(collect(columnar = true, data)),
          sumPairsString(collect(columnar = false, data)),
          sumPairsString(withColumnar(onceToString(child))),
        )
      case "flat-map" =>
        val child = ints(rows).filter(value => value % 2 == 0).map(value => value * 2)
        val data = child.flatMap(value => Seq(value, value + 1))
        watched(
          name,
          sumInts(collect(columnar = true, data)),
          sumInts(collect(columnar = false, data)),
          sumInts(withColumnar(onceFlatMap(child))),
        )
      case "element-widen" =>
        val child = ints(rows)
        val data = child.map(value => value.toLong)
        if ColumnarPlanner.lower(data.plan).isDefined then
          throw new IllegalStateException("element-widen lowered")
        watched(
          name,
          sumLongs(collect(columnar = true, data)),
          sumLongs(collect(columnar = false, data)),
          sumLongs(withColumnar(onceWiden(child))),
        )
      case "flat-map-values" =>
        val child = intPairs(rows)
        val data = child.flatMapValues[Int, Int, Int](value => Seq(value, value + 1))
        watched(
          name,
          sumPairsInt(collect(columnar = true, data)),
          sumPairsInt(collect(columnar = false, data)),
          sumPairsInt(withColumnar(onceFlatMapValues(child))),
        )
      case "sort-by" =>
        val child = ints(rows)
        val key = (value: Int) => value
        val data = child.sortBy(key)
        watched(
          name,
          sumOrderedInts(collect(columnar = true, data)),
          sumOrderedInts(collect(columnar = false, data)),
          sumOrderedInts(withColumnar(onceSorted(child, key))),
        )
      case "cogroup" =>
        val left = intPairs(rows)
        val right = intPairs(rows)
        val data = left.cogroup(right)
        watched(
          name,
          sumGroups(collect(columnar = true, data)),
          sumGroups(collect(columnar = false, data)),
          sumGroups(withColumnar(onceCogroup(left, right))),
        )
      case other =>
        throw new IllegalStateException(s"unknown query $other")

  private def watched(name: String, boundary: Long, row: Long, once: Long): Observed =
    Observed(name, boundary, row, once)

  private def timeOne(name: String): String =
    val rowNanos = p50Nanos(elapsedRow(name))
    if rowNanos == 0L then
      System.err.println(s"row p50 is 0 for $name")
      line(name, "0.00", "0.00", "0.00", "na", "FAIL")
    else
      val boundaryNanos = p50Nanos(elapsedBoundary(name))
      val onceNanos = p50Nanos(elapsedOnce(name))
      val ratio = onceNanos.toDouble / rowNanos.toDouble
      line(
        name,
        ms(boundaryNanos),
        ms(rowNanos),
        ms(onceNanos),
        String.format(Locale.US, "%.2f", Double.box(ratio)),
        judge(ratio),
      )

  private def elapsedRow(name: String): Long =
    name match
      case "to-string" =>
        val data = intPairs(largeRows).mapValues[Int, Int, String](value => value.toString)
        elapsedCollect(columnar = false, data)
      case "flat-map" =>
        elapsedCollect(columnar = false, flatMapped(largeRows))
      case "element-widen" =>
        elapsedCollect(columnar = false, ints(largeRows).map(value => value.toLong))
      case "flat-map-values" =>
        val data = intPairs(largeRows).flatMapValues[Int, Int, Int](value => Seq(value, value + 1))
        elapsedCollect(columnar = false, data)
      case "sort-by" =>
        elapsedCollect(columnar = false, ints(largeRows).sortBy((value: Int) => value))
      case "cogroup" =>
        elapsedCollect(columnar = false, intPairs(largeRows).cogroup(intPairs(largeRows)))
      case other =>
        throw new IllegalStateException(s"unknown query $other")

  private def elapsedBoundary(name: String): Long =
    name match
      case "to-string" =>
        val data = intPairs(largeRows).mapValues[Int, Int, String](value => value.toString)
        elapsedCollect(columnar = true, data)
      case "flat-map" =>
        elapsedCollect(columnar = true, flatMapped(largeRows))
      case "element-widen" =>
        elapsedCollect(columnar = true, ints(largeRows).map(value => value.toLong))
      case "flat-map-values" =>
        val data = intPairs(largeRows).flatMapValues[Int, Int, Int](value => Seq(value, value + 1))
        elapsedCollect(columnar = true, data)
      case "sort-by" =>
        elapsedCollect(columnar = true, ints(largeRows).sortBy((value: Int) => value))
      case "cogroup" =>
        elapsedCollect(columnar = true, intPairs(largeRows).cogroup(intPairs(largeRows)))
      case other =>
        throw new IllegalStateException(s"unknown query $other")

  private def elapsedOnce(name: String): Long =
    name match
      case "to-string" =>
        val child = intPairs(largeRows)
        elapsedWork(onceToString(child).size)
      case "flat-map" =>
        val child = ints(largeRows).filter(value => value % 2 == 0).map(value => value * 2)
        elapsedWork(onceFlatMap(child).size)
      case "element-widen" =>
        elapsedWork(onceWiden(ints(largeRows)).size)
      case "flat-map-values" =>
        elapsedWork(onceFlatMapValues(intPairs(largeRows)).size)
      case "sort-by" =>
        val child = ints(largeRows)
        elapsedWork(onceSorted(child, (value: Int) => value).size)
      case "cogroup" =>
        elapsedWork(onceCogroup(intPairs(largeRows), intPairs(largeRows)).size)
      case other =>
        throw new IllegalStateException(s"unknown query $other")

  private def onceToString(child: DistCollection[(Int, Int)]): Seq[(Int, String)] =
    decoded(child.plan).iterator
      .flatMap(part => part.iterator.map { case (key, value) => (key, value.toString) })
      .toSeq

  private def onceFlatMap(child: DistCollection[Int]): Seq[Int] =
    decoded(child.plan).iterator
      .flatMap(part => part.iterator.flatMap(value => Seq(value, value + 1)))
      .toSeq

  private def onceWiden(child: DistCollection[Int]): Seq[Long] =
    decoded(child.plan).iterator.flatMap(part => part.iterator.map(value => value.toLong)).toSeq

  private def onceFlatMapValues(child: DistCollection[(Int, Int)]): Seq[(Int, Int)] =
    decoded(child.plan).iterator.flatMap { part =>
      part.iterator.flatMap { case (key, value) =>
        Seq((key, value), (key, value + 1))
      }
    }.toSeq

  private def onceSorted(child: DistCollection[Int], key: Int => Int): Seq[Int] =
    val frozen = decoded(child.plan).map(part => Partition(part))
    scheduled(Plan.SortByOp(Plan.Source(frozen), key, Ordering.Int))

  private def onceCogroup(
      left: DistCollection[(Int, Int)],
      right: DistCollection[(Int, Int)],
  ): Seq[(Int, (Iterable[Int], Iterable[Int]))] =
    val leftParts = decoded(left.plan).map(part => Partition(part))
    val rightParts = decoded(right.plan).map(part => Partition(part))
    scheduled(Plan.CoGroupOp(Plan.Source(leftParts), Plan.Source(rightParts)))

  private def decoded[A](plan: Plan[A]): Seq[Vector[A]] =
    val parts = ColumnarPlanner
      .tryRun(plan)
      .getOrElse(
        throw new IllegalStateException("child did not decode"),
      )
    parts.map(part => part.data.iterator.toVector)

  private def scheduled[A](plan: Plan[A]): Seq[A] =
    val runtime = ScarletRuntime.get
    val scheduler = new DAGScheduler(runtime.shuffle, runtime.scheduler, runtime.partitioner)
    scheduler.executePartitions(plan).iterator.flatMap(_.data.iterator).toSeq

  private def withColumnar[A](body: => A): A =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = true))
      ScarletRuntime.get.shuffle.clear()
      body
    finally ScarletConf.set(previous)

  private def collect[A](columnar: Boolean, data: DistCollection[A]): Seq[A] =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = columnar))
      ScarletRuntime.get.shuffle.clear()
      data.collect().toSeq
    finally ScarletConf.set(previous)

  private def flatMapped(rows: Int): DistCollection[Int] =
    ints(rows)
      .filter(value => value % 2 == 0)
      .map(value => value * 2)
      .flatMap(value => Seq(value, value + 1))

  private def p50Nanos(sample: => Long): Long =
    (0 until warmup).foreach(_ => discard(sample))
    val samples = Array.fill(measure)(sample)
    Arrays.sort(samples)
    samples(p50Index)

  private def discard(sample: Long): Unit =
    if sample < 0L then throw new IllegalStateException("negative sample")

  private def elapsedCollect[A](columnar: Boolean, data: DistCollection[A]): Long =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = columnar))
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

  private def ints(rows: Int): DistCollection[Int] =
    DistCollection.ofInts(Array.tabulate(rows)(i => i + 1), 2)

  private def sumPairsString(rows: Seq[(Int, String)]): Long =
    rows.foldLeft(0L)((acc, row) => acc + row._1.toLong * 31L + row._2.hashCode.toLong)

  private def sumPairsInt(rows: Seq[(Int, Int)]): Long =
    rows.foldLeft(0L)((acc, row) => acc + row._1.toLong * 31L + row._2.toLong)

  private def sumInts(rows: Seq[Int]): Long =
    rows.foldLeft(0L)((acc, value) => acc + value.toLong)

  private def sumLongs(rows: Seq[Long]): Long =
    rows.foldLeft(0L)((acc, value) => acc + value)

  private def sumOrderedInts(rows: Seq[Int]): Long =
    rows.iterator.zipWithIndex.foldLeft(0L)((acc, item) =>
      acc + item._1.toLong * 31L + item._2.toLong,
    )

  private def sumGroups(rows: Seq[(Int, (Iterable[Int], Iterable[Int]))]): Long =
    rows.sortBy(_._1).foldLeft(0L) { (acc, row) =>
      val left = row._2._1.iterator.toVector
      val right = row._2._2.iterator.toVector
      val leftSum = left.foldLeft(0L)((inner, value) => inner + value.toLong)
      val rightSum = right.foldLeft(0L)((inner, value) => inner + value.toLong)
      acc + row._1.toLong * 31L + leftSum * 17L + rightSum + left.length.toLong * 1000003L +
        right.length.toLong
    }

  private def ms(nanos: Long): String =
    String.format(Locale.US, "%.2f", Double.box(nanos.toDouble / 1000000.0))

  private def untimed(one: Observed): String =
    line(one.name, "0.00", "0.00", "0.00", "0.00", if one.matched then "OK" else "FAIL")

  private def line(
      name: String,
      boundaryMs: String,
      rowMs: String,
      onceMs: String,
      ratio: String,
      verdict: String,
  ): String =
    s"SPLIT query=$name rows=$largeRows boundary_p50_ms=$boundaryMs row_p50_ms=$rowMs " +
      s"once_p50_ms=$onceMs once_ratio=$ratio verdict=$verdict"

  private def verdictOf(text: String): String =
    text.split("verdict=").lift(1).getOrElse("FAIL")
