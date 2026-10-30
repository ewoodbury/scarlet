package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Columnar rank split of an int-pair scan. A null key orders as zero and stays null. A null value
 * stays null. Ties keep input order. The row path is the comparison for the same plan with
 * columnar execution off.
 */
class TestSortByIntKey extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "sortByIntKey" should "match the row path for distinct, duplicate, and negative keys" in {
    agree(Seq(4 -> 1, 1 -> 2, 3 -> 3, 2 -> 4))
    agree((0 until 40).map(index => (index % 3, index)))
    agree(Seq(Int.MinValue -> 1, -1 -> 2, 0 -> 3, Int.MaxValue -> 4, -100 -> 5, 5 -> 6))
  }

  it should "retain null keys and null values and order a null key as zero" in {
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(-5), boxedInt(3)),
      intPair(boxedInt(0), boxedInt(7)),
      intPair(noInt, noInt),
    )
    val source = DistCollection(rows, 3)
    val sorted = source.sortByIntKey
    val columnar = collect(sorted)
    val row = rowCollect(sorted)

    columnar shouldBe row
    columnar should contain theSameElementsAs(row)
    columnar shouldBe rows.sortBy(effectiveKey)
    effectiveKeys(columnar) shouldBe effectiveKeys(columnar).sorted
    assertBatchValidity(sortedParts(sorted.plan), rows.sortBy(effectiveKey))
  }

  it should "retain a null value beside a present key" in {
    val rows = Seq(
      intPair(boxedInt(2), boxedInt(20)),
      intPair(boxedInt(-1), noInt),
      intPair(boxedInt(2), boxedInt(21)),
      intPair(boxedInt(0), boxedInt(1)),
    )
    val source = DistCollection(rows, 2)
    val sorted = source.sortByIntKey
    val columnar = collect(sorted)

    columnar shouldBe rowCollect(sorted)
    columnar should contain theSameElementsAs(rows)
    assertBatchValidity(sortedParts(sorted.plan), rows.sortBy(effectiveKey))
  }

  it should "sort an empty int-pair source into empty output partitions" in {
    val source = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4)
    val sorted = source.sortByIntKey
    val parts = sortedParts(sorted.plan)

    collect(sorted) shouldBe Seq.empty[(Int, Int)]
    rowCollect(sorted) shouldBe Seq.empty[(Int, Int)]
    parts.length shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    }
  }

  it should "split ranks so partition sizes differ by at most one" in {
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = 8))
    val source = DistCollection.ofIntPairs(Array(2, 0, 1), Array(20, 0, 10), 2)
    val parts = sortedParts(source.sortByIntKey.plan)

    parts.map(_.length) shouldBe Vector(1, 0, 1, 0, 0, 1, 0, 0)
    assertBoundaries(parts)
    parts.flatMap(batch => BatchCodec.decodeIntPairs(batch)) shouldBe Seq(0 -> 0, 1 -> 10, 2 -> 20)
  }

  it should "report range distribution, ascending order, and SortByIntKey(Scan)" in {
    val source = DistCollection.ofIntPairs(Array(3, 1, 2), Array(30, 10, 20), 2)
    val sorted = source.sortByIntKey
    val info = ColumnarPlanner.partitioning(sorted.plan).getOrElse(fail("expected a layout"))

    info.distribution shouldBe Distribution.Range(
      ScarletConf.get.defaultShufflePartitions,
      PartitioningInfo.RowKeyOrdinals,
    )
    info.ordering shouldBe OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true))
    info.layout shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByIntKey(Scan)")
  }

  it should "decode the same prefix as the row path, including zero and past the end" in {
    val source = DistCollection.ofIntPairs(
      Array(3, -1, 3, 0, 2, 8, -4, 1),
      Array(1, 2, 3, 4, 5, 6, 7, 8),
      3,
    )
    val sorted = source.sortByIntKey
    val full = rowCollect(sorted)
    Seq(0, 1, 2, full.length, full.length + 5).foreach { count =>
      ColumnarPlanner.tryTake(sorted.plan, count) shouldBe Some(full.take(count))
      sorted.take(count) shouldBe rowTake(sorted, count)
      sorted.take(count) shouldBe full.take(count).toList
    }
  }

  it should "decode a prefix that keeps null keys and null values" in {
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(-5), boxedInt(3)),
      intPair(boxedInt(0), boxedInt(7)),
      intPair(noInt, noInt),
      intPair(boxedInt(2), noInt),
    )
    val source = DistCollection(rows, 3)
    val sorted = source.sortByIntKey
    val full = rowCollect(sorted)
    Seq(0, 2, 4, full.length).foreach { count =>
      val taken = ColumnarPlanner.tryTake(sorted.plan, count)
      taken shouldBe Some(full.take(count))
      sorted.take(count) shouldBe full.take(count).toList
    }
    val prefix = ColumnarPlanner.tryTake(sorted.plan, 4).getOrElse(fail("missing prefix"))
    prefix.exists(pair => !present(pair, 0)) shouldBe true
    prefix.exists(pair => !present(pair, 1)) shouldBe true
  }

  it should "reject a clear bit on a non-sort int-pair batch" in {
    val batch = BatchCodec.nullableIntPairs(Seq((None, Some(1)), (Some(0), Some(2))))
    val columns = ColumnarInterpreter.Columns.Pairs(Vector(batch))
    val scan = PhysicalOp.Scan(PhysicalOp.Kind.Pairs, Seq.empty)
    val full = the[IllegalArgumentException] thrownBy {
      ColumnarInterpreter.decodeFor(columns, scan)
    }
    val prefix = the[IllegalArgumentException] thrownBy {
      ColumnarInterpreter.decodePrefixFor(columns, batch.length, scan)
    }

    full.getMessage should include("null Int32 pair at row 0")
    prefix.getMessage should include("null Int32 pair at row 0")
  }

  it should "reject a clear bit from the strict int-pair decoder" in {
    val batch = BatchCodec.nullableIntPairs(Seq((Some(1), None)))
    val full = the[IllegalArgumentException] thrownBy {
      BatchCodec.decodeIntPairs(batch)
    }
    val prefix = the[IllegalArgumentException] thrownBy {
      BatchCodec.decodeIntPairs(batch, batch.length)
    }

    full.getMessage should include("null Int32 pair at row 0")
    prefix.getMessage should include("null Int32 pair at row 0")
    val kept = BatchCodec.decodeIntPairsPreservingAbsence(batch, batch.length)
    kept.length shouldBe 1
    val pair = kept.headOption.getOrElse(fail("missing pair"))
    present(pair, 0) shouldBe true
    present(pair, 1) shouldBe false
    storedInt(pair, 0) shouldBe 1
  }

  it should "leave a filter above the sort, and any non-scan child, on the row path" in {
    val source = DistCollection.ofIntPairs(Array(3, 1, 2, 2), Array(30, 10, 20, 21), 2)
    val above = source.sortByIntKey.filter((pair: (Int, Int)) => pair._2 != 20)
    val below = source.filter((pair: (Int, Int)) => pair._1 > 0).sortByIntKey
    val mapped = source.map((pair: (Int, Int)) => (pair._1, pair._2)).sortByIntKey
    val rewritten = source.mapValues((value: Int) => value + 1).sortByIntKey

    ColumnarPlanner.partitioning(above.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.lower(above.plan) shouldBe Option.empty[PhysicalOp]
    collect(above) shouldBe rowCollect(above)
    ColumnarPlanner.partitioning(below.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(mapped.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(rewritten.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(source.sortByIntKey.keys.plan) shouldBe
      Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(source.sortByIntKey.values.plan) shouldBe
      Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(
      source.sortByIntKey.reduceByKey((left: Int, right: Int) => left + right).plan,
    ) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(source.sortByIntKey.join(source).plan) shouldBe
      Option.empty[PartitioningInfo]
  }

  "RankSplit" should "keep cross-partition ties in input order and place ranks by r * partitions / rows" in {
    val left = BatchCodec.nullableIntPairs(Seq((Some(5), Some(50)), (Some(1), Some(10))))
    val right = BatchCodec.nullableIntPairs(
      Seq((Some(1), Some(11)), (Some(4), Some(40)), (Some(0), Some(0))),
    )
    val parts = RankSplit.sortAndSplit(Vector(left, right), partitions = 2, parallelism = 1)

    val first = parts.lift(0).getOrElse(fail("missing partition 0"))
    val second = parts.lift(1).getOrElse(fail("missing partition 1"))
    parts.map(_.length) shouldBe Vector(3, 2)
    BatchCodec.decodeIntPairs(first) shouldBe Seq(0 -> 0, 1 -> 10, 1 -> 11)
    BatchCodec.decodeIntPairs(second) shouldBe Seq(4 -> 40, 5 -> 50)
    assertBoundaries(parts)
  }

  it should "keep a null key null when it ties with zero" in {
    val first = BatchCodec.nullableIntPairs(Seq((None, Some(1)), (Some(0), Some(2))))
    val second = BatchCodec.nullableIntPairs(Seq((Some(0), Some(3))))
    val parts = RankSplit.sortAndSplit(Vector(first, second), partitions = 1, parallelism = 1)
    val batch = parts.headOption.getOrElse(fail("expected one partition"))
    val keys = intColumn(batch, 0)
    val values = intColumn(batch, 1)

    batch.length shouldBe 3
    keys.isValid(0) shouldBe false
    values.isValid(0) shouldBe true
    values.values(0) shouldBe 1
    keys.isValid(1) shouldBe true
    keys.values(1) shouldBe 0
    values.values(1) shouldBe 2
    keys.isValid(2) shouldBe true
    keys.values(2) shouldBe 0
    values.values(2) shouldBe 3
  }

  it should "order a null key as zero even when its payload is not zero" in {
    val keys = Int32Column.of(Array(7, 1), Validity.pack(2, row => row == 1), 2)
    val values = Int32Column.of(Array(10, 20), Validity.allValid(2), 2)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(keys, values),
      2,
    )
    val parts = RankSplit.sortAndSplit(Vector(batch), partitions = 1, parallelism = 1)
    val sorted = parts.headOption.getOrElse(fail("expected one partition"))
    val outKeys = intColumn(sorted, 0)
    val outValues = intColumn(sorted, 1)

    outKeys.isValid(0) shouldBe false
    outValues.isValid(0) shouldBe true
    outValues.values(0) shouldBe 10
    outKeys.isValid(1) shouldBe true
    outKeys.values(1) shouldBe 1
    outValues.values(1) shouldBe 20
  }

  it should "reject an empty batch vector, a non-positive width, and a non-positive parallelism" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val missing = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplit(Vector.empty, partitions = 2, parallelism = 1)
    }
    val width = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplit(Vector(empty), partitions = 0, parallelism = 1)
    }
    val negative = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplit(Vector(empty), partitions = -1, parallelism = 1)
    }
    val workers = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplit(Vector(empty), partitions = 1, parallelism = 0)
    }

    missing.getMessage should include("sortAndSplit needs at least one batch")
    width.getMessage should include("numPartitions must be positive, got 0")
    negative.getMessage should include("numPartitions must be positive, got -1")
    workers.getMessage should include("parallelism must be positive, got 0")
  }

  it should "return one empty pair batch per output partition when every input row is absent" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val parts = RankSplit.sortAndSplit(Vector(empty, empty), partitions = 3, parallelism = 1)

    parts should have length 3
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    }
  }

  it should "agree for parallelism greater than one" in {
    val batches = Vector(
      BatchCodec.intPairs(Seq((3, 1), (1, 2))),
      BatchCodec.intPairs(Seq((2, 3))),
      BatchCodec.intPairs(Seq((2, 4), (0, 5), (4, 6))),
      BatchCodec.intPairs(Seq((-1, 8), (1, 7))),
    )
    val serial = RankSplit.sortAndSplit(batches, partitions = 3, parallelism = 1)
    val parallel = RankSplit.sortAndSplit(batches, partitions = 3, parallelism = 2)

    parallel.map(batch => BatchCodec.decodeIntPairs(batch)) shouldBe
      serial.map(batch => BatchCodec.decodeIntPairs(batch))
    parallel.map(validityBits) shouldBe serial.map(validityBits)
    assertBoundaries(parallel)
  }

  private def agree(rows: Seq[(Int, Int)]): Unit =
    val source = DistCollection(rows, 4)
    val sorted = source.sortByIntKey
    val columnar = collect(sorted)
    val row = rowCollect(sorted)

    columnar shouldBe row
    columnar should contain theSameElementsAs(row)
    effectiveKeys(columnar) shouldBe effectiveKeys(columnar).sorted
    assertBoundaries(sortedParts(sorted.plan))

  private def assertBoundaries(parts: Vector[ColumnBatch]): Unit =
    val span = parts.foldLeft(Option.empty[(Int, Int)]) { (bounds, batch) =>
      val length = batch.length
      bounds match
        case None => Some((length, length))
        case Some((low, high)) => Some((math.min(low, length), math.max(high, length)))
    }
    span.foreach { (low, high) =>
      (high - low) should be <= 1
    }
    parts.foreach(assertSortedPartition)
    parts.zip(parts.drop(1)).foreach { (current, next) =>
      if (current.length > 0 && next.length > 0) then
        lastEffective(current) should be <= firstEffective(next)
    }
    val keys = parts.flatMap { batch =>
      val column = intColumn(batch, 0)
      (0 until batch.length).map(row => effectiveAt(column, row))
    }
    keys shouldBe keys.sorted

  private def assertSortedPartition(batch: ColumnBatch): Unit =
    val keys = intColumn(batch, 0)
    (0 until batch.length - 1).foreach { row =>
      effectiveAt(keys, row) should be <= effectiveAt(keys, row + 1)
    }

  private def assertBatchValidity(
      parts: Vector[ColumnBatch],
      expected: Seq[(Int, Int)],
  ): Unit =
    val located = parts.flatMap { batch =>
      (0 until batch.length).map(row => (batch, row))
    }
    located.length shouldBe expected.length
    located.zip(expected).foreach { case ((batch, row), pair) =>
      val keys = intColumn(batch, 0)
      val values = intColumn(batch, 1)
      keys.isValid(row) shouldBe present(pair, 0)
      values.isValid(row) shouldBe present(pair, 1)
      if (present(pair, 0)) then keys.values(row) shouldBe storedInt(pair, 0)
      if (present(pair, 1)) then values.values(row) shouldBe storedInt(pair, 1)
    }

  private def sortedParts(plan: Plan[(Int, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.Pairs(parts) => parts
      case other => fail(s"expected int pairs, got $other")

  private def validityBits(batch: ColumnBatch): Seq[(Boolean, Boolean)] =
    val keys = intColumn(batch, 0)
    val values = intColumn(batch, 1)
    (0 until batch.length).map(row => (keys.isValid(row), values.isValid(row)))

  private def intColumn(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal")) match
      case column: Int32Column => column
      case other => fail(s"column $ordinal is ${other.logicalType}")

  private def effectiveAt(column: Int32Column, row: Int): Int =
    if (column.isValid(row)) column.values(row) else 0

  private def firstEffective(batch: ColumnBatch): Int =
    effectiveAt(intColumn(batch, 0), 0)

  private def lastEffective(batch: ColumnBatch): Int =
    effectiveAt(intColumn(batch, 0), batch.length - 1)

  private def effectiveKeys(rows: Seq[(Int, Int)]): Seq[Int] =
    rows.map(effectiveKey)

  private def effectiveKey(pair: (Int, Int)): Int =
    if (present(pair, 0)) storedInt(pair, 0) else 0

  private def present(pair: (Int, Int), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Integer => true
      case _ => false

  private def storedInt(pair: (Int, Int), ordinal: Int): Int =
    pair.productElement(ordinal) match
      case key: java.lang.Integer => key.intValue()
      case other => fail(s"expected an int component, found $other")

  private def collect[A](collection: DistCollection[A]): Seq[A] =
    ScarletRuntime.get.shuffle.clear()
    collection.collect().toSeq

  private def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    rowAction(collection.collect().toSeq)

  private def rowTake[A](collection: DistCollection[A], count: Int): List[A] =
    rowAction(collection.take(count))

  private def rowAction[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
