package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.LogicalType
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Type-by-operation coverage. Every cell runs the same gate checks. The expected record is a
 * literal, so a new type or operation without a row fails this suite. The key set is the product
 * of the types and operations. `Sum` repeats `Source` on purpose: sum is that plan, not a separate
 * node, and the cell documents its sum gates. An accepted Boolean or String source still leaves
 * every sum gate closed.
 *
 * `Count` and `Take` follow a filter. `MapCount` and `MapTake` follow a same-type map and use that
 * map's gates. Int, Long, Double, and String same-type maps are accepted. A Boolean same-type map
 * is refused, so its `MapCount` and `MapTake` cells are refused too. `Function1` has no
 * `apply$mcZZ$sp`, so `Boolean => Boolean` cannot be distinguished from `Boolean => Int` by
 * declared signature. Refusing is the honest contract.
 *
 * Long rows are `1, 2, 3, 4` and sum to 10, so the matrix does not overflow. Wrapping is asserted
 * beside the matrix and in `TestLongColumnar`.
 */
class TestTypeCoverage extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val Partitions = 2
  private val Prefix = 2
  private val intRows = Array(1, 2, 3, 4)
  private val longRows = Array(1L, 2L, 3L, 4L)
  private val doubleRows = Array(1.0, 2.0, 3.0, 4.0)
  private val boolRows = Array(true, false, true, false)
  private val stringRows = Array("a", "bb", "a", "c")

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "the type coverage matrix" should "run every type and operation against the row path" in {
    val product = for
      kind <- CoverageType.values.toList
      op <- CoverageOp.values.toList
    yield (kind, op)

    product.size shouldBe 45
    product.size shouldBe CoverageType.values.length * CoverageOp.values.length
    matrix.keySet shouldBe product.toSet
    product.foreach { case (kind, op) =>
      val gates = matrix((kind, op))
      // Acceptance is the row's partitioning bit. The other gates must agree with it.
      val accepted = gates.partitioning
      withClue(s"$kind $op table: ") {
        requireSameMapGates(kind, op, gates)
        gates.run shouldBe accepted
        gates.count shouldBe accepted
        gates.take shouldBe accepted
        kind match
          case CoverageType.Ints =>
            gates.sumInt shouldBe accepted
            gates.sumLong shouldBe false
            gates.sumDouble shouldBe false
          case CoverageType.Longs =>
            gates.sumInt shouldBe false
            gates.sumLong shouldBe accepted
            gates.sumDouble shouldBe false
          case CoverageType.Doubles =>
            gates.sumInt shouldBe false
            gates.sumLong shouldBe false
            gates.sumDouble shouldBe accepted
          case CoverageType.Booleans | CoverageType.Strings =>
            gates.sumInt shouldBe false
            gates.sumLong shouldBe false
            gates.sumDouble shouldBe false
      }
      withClue(s"$kind $op: ") {
        run(kind, op, gates)
      }
    }
  }

  "long sum" should "wrap Long.MaxValue + 1 to Long.MinValue on both paths" in {
    val values = Array(Long.MaxValue, 1L)
    val wrapped = DistCollection.ofLongs(values, Partitions)
    wrapped.plan match
      case Plan.Source(partitions, descriptor) =>
        descriptor shouldBe Plan.SourceDescriptor.Longs
        partitions.map(_.data.toVector) shouldBe Seq(Vector(Long.MaxValue), Vector(1L))
      case other => fail(s"expected a source, got $other")
    val summed = rowPath(wrapped.sum)
    summed shouldBe Long.MinValue
    wrapped.sum shouldBe Long.MinValue
    ColumnarPlanner.trySumLong(wrapped.plan) shouldBe Some(Long.MinValue)
    ColumnarPlanner.trySumInt(wrapped.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumDouble(wrapped.plan) shouldBe Option.empty[Double]
  }

  "int take" should "read past the end and through an empty partition" in {
    val source = DistCollection.ofInts(intRows, Partitions)
    val pastEnd = intRows.length + 3
    val beyond = source.take(pastEnd)
    beyond shouldBe List(1, 2, 3, 4)
    beyond shouldBe rowPath(source.take(pastEnd))
    ColumnarPlanner.tryTake(source.plan, pastEnd).map(_.toList) shouldBe Some(beyond)

    // The empty partition sits between values, so a prefix of 3 has to read past it.
    val gapped = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(1, 2)),
          Partition(Seq.empty[Int]),
          Partition(Seq(3, 4)),
        ),
        Plan.SourceDescriptor.Ints,
      ),
    )
    gapped.plan match
      case Plan.Source(partitions, _) =>
        partitions.map(_.data.isEmpty) shouldBe Seq(false, true, false)
      case other => fail(s"expected a source, got $other")
    val crossed = gapped.take(3)
    crossed shouldBe List(1, 2, 3)
    crossed shouldBe rowPath(gapped.take(3))
    ColumnarPlanner.tryTake(gapped.plan, 3).map(_.toList) shouldBe Some(crossed)
  }

  "coverage enums" should "name every logical type, source descriptor, and physical kind" in {
    LogicalType.values.toSet shouldBe CoverageType.values.map(logicalType).toSet
    Plan.SourceDescriptor.values.toSet shouldBe Set(
      Plan.SourceDescriptor.Unknown,
      Plan.SourceDescriptor.Ints,
      Plan.SourceDescriptor.Longs,
      Plan.SourceDescriptor.Doubles,
      Plan.SourceDescriptor.Pairs,
      Plan.SourceDescriptor.Strings,
      Plan.SourceDescriptor.Bools,
      Plan.SourceDescriptor.LongPairs,
    )
    PhysicalOp.Kind.values.toSet shouldBe Set(
      PhysicalOp.Kind.Ints,
      PhysicalOp.Kind.Longs,
      PhysicalOp.Kind.Floats,
      PhysicalOp.Kind.Pairs,
      PhysicalOp.Kind.Triples,
      PhysicalOp.Kind.Strings,
      PhysicalOp.Kind.Bools,
      PhysicalOp.Kind.LongPairs,
    )
  }

  "grouped sum" should "fold int pairs and scan long pairs without summing them" in {
    val pairs = DistCollection.ofIntPairs(Array(1, 1, 2), Array(10, 5, 20), Partitions)
    val summed = rowPath(pairs.groupedSum)
    summed shouldBe 35L
    pairs.groupedSum shouldBe summed
    ColumnarPlanner.tryGroupedSum(pairs.plan) shouldBe Some(summed)
    ColumnarPlanner.partitioning(pairs.plan).isDefined shouldBe true
    ColumnarPlanner.trySumInt(pairs.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumLong(pairs.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumDouble(pairs.plan) shouldBe Option.empty[Double]

    // `(Long, Int)` is not a long-pair column. It stays on the row path.
    val mixed = DistCollection(Seq((1L, 10), (1L, 5)), Partitions)
    refuse(mixed.plan)

    val longPairs = DistCollection(Seq((1L, 10L), (1L, 5L)), Partitions)
    ColumnarPlanner.lower(longPairs.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.LongPairs)
    ColumnarPlanner.partitioning(longPairs.plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(longPairs.plan).isDefined shouldBe true
    ColumnarPlanner.tryCount(longPairs.plan) shouldBe Some(2L)
    ColumnarPlanner.trySumInt(longPairs.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumLong(longPairs.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumDouble(longPairs.plan) shouldBe Option.empty[Double]
    val taken = rowPath(longPairs.take(Prefix))
    longPairs.take(Prefix) shouldBe taken
    ColumnarPlanner.tryTake(longPairs.plan, Prefix).map(_.toList) shouldBe Some(taken)
    ColumnarPlanner.tryGroupedSum(longPairs.plan) shouldBe Option.empty[Long]
    longPairs.collect().toList should contain theSameElementsAs rowPath(longPairs.collect().toList)
  }

  "string keys" should "keep groupByKey and join on the row path" in {
    val keyed = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 4), Partitions)
    refuse(keyed.groupByKey.plan)
    refuse(keyed.join(keyed).plan)
  }

  "ofStrings" should "select an empty array and leave an empty iterable unknown" in {
    val owned = DistCollection.ofStrings(Array.empty[String], 3)
    descriptor(owned) shouldBe Plan.SourceDescriptor.Strings
    ColumnarPlanner.partitioning(owned.plan).isDefined shouldBe true
    ColumnarPlanner.lower(owned.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Strings)
    owned.collect().toVector shouldBe Vector.empty[String]
    owned.count() shouldBe 0L
    owned.take(Prefix) shouldBe Nil
    rowPath(owned.collect().toVector) shouldBe Vector.empty[String]
    rowPath(owned.count()) shouldBe 0L
    rowPath(owned.take(Prefix)) shouldBe Nil

    val input = Array("a", "bb", "c")
    val copied = DistCollection.ofStrings(input, Partitions)
    input(0) = "z"
    copied.collect().toVector shouldBe Vector("a", "bb", "c")
    rowPath(copied.collect().toVector) shouldBe Vector("a", "bb", "c")

    val present = DistCollection(Seq("a", "bb"), Partitions)
    descriptor(present) shouldBe Plan.SourceDescriptor.Unknown
    ColumnarPlanner.lower(present.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Strings)
    present.collect().toVector shouldBe Vector("a", "bb")

    val emptyIterable = DistCollection(Seq.empty[String], 3)
    descriptor(emptyIterable) shouldBe Plan.SourceDescriptor.Unknown
    ColumnarPlanner.partitioning(emptyIterable.plan).isDefined shouldBe false
    emptyIterable.collect().toVector shouldBe Vector.empty[String]
    emptyIterable.count() shouldBe 0L
    rowPath(emptyIterable.count()) shouldBe 0L
  }

  private def run(kind: CoverageType, op: CoverageOp, gates: Gates): Unit = (kind, op) match
    case (CoverageType.Ints, _) => ints(op, gates)
    case (CoverageType.Longs, _) => longs(op, gates)
    case (CoverageType.Doubles, _) => doubles(op, gates)
    case (CoverageType.Booleans, _) => booleans(op, gates)
    case (CoverageType.Strings, _) => strings(op, gates)

  private def ints(op: CoverageOp, gates: Gates): Unit =
    val source = DistCollection.ofInts(intRows, Partitions)
    op match
      case CoverageOp.Source =>
        val summed = rowPath(source.sum)
        check(source, gates, Some(summed), None, None)
        source.collect().toVector shouldBe Vector(1, 2, 3, 4)
        source.sum shouldBe summed
        summed shouldBe 10L
      case CoverageOp.Filter =>
        val filtered = source.filter(value => value > 2)
        val summed = rowPath(filtered.sum)
        check(filtered, gates, Some(summed), None, None)
        filtered.collect().toVector shouldBe Vector(3, 4)
        filtered.sum shouldBe summed
        summed shouldBe 7L
      case CoverageOp.MapSame =>
        val mapped = source.map(value => value + 1)
        val summed = rowPath(mapped.sum)
        check(mapped, gates, Some(summed), None, None)
        mapped.collect().toVector shouldBe Vector(2, 3, 4, 5)
        mapped.sum shouldBe summed
        summed shouldBe 14L
      case CoverageOp.MapChanging =>
        val mapped = source.map(value => value.toLong)
        check(mapped, gates, None, None, None)
        mapped.collect().toVector shouldBe Vector(1L, 2L, 3L, 4L)
        mapped.sum shouldBe rowPath(mapped.sum)
        mapped.sum shouldBe 10L
      case CoverageOp.Count =>
        val filtered = source.filter(value => value > 2)
        check(filtered, gates, Some(rowPath(filtered.sum)), None, None)
        filtered.count() shouldBe 2L
      case CoverageOp.MapCount =>
        val mapped = source.map(value => value + 1)
        check(mapped, gates, Some(rowPath(mapped.sum)), None, None)
        mapped.count() shouldBe 4L
      case CoverageOp.Sum =>
        val summed = rowPath(source.sum)
        check(source, gates, Some(summed), None, None)
        source.sum shouldBe summed
        summed shouldBe 10L
      case CoverageOp.Take =>
        val filtered = source.filter(value => value > 2)
        check(filtered, gates, Some(rowPath(filtered.sum)), None, None)
        filtered.take(Prefix) shouldBe List(3, 4)
      case CoverageOp.MapTake =>
        val mapped = source.map(value => value + 1)
        check(mapped, gates, Some(rowPath(mapped.sum)), None, None)
        mapped.take(Prefix) shouldBe List(2, 3)

  private def longs(op: CoverageOp, gates: Gates): Unit =
    val source = DistCollection.ofLongs(longRows, Partitions)
    op match
      case CoverageOp.Source =>
        val summed = rowPath(source.sum)
        check(source, gates, None, Some(summed), None)
        source.collect().toVector shouldBe Vector(1L, 2L, 3L, 4L)
        source.sum shouldBe summed
        summed shouldBe 10L
      case CoverageOp.Filter =>
        val filtered = source.filter(value => value > 2L)
        val summed = rowPath(filtered.sum)
        check(filtered, gates, None, Some(summed), None)
        filtered.collect().toVector shouldBe Vector(3L, 4L)
        filtered.sum shouldBe summed
        summed shouldBe 7L
      case CoverageOp.MapSame =>
        val mapped = source.map(value => value + 1L)
        val summed = rowPath(mapped.sum)
        check(mapped, gates, None, Some(summed), None)
        mapped.collect().toVector shouldBe Vector(2L, 3L, 4L, 5L)
        mapped.sum shouldBe summed
        summed shouldBe 14L
      case CoverageOp.MapChanging =>
        val mapped = source.map(value => value.toInt)
        check(mapped, gates, None, None, None)
        mapped.collect().toVector shouldBe Vector(1, 2, 3, 4)
        mapped.sum shouldBe rowPath(mapped.sum)
        mapped.sum shouldBe 10L
      case CoverageOp.Count =>
        val filtered = source.filter(value => value > 2L)
        check(filtered, gates, None, Some(rowPath(filtered.sum)), None)
        filtered.count() shouldBe 2L
      case CoverageOp.MapCount =>
        val mapped = source.map(value => value + 1L)
        check(mapped, gates, None, Some(rowPath(mapped.sum)), None)
        mapped.count() shouldBe 4L
      case CoverageOp.Sum =>
        val summed = rowPath(source.sum)
        check(source, gates, None, Some(summed), None)
        source.sum shouldBe summed
        summed shouldBe 10L
      case CoverageOp.Take =>
        val filtered = source.filter(value => value > 2L)
        check(filtered, gates, None, Some(rowPath(filtered.sum)), None)
        filtered.take(Prefix) shouldBe List(3L, 4L)
      case CoverageOp.MapTake =>
        val mapped = source.map(value => value + 1L)
        check(mapped, gates, None, Some(rowPath(mapped.sum)), None)
        mapped.take(Prefix) shouldBe List(2L, 3L)

  private def doubles(op: CoverageOp, gates: Gates): Unit =
    val source = DistCollection.ofDoubles(doubleRows, Partitions)
    op match
      case CoverageOp.Source =>
        val summed = rowPath(source.sum)
        check(source, gates, None, None, Some(summed))
        source.collect().toVector shouldBe Vector(1.0, 2.0, 3.0, 4.0)
        source.sum shouldBe summed
        summed shouldBe 10.0
      case CoverageOp.Filter =>
        val filtered = source.filter(value => value > 2.0)
        val summed = rowPath(filtered.sum)
        check(filtered, gates, None, None, Some(summed))
        filtered.collect().toVector shouldBe Vector(3.0, 4.0)
        filtered.sum shouldBe summed
        summed shouldBe 7.0
      case CoverageOp.MapSame =>
        val mapped = source.map(value => value + 1.0)
        val summed = rowPath(mapped.sum)
        check(mapped, gates, None, None, Some(summed))
        mapped.collect().toVector shouldBe Vector(2.0, 3.0, 4.0, 5.0)
        mapped.sum shouldBe summed
        summed shouldBe 14.0
      case CoverageOp.MapChanging =>
        val mapped = source.map(value => value.toInt)
        check(mapped, gates, None, None, None)
        mapped.collect().toVector shouldBe Vector(1, 2, 3, 4)
        mapped.sum shouldBe rowPath(mapped.sum)
        mapped.sum shouldBe 10L
      case CoverageOp.Count =>
        val filtered = source.filter(value => value > 2.0)
        check(filtered, gates, None, None, Some(rowPath(filtered.sum)))
        filtered.count() shouldBe 2L
      case CoverageOp.MapCount =>
        val mapped = source.map(value => value + 1.0)
        check(mapped, gates, None, None, Some(rowPath(mapped.sum)))
        mapped.count() shouldBe 4L
      case CoverageOp.Sum =>
        val summed = rowPath(source.sum)
        check(source, gates, None, None, Some(summed))
        source.sum shouldBe summed
        summed shouldBe 10.0
      case CoverageOp.Take =>
        val filtered = source.filter(value => value > 2.0)
        check(filtered, gates, None, None, Some(rowPath(filtered.sum)))
        filtered.take(Prefix) shouldBe List(3.0, 4.0)
      case CoverageOp.MapTake =>
        val mapped = source.map(value => value + 1.0)
        check(mapped, gates, None, None, Some(rowPath(mapped.sum)))
        mapped.take(Prefix) shouldBe List(2.0, 3.0)

  private def booleans(op: CoverageOp, gates: Gates): Unit =
    val source = DistCollection.ofBools(boolRows, Partitions)
    op match
      case CoverageOp.Source =>
        check(source, gates, None, None, None)
        source.collect().toVector shouldBe Vector(true, false, true, false)
        source.count() shouldBe 4L
      case CoverageOp.Filter =>
        val filtered = source.filter(value => value)
        check(filtered, gates, None, None, None)
        filtered.collect().toVector shouldBe Vector(true, true)
        filtered.count() shouldBe 2L
      case CoverageOp.MapSame =>
        val mapped = source.map(value => !value)
        check(mapped, gates, None, None, None)
        mapped.collect().toVector shouldBe Vector(false, true, false, true)
      case CoverageOp.MapChanging =>
        val mapped = source.map(value => if value then 1 else 0)
        check(mapped, gates, None, None, None)
        mapped.collect().toVector shouldBe Vector(1, 0, 1, 0)
        mapped.sum shouldBe rowPath(mapped.sum)
        mapped.sum shouldBe 2L
      case CoverageOp.Count =>
        val filtered = source.filter(value => value)
        check(filtered, gates, None, None, None)
        filtered.count() shouldBe 2L
      case CoverageOp.MapCount =>
        val mapped = source.map(value => !value)
        check(mapped, gates, None, None, None)
        mapped.count() shouldBe 4L
      case CoverageOp.Sum =>
        check(source, gates, None, None, None)
        source.count() shouldBe 4L
      case CoverageOp.Take =>
        val filtered = source.filter(value => value)
        check(filtered, gates, None, None, None)
        filtered.take(Prefix) shouldBe List(true, true)
      case CoverageOp.MapTake =>
        val mapped = source.map(value => !value)
        check(mapped, gates, None, None, None)
        mapped.take(Prefix) shouldBe List(false, true)

  private def strings(op: CoverageOp, gates: Gates): Unit =
    val source = DistCollection.ofStrings(stringRows, Partitions)
    op match
      case CoverageOp.Source =>
        check(source, gates, None, None, None)
        source.collect().toVector shouldBe Vector("a", "bb", "a", "c")
        source.count() shouldBe 4L
      case CoverageOp.Filter =>
        val filtered = source.filter(value => value.length > 1)
        check(filtered, gates, None, None, None)
        filtered.collect().toVector shouldBe Vector("bb")
        filtered.count() shouldBe 1L
      case CoverageOp.MapSame =>
        val mapped = source.map(value => value.toUpperCase(java.util.Locale.ENGLISH))
        check(mapped, gates, None, None, None)
        mapped.collect().toVector shouldBe Vector("A", "BB", "A", "C")
      case CoverageOp.MapChanging =>
        val mapped = source.map(value => value.length)
        check(mapped, gates, None, None, None)
        mapped.collect().toVector shouldBe Vector(1, 2, 1, 1)
        mapped.sum shouldBe rowPath(mapped.sum)
        mapped.sum shouldBe 5L
      case CoverageOp.Count =>
        val filtered = source.filter(value => value.length > 1)
        check(filtered, gates, None, None, None)
        filtered.count() shouldBe 1L
      case CoverageOp.MapCount =>
        val mapped = source.map(value => value.toUpperCase(java.util.Locale.ENGLISH))
        check(mapped, gates, None, None, None)
        mapped.count() shouldBe 4L
      case CoverageOp.Sum =>
        check(source, gates, None, None, None)
        source.count() shouldBe 4L
      case CoverageOp.Take =>
        val filtered = source.filter(value => value.length > 1)
        check(filtered, gates, None, None, None)
        filtered.take(Prefix) shouldBe List("bb")
      case CoverageOp.MapTake =>
        val mapped = source.map(value => value.toUpperCase(java.util.Locale.ENGLISH))
        check(mapped, gates, None, None, None)
        mapped.take(Prefix) shouldBe List("A", "BB")

  private def check[A](
      collection: DistCollection[A],
      gates: Gates,
      sumInt: Option[Long],
      sumLong: Option[Long],
      sumDouble: Option[Double],
  ): Unit =
    val plan = collection.plan
    val rowValues = rowPath(collection.collect().toVector)
    val rowCount = rowPath(collection.count())
    val rowTake = rowPath(collection.take(Prefix))
    collection.collect().toVector shouldBe rowValues
    collection.count() shouldBe rowCount
    collection.take(Prefix) shouldBe rowTake

    ColumnarPlanner.partitioning(plan).isDefined shouldBe gates.partitioning
    ColumnarPlanner.tryRun(plan).map(flat) shouldBe Option.when(gates.run)(rowValues)
    ColumnarPlanner.tryCount(plan) shouldBe Option.when(gates.count)(rowCount)
    sumInt.isDefined shouldBe gates.sumInt
    sumLong.isDefined shouldBe gates.sumLong
    sumDouble.isDefined shouldBe gates.sumDouble
    ColumnarPlanner.trySumInt(plan) shouldBe sumInt
    ColumnarPlanner.trySumLong(plan) shouldBe sumLong
    ColumnarPlanner.trySumDouble(plan) shouldBe sumDouble
    ColumnarPlanner.tryTake(plan, Prefix).map(_.toList) shouldBe Option.when(gates.take)(rowTake)
    ColumnarPlanner.tryGroupedSum(plan) shouldBe Option.empty[Long]

  private def flat[A](parts: Seq[Partition[A]]): Vector[A] =
    parts.iterator.flatMap(part => part.data.iterator).toVector

  private def refuse(plan: Plan[_]): Unit =
    ColumnarPlanner.partitioning(plan).isDefined shouldBe false
    ColumnarPlanner.tryRun(plan).isDefined shouldBe false
    ColumnarPlanner.tryCount(plan).isDefined shouldBe false
    ColumnarPlanner.trySumInt(plan).isDefined shouldBe false
    ColumnarPlanner.trySumLong(plan).isDefined shouldBe false
    ColumnarPlanner.trySumDouble(plan).isDefined shouldBe false
    ColumnarPlanner.tryTake(plan, Prefix).isDefined shouldBe false
    ColumnarPlanner.tryGroupedSum(plan).isDefined shouldBe false

  private def descriptor[A](collection: DistCollection[A]): Plan.SourceDescriptor =
    collection.plan match
      case Plan.Source(_, sourceDescriptor) => sourceDescriptor
      case other => fail(s"expected a source, got $other")

  private def rowPath[A](read: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      read
    finally ScarletConf.set(saved)

  /** `MapCount` and `MapTake` are the same gate record as that type's same-type map. */
  private def requireSameMapGates(kind: CoverageType, op: CoverageOp, gates: Gates): Unit =
    op match
      case CoverageOp.MapCount | CoverageOp.MapTake =>
        gates shouldBe matrix((kind, CoverageOp.MapSame))
      case CoverageOp.Source | CoverageOp.Filter | CoverageOp.MapSame | CoverageOp.MapChanging |
          CoverageOp.Count | CoverageOp.Sum | CoverageOp.Take =>
        ()

  private def logicalType(kind: CoverageType): LogicalType = kind match
    case CoverageType.Ints => LogicalType.Int32
    case CoverageType.Longs => LogicalType.Int64
    case CoverageType.Doubles => LogicalType.Float64
    case CoverageType.Booleans => LogicalType.Bool
    case CoverageType.Strings => LogicalType.Utf8Dict

  private def open(sumInt: Boolean, sumLong: Boolean, sumDouble: Boolean): Gates =
    Gates(
      partitioning = true,
      run = true,
      count = true,
      sumInt = sumInt,
      sumLong = sumLong,
      sumDouble = sumDouble,
      take = true,
    )

  private val intOpen = open(sumInt = true, sumLong = false, sumDouble = false)
  private val longOpen = open(sumInt = false, sumLong = true, sumDouble = false)
  private val doubleOpen = open(sumInt = false, sumLong = false, sumDouble = true)
  private val nonNumericOpen = open(sumInt = false, sumLong = false, sumDouble = false)
  private val refusedGates = Gates(
    partitioning = false,
    run = false,
    count = false,
    sumInt = false,
    sumLong = false,
    sumDouble = false,
    take = false,
  )

  /**
   * Hard-coded support. The key set is every type crossed with every operation. Boolean and string
   * `Sum` rows are accepted sources with sum gates closed. `Sum` repeats `Source` so that fact
   * stays in the table. `MapCount` and `MapTake` repeat `MapSame`.
   *
   * A Boolean same-type map is refused. `Function1` has no `apply$mcZZ$sp`, so `Boolean =>
   * Boolean` cannot be distinguished from `Boolean => Int` by declared signature. Refusing is the
   * honest contract. Long rows sum to 10. Overflow is not a matrix cell. `TestLongColumnar` covers
   * the wrap, and this suite asserts it on its own two-element source.
   */
  private val matrix: Map[(CoverageType, CoverageOp), Gates] = Map(
    (CoverageType.Ints, CoverageOp.Source) -> intOpen,
    (CoverageType.Ints, CoverageOp.Filter) -> intOpen,
    (CoverageType.Ints, CoverageOp.MapSame) -> intOpen,
    (CoverageType.Ints, CoverageOp.MapChanging) -> refusedGates,
    (CoverageType.Ints, CoverageOp.Count) -> intOpen,
    (CoverageType.Ints, CoverageOp.MapCount) -> intOpen,
    (CoverageType.Ints, CoverageOp.Sum) -> intOpen,
    (CoverageType.Ints, CoverageOp.Take) -> intOpen,
    (CoverageType.Ints, CoverageOp.MapTake) -> intOpen,
    (CoverageType.Longs, CoverageOp.Source) -> longOpen,
    (CoverageType.Longs, CoverageOp.Filter) -> longOpen,
    (CoverageType.Longs, CoverageOp.MapSame) -> longOpen,
    (CoverageType.Longs, CoverageOp.MapChanging) -> refusedGates,
    (CoverageType.Longs, CoverageOp.Count) -> longOpen,
    (CoverageType.Longs, CoverageOp.MapCount) -> longOpen,
    (CoverageType.Longs, CoverageOp.Sum) -> longOpen,
    (CoverageType.Longs, CoverageOp.Take) -> longOpen,
    (CoverageType.Longs, CoverageOp.MapTake) -> longOpen,
    (CoverageType.Doubles, CoverageOp.Source) -> doubleOpen,
    (CoverageType.Doubles, CoverageOp.Filter) -> doubleOpen,
    (CoverageType.Doubles, CoverageOp.MapSame) -> doubleOpen,
    (CoverageType.Doubles, CoverageOp.MapChanging) -> refusedGates,
    (CoverageType.Doubles, CoverageOp.Count) -> doubleOpen,
    (CoverageType.Doubles, CoverageOp.MapCount) -> doubleOpen,
    (CoverageType.Doubles, CoverageOp.Sum) -> doubleOpen,
    (CoverageType.Doubles, CoverageOp.Take) -> doubleOpen,
    (CoverageType.Doubles, CoverageOp.MapTake) -> doubleOpen,
    (CoverageType.Booleans, CoverageOp.Source) -> nonNumericOpen,
    (CoverageType.Booleans, CoverageOp.Filter) -> nonNumericOpen,
    (CoverageType.Booleans, CoverageOp.MapSame) -> refusedGates,
    (CoverageType.Booleans, CoverageOp.MapChanging) -> refusedGates,
    (CoverageType.Booleans, CoverageOp.Count) -> nonNumericOpen,
    (CoverageType.Booleans, CoverageOp.MapCount) -> refusedGates,
    (CoverageType.Booleans, CoverageOp.Sum) -> nonNumericOpen,
    (CoverageType.Booleans, CoverageOp.Take) -> nonNumericOpen,
    (CoverageType.Booleans, CoverageOp.MapTake) -> refusedGates,
    (CoverageType.Strings, CoverageOp.Source) -> nonNumericOpen,
    (CoverageType.Strings, CoverageOp.Filter) -> nonNumericOpen,
    (CoverageType.Strings, CoverageOp.MapSame) -> nonNumericOpen,
    (CoverageType.Strings, CoverageOp.MapChanging) -> refusedGates,
    (CoverageType.Strings, CoverageOp.Count) -> nonNumericOpen,
    (CoverageType.Strings, CoverageOp.MapCount) -> nonNumericOpen,
    (CoverageType.Strings, CoverageOp.Sum) -> nonNumericOpen,
    (CoverageType.Strings, CoverageOp.Take) -> nonNumericOpen,
    (CoverageType.Strings, CoverageOp.MapTake) -> nonNumericOpen,
  )

private final case class Gates(
    partitioning: Boolean,
    run: Boolean,
    count: Boolean,
    sumInt: Boolean,
    sumLong: Boolean,
    sumDouble: Boolean,
    take: Boolean,
)

enum CoverageType:
  case Ints, Longs, Doubles, Booleans, Strings

enum CoverageOp:
  case Source, Filter, MapSame, MapChanging, Count, MapCount, Sum, Take, MapTake
