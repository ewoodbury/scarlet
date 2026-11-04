package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Columnar inner shuffle-hash join of `(String, Int)` with `(String, Int)`.
 *
 * The row path is the oracle. The two sides keep independent dictionaries and match by string
 * equality. A null key is dropped. `""` matches `""`. A null value stays on the row path.
 * Broadcast, sort-merge, and outer joins stay on the row path.
 */
class TestStringKeyJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val accent = "é"
  private val decomposed = "e\u0301"
  private val tripleSchema = Vector(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32)

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "string inner join" should "emit the cartesian product of duplicate keys" in {
    val left = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3), 2)
    val right = DistCollection(Seq("a" -> 10, "a" -> 11, "c" -> 4), 2)
    val expected = Seq(
      "a" -> ((1, 10)),
      "a" -> ((1, 11)),
      "a" -> ((2, 10)),
      "a" -> ((2, 11)),
    )

    agree(left.join(right), expected)
    agree(left.shuffleHashJoin(right), expected)
  }

  it should "concatenate a union of two string joins" in {
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 2)
    val right = DistCollection(Seq("a" -> 10, "b" -> 20), 2)
    val joined = left.join(right)
    val expected = Seq("a" -> ((1, 10)), "b" -> ((2, 20)))
    val union = joined.union(joined)

    ColumnarPlanner.lower(union.plan).map(_.shape).getOrElse(fail("union did not lower")) should
      startWith("Union(")
    val columnar = union.collect().toList
    columnar should contain theSameElementsAs expected ++ expected
    columnar should contain theSameElementsAs row(union.collect().toList)
  }

  it should "match an empty string and drop a null key" in {
    // `""` and a null key both hash to 0. Only the present empty string matches.
    val left = DistCollection(Seq("" -> 1, nullKey(9), "red" -> 2), 2)
    val right = DistCollection(Seq("" -> 10, nullKey(8), "blue" -> 3), 2)

    agree(left.join(right), Seq("" -> ((1, 10))))
  }

  it should "match the same strings when each side's dictionary order differs" in {
    val left = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("b" -> 1, "a" -> 2)),
          Partition(Seq("a" -> 3)),
        ),
      ),
    )
    val right = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("a" -> 10, "z" -> 7)),
          Partition(Seq("b" -> 20)),
        ),
      ),
    )

    agree(left.join(right), Seq("b" -> ((1, 20)), "a" -> ((2, 10)), "a" -> ((3, 10))))
  }

  it should "keep the probe dictionary when one partition shares the batch" in {
    val build = BatchCodec.stringIntPairs(Seq("b" -> 1, "a" -> 2))
    val probe = BatchCodec.stringIntPairs(Seq("a" -> 10, "z" -> 7, "b" -> 20))
    val probeDictionary = utf8(probe).dictionary
    val parts = StringHashJoin.inner(Vector(build), Vector(probe), partitions = 1, parallelism = 1)

    parts.length shouldBe 1
    val batch = parts.lift(0).getOrElse(fail("expected the one join partition"))
    val keys = utf8(batch)
    keys.dictionary should be theSameInstanceAs probeDictionary
    keys.dictionary.toSeq shouldBe Seq("a", "z", "b")
    val decoded = BatchCodec.decodeStringJoin(batch)
    decoded should contain theSameElementsAs Seq("a" -> ((2, 10)), "b" -> ((1, 20)))
    (0 until batch.length).foreach { row =>
      keys.dictionary(keys.codes(row)) match
        case "a" => keys.codes(row) shouldBe 0
        case "b" => keys.codes(row) shouldBe 2
        case other => fail(s"unexpected key $other")
    }
    assertPresent(batch, part = 0, parts = 1)
  }

  it should "not match a precomposed accent with its decomposed form" in {
    accent should not be decomposed
    val both = DistCollection(Seq(accent -> 1, decomposed -> 2), 1).join(
      DistCollection(Seq(accent -> 10, decomposed -> 20), 1),
    )
    val crossed = DistCollection(Seq(accent -> 1), 1).join(
      DistCollection(Seq(decomposed -> 10), 1),
    )

    agree(both, Seq(accent -> ((1, 10)), decomposed -> ((2, 20))))
    agree(crossed, Seq.empty)
  }

  it should "return an empty batch per partition when a side or the matches are empty" in {
    val present = DistCollection(Seq("a" -> 1, "b" -> 2), 2)
    val other = DistCollection(Seq("a" -> 10), 1)
    val emptyBuild = present.filterKeys((_: String) => false).join(other)
    val emptyProbe = present.join(other.filterKeys((_: String) => false))
    val unmatched = present.join(DistCollection(Seq("c" -> 3, "d" -> 4), 2))

    ColumnarPlanner.lower(emptyBuild.plan).map(_.shape) shouldBe Some(
      "StringHashJoin(Exchange(FilterKey(Scan)), Exchange(Scan))",
    )
    ColumnarPlanner.lower(emptyProbe.plan).map(_.shape) shouldBe Some(
      "StringHashJoin(Exchange(Scan), Exchange(FilterKey(Scan)))",
    )
    agree(emptyBuild, Seq.empty)
    agree(emptyProbe, Seq.empty)
    agree(unmatched, Seq.empty)

    val once = StringHashJoin.inner(
      Vector.empty,
      Vector(BatchCodec.stringIntPairs(Seq("a" -> 1))),
      partitions = 4,
      parallelism = 1,
    )
    val twice = StringHashJoin.inner(
      Vector.empty,
      Vector(BatchCodec.stringIntPairs(Seq("a" -> 1))),
      partitions = 4,
      parallelism = 1,
    )
    once.length shouldBe 4
    once.zip(twice).foreach { (left, right) =>
      left should not be theSameInstanceAs(right)
      left.length shouldBe 0
      left.schema shouldBe tripleSchema
    }
    once.zip(once.drop(1)).foreach { (left, right) =>
      left should not be theSameInstanceAs(right)
    }
  }

  it should "lower a filter or mapValues under the join" in {
    val left = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3), 2).filterKeys((key: String) =>
      keep(key, "b"),
    )
    val right = DistCollection(Seq("a" -> 10, "c" -> 4), 2).mapValues((value: Int) => value + 1)
    val joined = left.join(right)
    val byValue = DistCollection(Seq("a" -> 1, "a" -> 0), 1)
      .filterValues((value: Int) => value > 0)
      .join(DistCollection(Seq("a" -> 5), 1))

    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
      "StringHashJoin(Exchange(FilterKey(Scan)), Exchange(MapValue(Scan)))",
    )
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some(
      "StringHashJoin(Exchange(FilterValue(Scan)), Exchange(Scan))",
    )
    agree(joined, Seq("a" -> ((1, 11)), "a" -> ((3, 11))))
    agree(byValue, Seq("a" -> ((1, 5))))
  }

  it should "keep outer, broadcast, and sort-merge joins on the row path" in {
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 2)
    val right = DistCollection(Seq("a" -> 10, "c" -> 3), 2)

    refuse(left.broadcastJoin(right))
    refuse(left.sortMergeJoin(right))
    refuse(left.leftOuterJoin(right))
    refuse(left.rightOuterJoin(right))
    refuse(left.fullOuterJoin(right))
  }

  it should "keep keys, values, and a filter above the join on the row path" in {
    val joined = DistCollection(Seq("a" -> 1, "b" -> 2), 2).join(DistCollection(Seq("a" -> 10), 1))
    val keys = joined.keys
    val values = joined.values
    val filtered = joined.filter { case (_, (left, _)) => left > 0 }

    ColumnarPlanner.lower(keys.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(values.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(filtered.plan) shouldBe Option.empty[PhysicalOp]
    keys.collect().toList should contain theSameElementsAs row(keys.collect().toList)
    values.collect().toList should contain theSameElementsAs row(values.collect().toList)
    filtered.collect().toList should contain theSameElementsAs row(filtered.collect().toList)
  }

  it should "refuse a null value and throw when a value column is not fully present" in {
    val source = DistCollection(
      Plan.Source(Seq(Partition(Seq("red" -> 1, erasedNullValue("blue"))))),
    )
    ColumnarPlanner.lower(source.join(DistCollection(Seq("red" -> 2), 1)).plan) shouldBe
      Option.empty[PhysicalOp]

    val missing = new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(DictUtf8Column(Seq("a")), Int32Column.fromNullable(Seq(None))),
      1,
    )
    val probe = BatchCodec.stringIntPairs(Seq("a" -> 1))
    an[IllegalStateException] should be thrownBy {
      StringHashJoin.inner(Vector(missing), Vector(probe), partitions = 1, parallelism = 1)
    }
    an[IllegalStateException] should be thrownBy {
      StringHashJoin.inner(Vector(probe), Vector(missing), partitions = 1, parallelism = 1)
    }
  }

  private def refuse[A](collection: DistCollection[A]): Unit =
    ColumnarPlanner.lower(collection.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.partitioning(collection.plan) shouldBe Option.empty[PartitioningInfo]
    collection.collect().toList should contain theSameElementsAs row(collection.collect().toList)

  private def agree(
      joined: DistCollection[(String, (Int, Int))],
      expected: Seq[(String, (Int, Int))],
  ): Unit =
    val columnar = joined.collect().toList
    columnar should contain theSameElementsAs expected
    columnar should contain theSameElementsAs row(joined.collect().toList)
    val parts = stringTripleBatches(joined.plan)
    parts.length shouldBe ScarletConf.get.defaultShufflePartitions
    parts.zipWithIndex.foreach { (batch, index) =>
      assertPresent(batch, index, parts.length)
    }

  private def stringTripleBatches(plan: Plan[_]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail(s"expected a lowered plan, got $plan"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.StringTriples(parts) => parts
      case other => fail(s"expected string triples, got $other")

  private def assertPresent(batch: ColumnBatch, part: Int, parts: Int): Unit =
    batch.schema shouldBe tripleSchema
    val decoded = BatchCodec.decodeStringJoin(batch)
    decoded.length shouldBe batch.length
    batch.columns.toList match
      case (keys: DictUtf8Column) :: (left: Int32Column) :: (right: Int32Column) :: Nil =>
        (0 until batch.length).foreach { row =>
          keys.isValid(row) shouldBe true
          left.isValid(row) shouldBe true
          right.isValid(row) shouldBe true
        }
        if (keys.validity.capacity > batch.length) then
          keys.validity.isValid(batch.length) shouldBe false
          left.validity.isValid(batch.length) shouldBe false
          right.validity.isValid(batch.length) shouldBe false
        decoded.foreach { (key, _) =>
          Math.floorMod(StringHash.hash(key), parts) shouldBe part
        }
      case other => fail(s"expected a string join batch, found $other")

  private def utf8(batch: ColumnBatch): DictUtf8Column = batch.columns.toList match
    case (column: DictUtf8Column) :: _ => column
    case other => fail(s"expected a dictionary key, found $other")

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def keep(key: String, dropped: String): Boolean = key match
    case found if found == dropped => false
    case _ => true

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedNullValue(key: String): (String, Int) =
    Tuple2[String, Integer](key, null).asInstanceOf[(String, Int)]

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
