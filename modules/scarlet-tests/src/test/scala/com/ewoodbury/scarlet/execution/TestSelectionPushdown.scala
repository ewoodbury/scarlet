package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Column pruning at pair encode. A key projection encodes one `Int32` column. A filter that reads
 * the other ordinal keeps both. Grouped sum, sort, and join stay on both columns.
 */
class TestSelectionPushdown extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get
  private val parts = 2

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "key filters then keys" should "encode one column and match the row path" in {
    val source = pairs()
    val queried = source.filterKeys((key: Int) => key % 2 == 1).filterKeys((key: Int) => key > 0).keys

    shape(queried.plan) shouldBe "Key(FilterKey(FilterKey(Scan)))"
    val ordinals = demanded(queried.plan)
    ordinals shouldBe Set(0)
    val encoded = encodedPairs(source, ordinals)
    encoded.map(_.columns.length) shouldBe Vector(1, 1)
    encoded.length shouldBe parts
    encodeFootprint(source, ordinals) shouldBe EncodeFootprint(batches = parts.toLong, encodes = parts.toLong)

    val expected = row(queried.collect().toList)
    val (collected, batches) = footprint(queried.collect().toList)
    collected shouldBe expected
    collected shouldBe List(1, 3)
    // Encode, one stack publish, and the key projection. One morsel per partition.
    batches shouldBe (3L * parts)
  }

  "filterValues then keys" should "encode both ordinals because the filter reads values" in {
    val source = pairs()
    val queried = source.filterValues((value: Int) => value > 15).keys

    shape(queried.plan) shouldBe "Key(FilterValue(Scan))"
    demanded(queried.plan) shouldBe Set(0, 1)
    encodedPairs(source, demanded(queried.plan)).map(_.columns.length) shouldBe Vector(2, 2)

    footprint(queried.collect().toList)._1 shouldBe row(queried.collect().toList)
  }

  "FilterValue(FilterKey(Scan)) then keys" should "keep both columns" in {
    val source = DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 5, 30, 40), parts)
    val queried = source
      .filterKeys((key: Int) => key > 1)
      .filterValues((value: Int) => value > 15)
      .keys

    shape(queried.plan) shouldBe "Key(FilterValue(FilterKey(Scan)))"
    demanded(queried.plan) shouldBe Set(0, 1)
    encodedPairs(source, demanded(queried.plan)).map(_.columns.length) shouldBe Vector(2, 2)
    footprint(queried.collect().toList)._1 shouldBe List(3, 4)
    footprint(queried.collect().toList)._1 shouldBe row(queried.collect().toList)
  }

  "filterValues then values" should "encode the value column only" in {
    val source = pairs()
    val queried = source.filterValues((value: Int) => value > 15).values

    shape(queried.plan) shouldBe "Value(FilterValue(Scan))"
    demanded(queried.plan) shouldBe Set(1)
    encodedPairs(source, demanded(queried.plan)).map(_.columns.length) shouldBe Vector(1, 1)
    footprint(queried.collect().toList)._1 shouldBe row(queried.collect().toList)
  }

  "reduceByKey after a key filter" should "still see values" in {
    val source = DistCollection.ofIntPairs(Array(1, 1, 2, 2), Array(10, 5, 7, 1), parts)
    val queried = source.filterKeys((key: Int) => key > 0).reduceByKey[Int, Int](_ + _)

    shape(queried.plan) shouldBe "HashAggregate(Exchange(FilterKey(Scan)))"
    demanded(queried.plan) shouldBe Set(0, 1)
    encodedPairs(source, demanded(queried.plan)).map(_.columns.length) shouldBe Vector(2, 2)
    queried.collect().toMap shouldBe Map(1 -> 15, 2 -> 8)
    queried.collect().toMap shouldBe row(queried.collect().toMap)
  }

  "grouped sum after a key filter" should "still see values" in {
    val source = pairs()
    val queried = source.filterKeys((key: Int) => key > 1)

    shape(queried.plan) shouldBe "FilterKey(Scan)"
    demanded(queried.plan) shouldBe Set(0, 1)
    val encoded = ColumnarInterpreter.encodeGroupedPairs(partitionsOf(source))
    encoded.map(_.columns.length) shouldBe Vector(2, 2)
    encoded.length shouldBe parts
    footprint(queried.groupedSum)._1 shouldBe 90L
    queried.groupedSum shouldBe row(queried.groupedSum)
  }

  "a sort after a key filter" should "demand both columns" in {
    val source = pairs()
    val filtered = source.filterKeys((key: Int) => key > 0)
    val sortPlan = Plan.SortByIntKeyOp(filtered.plan)
    val filterOp = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a filter"))
    val sortOp = PhysicalOp.SortByIntKey(filterOp)

    // A filter child does not lower. The demand walk still keeps both ordinals.
    ColumnarPlanner.lower(sortPlan) shouldBe None
    ColumnUse.ordinals(sortPlan, sortOp) shouldBe Set(0, 1)
    encodedPairs(source, ColumnUse.ordinals(sortPlan, sortOp)).map(_.columns.length) shouldBe
      Vector(2, 2)

    arm()
    val columns = ColumnarInterpreter.interpret(sortPlan, sortOp)
    ColumnarInterpreter.batchesOf(columns).foreach { batch =>
      batch.columns.length shouldBe 2
    }
    decodedPairs(columns) shouldBe row(filtered.sortByIntKey.collect().toList)
  }

  "a join after a key filter" should "demand both columns" in {
    val source = pairs()
    val left = source.filterKeys((key: Int) => key > 1)
    val right = DistCollection.ofIntPairs(Array(2, 4), Array(7, 8), parts)
    val joined = left.join(right)

    shape(joined.plan) shouldBe "HashJoin(Exchange(FilterKey(Scan)), Exchange(Scan))"
    val ordinals = demanded(joined.plan)
    ordinals shouldBe Set(0, 1)
    encodedPairs(source, ordinals).map(_.columns.length) shouldBe Vector(2, 2)
    encodedPairs(right, ordinals).map(_.columns.length) shouldBe Vector(2, 2)

    val expected = row(joined.collect().toList).sorted
    footprint(joined.collect().toList.sorted)._1 shouldBe expected
    expected shouldBe List((2, (20, 7)), (4, (40, 8)))
  }

  "map then keys" should "encode both ordinals and match the row path" in {
    val source = pairs()
    val queried = source.map((pair: (Int, Int)) => (pair._1 + pair._2, pair._2)).keys
    val ordinals = ColumnUse.ordinals(queried.plan, projectedMap(source, 0))

    encodedPairs(source, ordinals).map(_.columns.length) shouldBe Vector(2, 2)
    arm()
    val collected = queried.collect().toList
    collected shouldBe List(11, 22, 33, 44)
    collected shouldBe row(queried.collect().toList)
  }

  "map then values" should "encode both ordinals and match the row path" in {
    val source = pairs()
    val queried = source.map((pair: (Int, Int)) => (pair._1 + pair._2, pair._2)).values
    val ordinals = ColumnUse.ordinals(queried.plan, projectedMap(source, 1))

    encodedPairs(source, ordinals).map(_.columns.length) shouldBe Vector(2, 2)
    arm()
    val collected = queried.collect().toList
    collected shouldBe List(10, 20, 30, 40)
    collected shouldBe row(queried.collect().toList)
  }

  "filterKeys then map then keys" should "encode both ordinals and match the row path" in {
    val source = pairs()
    val queried = source
      .filterKeys((key: Int) => key > 1)
      .map((pair: (Int, Int)) => (pair._1 + pair._2, pair._2))
      .keys
    val scan = pairScan(source)
    val op = PhysicalOp.Column(
      PhysicalOp.Project(PhysicalOp.Filter(scan, PhysicalOp.Slot.Key)),
      0,
    )

    encodedPairs(source, ColumnUse.ordinals(queried.plan, op)).map(_.columns.length) shouldBe
      Vector(2, 2)
    arm()
    val collected = queried.collect().toList
    collected shouldBe List(22, 33, 44)
    collected shouldBe row(queried.collect().toList)
  }

  "a string filter stack" should "stay encode plus one publish" in {
    val stacked = DistCollection(Seq("US", "CA", "US"), 1)
      .filterStringEquals("US")
      .filterStringEquals("US")

    shape(stacked.plan) shouldBe "Filter(Filter(Scan))"
    val expected = row(stacked.collect().toList)
    val (collected, batches, visits) = stringFootprint(stacked.collect().toList)
    collected shouldBe expected
    collected shouldBe List("US", "US")
    batches shouldBe 2L
    visits shouldBe 1L
  }

  private def pairs(): DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 20, 30, 40), parts)

  private def demanded(plan: Plan[_]): Set[Int] =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("expected a columnar plan"))
    ColumnUse.ordinals(plan, op)

  /** A pair `map` does not lower. This is `keys` or `values` above `Project` of the pair scan. */
  private def projectedMap(source: DistCollection[_], ordinal: Int): PhysicalOp =
    PhysicalOp.Column(PhysicalOp.Project(pairScan(source)), ordinal)

  private def pairScan(source: DistCollection[_]): PhysicalOp =
    PhysicalOp.Scan(PhysicalOp.Kind.Pairs, partitionsOf(source))

  private def encodedPairs(
      collection: DistCollection[_],
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    collection.plan match
      case source: Plan.Source[_] =>
        ColumnarInterpreter.batchesOf(
          ColumnarInterpreter.interpretAt(
            source,
            PhysicalOp.Scan(PhysicalOp.Kind.Pairs, source.partitions),
            None,
            false,
            ordinals,
          ),
        )
      case _ => fail("expected a source")

  private def partitionsOf(collection: DistCollection[_]): Seq[Partition[_]] =
    collection.plan match
      case source: Plan.Source[_] => source.partitions
      case _ => fail("expected a source")

  private def encodeFootprint(
      collection: DistCollection[_],
      ordinals: Set[Int],
  ): EncodeFootprint =
    arm()
    ColumnMetrics.withSink(new CountingSink) {
      encodedPairs(collection, ordinals)
      EncodeFootprint(ColumnMetrics.batchCount, ColumnMetrics.primitiveEncodeCount)
    }

  private def footprint[A](body: => A): (A, Long) =
    arm()
    ColumnMetrics.withSink(new CountingSink) {
      val value = body
      (value, ColumnMetrics.batchCount)
    }

  private def stringFootprint[A](body: => A): (A, Long, Long) =
    arm()
    ColumnMetrics.withSink(new CountingSink) {
      val value = body
      (value, ColumnMetrics.batchCount, ColumnMetrics.visitCount)
    }

  private def decodedPairs(columns: ColumnarInterpreter.Columns): List[(Int, Int)] =
    ColumnarInterpreter.batchesOf(columns).flatMap { batch =>
      BatchCodec.decodeIntPairs(batch).toList
    }.toList

  private def shape(plan: Plan[_]): String =
    arm()
    ColumnarPlanner.lower(plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  private def arm(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      original.copy(columnarExecution = true, columnarBatchSize = 1024, threadPoolSize = 1),
    )

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletRuntime.get.shuffle.clear()
      ScarletConf.set(saved.copy(columnarExecution = false, threadPoolSize = 1))
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private final case class EncodeFootprint(batches: Long, encodes: Long)
