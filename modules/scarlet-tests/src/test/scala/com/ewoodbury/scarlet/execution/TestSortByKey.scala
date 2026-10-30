package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, PlanWide, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Global sort of typed pairs. An int-pair scan lowers to `SortByIntKey(Scan)`. A long-pair sort
 * stays on the row path. A null key orders as zero and the row is retained, matching `sortBy`.
 */
class TestSortByKey extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val partitions = 4

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "sortByIntKey" should "sort int pairs ascending by column 0, including ties" in {
    val keys = Array(3, -1, 3, 0, Int.MinValue, 2, -1, 8)
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8)
    val source = DistCollection.ofIntPairs(keys, values, partitions)
    val expected = keys.zip(values).sortBy(_._1).toSeq
    val sorted = source.sortByIntKey
    val generic = source.sortBy(_._1)

    sorted.plan match
      case Plan.SortByIntKeyOp(child) => child shouldBe source.plan
      case other => fail(s"expected SortByIntKeyOp, got $other")
    generic.plan shouldBe a[Plan.SortByOp[_, _]]
    assertWideSort(sorted.plan, Ordering.Int)
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByIntKey(Scan)")
    val info = ColumnarPlanner.partitioning(sorted.plan).getOrElse(fail("expected a layout"))
    info.distribution shouldBe Distribution.Range(
      ScarletConf.get.defaultShufflePartitions,
      PartitioningInfo.RowKeyOrdinals,
    )
    info.ordering shouldBe OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true))

    val first = collect(sorted)
    val second = collect(sorted) // idempotent
    first shouldBe expected
    second shouldBe expected
    collect(generic) shouldBe expected
  }

  it should "sort an empty int-pair source" in {
    val source = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], partitions)
    val sorted = source.sortByIntKey

    sorted.plan shouldBe a[Plan.SortByIntKeyOp]
    PlanWide.isWide(sorted.plan) shouldBe true
    collect(sorted) shouldBe Seq.empty[(Int, Int)]
    collect(source.sortBy(_._1)) shouldBe Seq.empty[(Int, Int)]
  }

  "sortByLongKey" should "sort long pairs ascending by column 0, including ties" in {
    val keys = Array(3L, -4L, Long.MinValue, 3L, 0L, -4L, Long.MaxValue, 1L)
    val values = Array(10L, 20L, 30L, 11L, 40L, 21L, 50L, 60L)
    val source = DistCollection.ofLongPairs(keys, values, partitions)
    val expected = keys.zip(values).sortBy(_._1)(Ordering.Long).toSeq
    val sorted = source.sortByLongKey
    val generic = source.sortBy(_._1)

    sorted.plan match
      case Plan.SortByLongKeyOp(child) => child shouldBe source.plan
      case other => fail(s"expected SortByLongKeyOp, got $other")
    generic.plan shouldBe a[Plan.SortByOp[_, _]]
    assertWideSort(sorted.plan, Ordering.Long)
    assertRowPath(sorted.plan)

    collect(sorted) shouldBe expected
    collect(sorted) shouldBe expected // idempotent
    collect(generic) shouldBe expected
  }

  it should "sort an empty long-pair source" in {
    val source = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], partitions)
    val sorted = source.sortByLongKey

    sorted.plan shouldBe a[Plan.SortByLongKeyOp]
    PlanWide.isWide(sorted.plan) shouldBe true
    collect(sorted) shouldBe Seq.empty[(Long, Long)]
    collect(source.sortBy(_._1)) shouldBe Seq.empty[(Long, Long)]
  }

  "a null component" should "keep a null value beside its key" in {
    val ints = Seq(
      intPair(boxedInt(2), boxedInt(20)),
      intPair(boxedInt(-1), noInt),
      intPair(boxedInt(2), boxedInt(21)),
      intPair(boxedInt(0), boxedInt(1)),
    )
    val longs = Seq(
      longPair(boxedLong(2L), boxedLong(20L)),
      longPair(boxedLong(Long.MinValue), noLong),
      longPair(boxedLong(-4L), boxedLong(7L)),
      longPair(boxedLong(2L), boxedLong(21L)),
    )

    val intSource = DistCollection(ints, partitions)
    val longSource = DistCollection(longs, partitions)
    val expectedInts = ints.sortBy(_._1)
    val expectedLongs = longs.sortBy(_._1)(Ordering.Long)

    collect(intSource.sortByIntKey) shouldBe expectedInts
    collect(intSource.sortBy(_._1)) shouldBe expectedInts
    collect(longSource.sortByLongKey) shouldBe expectedLongs
    collect(longSource.sortBy(_._1)) shouldBe expectedLongs
  }

  it should "sort a null key where zero sorts, matching sortBy" in {
    // The key function unboxes column 0. A null int or long unboxes to zero, so it ties with a
    // present zero and keeps its place in that tie. The null stays in the row. The int scan runs
    // on the columnar rank split. The long scan stays on the row path.
    val ints = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(-5), boxedInt(3)),
      intPair(boxedInt(0), boxedInt(7)),
      intPair(noInt, boxedInt(98)),
    )
    val longs = Seq(
      longPair(boxedLong(1L), boxedLong(10L)),
      longPair(noLong, boxedLong(99L)),
      longPair(boxedLong(Long.MinValue), boxedLong(1L)),
      longPair(boxedLong(0L), boxedLong(7L)),
      longPair(noLong, boxedLong(98L)),
    )
    val intSource = DistCollection(ints, partitions)
    val longSource = DistCollection(longs, partitions)
    val expectedInts = ints.sortBy(_._1)
    val expectedLongs = longs.sortBy(_._1)(Ordering.Long)

    collect(intSource.sortByIntKey) shouldBe expectedInts
    collect(intSource.sortBy(_._1)) shouldBe expectedInts
    collect(longSource.sortByLongKey) shouldBe expectedLongs
    collect(longSource.sortBy(_._1)) shouldBe expectedLongs
  }

  private def assertWideSort(plan: Plan[_], ordering: Ordering[_]): Unit =
    PlanWide.isWide(plan) shouldBe true
    PlanWide.isDirectlyWide(plan) shouldBe true
    val graph = StageBuilder.buildStageGraph(plan)
    val sortOps = graph.stages.values.flatMap(_.wideOp).toVector.collect {
      case op: SortByWideOp[_, _] => op
    }
    val op = sortOps match
      case Vector(one) => one
      case other => fail(s"expected one sort stage, got ${other.length}")
    val shufflePartitions = ScarletConf.get.defaultShufflePartitions
    op.meta.numPartitions shouldBe shufflePartitions
    op.meta.ordering shouldBe ordering
    graph.stages(graph.finalStageId).outputPartitioning shouldBe Some(
      PartitioningInfo.rangeSorted(shufflePartitions),
    )

  private def assertRowPath(plan: Plan[_]): Unit =
    ColumnarPlanner.lower(plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.partitioning(plan) shouldBe Option.empty[PartitioningInfo]

  private def collect[A](collection: DistCollection[A]): Seq[A] =
    ScarletRuntime.get.shuffle.clear()
    collection.collect().toSeq

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noLong: java.lang.Long = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def boxedLong(value: Long): java.lang.Long = java.lang.Long.valueOf(value)
