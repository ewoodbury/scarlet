package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan}

/** Empty typed sources keep a descriptor; a disagreeing descriptor is refused. */
class TestColumnarEmptySources
    extends AnyFlatSpec
    with BeforeAndAfterEach
    with ColumnarContractFixture:

  override def beforeEach(): Unit =
    super.beforeEach()
    installContractConf()

  override def afterEach(): Unit =
    restoreContractConf()
    ()

  "the columnar contract" should "use source descriptors for empty primitive sources without changing empty results" in {
    val emptyInts = DistCollection.ofInts(Array.empty[Int], 2)
    val emptyDoubles = DistCollection.ofDoubles(Array.empty[Double], 2)
    val emptyStrings = Plan.Source(
      Seq(Partition(Seq.empty[String]), Partition(Seq.empty[String])),
      Plan.SourceDescriptor.Strings,
    )
    val emptyStringCollection = DistCollection(emptyStrings)

    val emptyLongs = DistCollection.ofLongs(Array.empty[Long], 2)
    val emptyBools = DistCollection.ofBools(Array.empty[Boolean], 2)
    ColumnarPlanner.partitioning(emptyInts.plan).map(_.layout) shouldBe Some(columnarLayout)
    emptyInts.sum shouldBe 0L
    emptyInts.count() shouldBe 0L
    ColumnarPlanner.partitioning(emptyLongs.plan).map(_.layout) shouldBe Some(columnarLayout)
    ColumnarPlanner.lower(emptyLongs.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Longs)
    emptyLongs.sum shouldBe 0L
    emptyLongs.count() shouldBe 0L
    ColumnarPlanner.partitioning(emptyBools.plan).map(_.layout) shouldBe Some(columnarLayout)
    ColumnarPlanner.lower(emptyBools.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Bools)
    emptyBools.count() shouldBe 0L
    ColumnarPlanner.partitioning(emptyDoubles.plan).map(_.layout) shouldBe Some(columnarLayout)
    emptyDoubles.sum shouldBe 0.0
    ColumnarPlanner.partitioning(emptyStringCollection.plan).map(_.layout) shouldBe Some(
      utf8Layout,
    )

    emptyInts.collect() shouldBe empty
    emptyInts.take(3) shouldBe empty
    emptyLongs.collect() shouldBe empty
    emptyLongs.take(3) shouldBe empty
    emptyBools.collect() shouldBe empty
    emptyBools.take(3) shouldBe empty
    emptyDoubles.collect() shouldBe empty
    emptyDoubles.take(3) shouldBe empty
    emptyStringCollection.collect() shouldBe empty
    emptyStringCollection.take(3) shouldBe empty
  }

  it should "refuse a source descriptor that disagrees with non-empty data" in {
    val wrongDescriptor = Plan.Source(
      Seq(Partition(Seq(1, 2))),
      Plan.SourceDescriptor.Pairs,
    )

    ColumnarPlanner.partitioning(wrongDescriptor) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.lower(wrongDescriptor) shouldBe Option.empty[PhysicalOp]
    DistCollection(wrongDescriptor).sum shouldBe 3L
  }
