package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{BoolArrays, Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Boolean columnar plans against the row path. Filters, count, and take are columnar. A map is
 * refused and runs once per element on the row path. Boolean has no sum.
 */
class TestBoolColumnar extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "boolean plans" should "match the row path for a short array and an empty array" in {
    check(Array.tabulate(32)(index => (index % 3) != 0), 4)
    check(Array(true, false, true, false, true), 3)
    check(Array.empty[Boolean], 2)
  }

  it should "match a million-row filter count once" in {
    val rows = million
    val expected = rows.count(value => value).toLong
    val counted = agree(DistCollection.ofBools(rows, 4).filter(value => value).count())
    counted shouldBe expected
  }

  it should "nest each filter and keep a five-filter chain on the columnar path" in {
    val source = DistCollection.ofBools(Array(true, false, true, false), 2)
    val one = source.filter(value => value)
    val two = one.filter(value => !value)
    val five = (0 until 5).foldLeft(source)((current, _) => current.filter(value => value))

    shape(one) shouldBe "Filter(Scan)"
    shape(two) shouldBe "Filter(Filter(Scan))"
    shape(five) shouldBe "Filter(Filter(Filter(Filter(Filter(Scan)))))"
    agree(one.collect().toVector) shouldBe Vector(true, true)
    agree(two.collect().toVector) shouldBe Vector.empty[Boolean]
    val fiveCount = agree(five.count())
    fiveCount shouldBe 2L
    agree(five.take(1)) shouldBe List(true)
  }

  it should "keep a single element and an empty partition" in {
    val single = DistCollection.ofBools(Array(false), 1)
    shape(single) shouldBe "Scan"
    agree(single.collect().toVector) shouldBe Vector(false)
    agree(single.count()) shouldBe 1L
    agree(single.take(1)) shouldBe List(false)
    agree(single.filter(value => value).count()) shouldBe 0L

    val sparse = DistCollection.ofBools(Array(true), 3).filter(value => value)
    agree(sparse.collect().toVector) shouldBe Vector(true)
    agree(sparse.count()) shouldBe 1L
    agree(sparse.take(2)) shouldBe List(true)
  }

  it should "classify an empty source from its descriptor" in {
    val empty = DistCollection.ofBools(Array.empty[Boolean], 2)
    ColumnarPlanner.lower(empty.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Bools)
    empty.plan match
      case Plan.Source(_, descriptor) => descriptor shouldBe Plan.SourceDescriptor.Bools
      case other => fail(s"expected a source, got $other")
    agree(empty.count()) shouldBe 0L
    agree(empty.take(4)) shouldBe List.empty[Boolean]
    agree(empty.collect().toVector) shouldBe Vector.empty[Boolean]
    agree(empty.filter(value => value).count()) shouldBe 0L
  }

  it should "ignore later writes to the caller's array" in {
    val raw = Array(true, false, true, false)
    val collection = DistCollection.ofBools(raw, 2)
    raw(0) = false
    raw(3) = true
    collection.collect() shouldBe Seq(true, false, true, false)
    collection.count() shouldBe 4L
    collection.plan match
      case Plan.Source(partitions, _) =>
        partitions.foreach { partition =>
          partition.data match
            case stored: BoolArrays => stored.values should not be theSameInstanceAs(raw)
            case other => fail(s"expected BoolArrays, got $other")
        }
      case other => fail(s"expected a source, got $other")
  }

  it should "classify a boxed java.lang.Boolean as bools" in {
    val first = java.lang.Boolean.TRUE
    val second = java.lang.Boolean.FALSE
    first.getClass shouldBe classOf[java.lang.Boolean]
    val boxed = DistCollection(Seq(first, second), 1)
    ColumnarPlanner.lower(boxed.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Bools)
    agree(boxed.collect().toVector) shouldBe Vector(first, second)
    agree(boxed.count()) shouldBe 2L

    val bools = DistCollection(Seq(true, false), 1)
    ColumnarPlanner.lower(bools.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Bools)
    agree(bools.filter(value => value).count()) shouldBe 1L
  }

  it should "refuse a Boolean to Boolean map and run it once per element" in {
    refuseMap(Array(true, false, true), value => value, Seq(true, false, true))
  }

  it should "refuse a Boolean to Int map and run it once per element" in {
    refuseMap(Array(true, false, true), value => if (value) 1 else 0, Seq(1, 0, 1))
  }

  it should "throw when a boolean column reaches the element pipeline" in {
    val source = DistCollection.ofBools(Array(true, false), 1).plan
    val plan = Plan.FilterOp(
      Plan.FilterOp(source, (value: Boolean) => value),
      (value: Boolean) => value,
    )
    val partitions = source match
      case Plan.Source(stored, _) => stored
      case other => fail(s"expected a source, got $other")
    val pipeline = PhysicalOp.Pipeline(PhysicalOp.Scan(PhysicalOp.Kind.Bools, partitions), 2)

    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("Filter(Filter(Scan))")
    ColumnarPlanner.tryRun(plan).isDefined shouldBe true

    val thrown = the[IllegalStateException] thrownBy {
      ColumnarPlanner.runLowered(plan, pipeline)
    }
    thrown.getMessage shouldBe "pipeline expects an int, long, float, or string column"
  }

  it should "accept filter, count, and take, and refuse map and sum" in {
    val source = DistCollection.ofBools(Array(true, false, true), 2)
    val filtered = source.filter(value => value)
    val chained = filtered.filter(value => value)
    assertColumnar(filtered.plan)
    assertColumnar(chained.plan)
    assertRow(source.map(value => value).plan)
    assertRow(source.map(value => if (value) 1 else 0).plan)
  }

  private def check(values: Array[Boolean], parts: Int): Unit =
    val source = DistCollection.ofBools(values, parts)
    val kept = source.filter(value => value)
    val dropped = source.filter(value => !value).filter(value => !value)
    val emptied = source.filter(value => value).filter(value => !value)
    agree(kept.collect().toVector)
    agree(dropped.collect().toVector)
    agree(emptied.collect().toVector)
    agree(kept.count())
    agree(dropped.count())
    agree(emptied.count())
    agree(kept.take(3))
    agree(dropped.take(1))
    agree(emptied.take(5))
    ()

  private def assertColumnar(plan: Plan[_]): Unit =
    ColumnarPlanner.partitioning(plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(plan).isDefined shouldBe true
    ColumnarPlanner.tryCount(plan).isDefined shouldBe true
    ColumnarPlanner.tryTake(plan, 1).isDefined shouldBe true
    ColumnarPlanner.trySumInt(plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumLong(plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumDouble(plan) shouldBe Option.empty[Double]

  private def assertRow(plan: Plan[_]): Unit =
    ColumnarPlanner.partitioning(plan).isDefined shouldBe false
    ColumnarPlanner.tryRun(plan).isDefined shouldBe false
    ColumnarPlanner.tryCount(plan).isDefined shouldBe false
    ColumnarPlanner.tryTake(plan, 1).isDefined shouldBe false
    ColumnarPlanner.trySumInt(plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumLong(plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumDouble(plan) shouldBe Option.empty[Double]

  private def refuseMap[B](
      values: Array[Boolean],
      mapper: Boolean => B,
      expected: Seq[B],
  ): Unit =
    val hits = new AtomicInteger
    val mapped = DistCollection.ofBools(values, 2).map { value =>
      hits.incrementAndGet()
      mapper(value)
    }

    ColumnarPlanner.partitioning(mapped.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.tryRun(mapped.plan) shouldBe Option.empty[Seq[Partition[B]]]
    ColumnarPlanner.tryCount(mapped.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.tryTake(mapped.plan, 1) shouldBe Option.empty[Seq[B]]
    ColumnarPlanner.trySumInt(mapped.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumLong(mapped.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.trySumDouble(mapped.plan) shouldBe Option.empty[Double]
    hits.get() shouldBe 0

    mapped.collect() shouldBe expected
    hits.get() shouldBe values.length
    hits.set(0)
    mapped.count() shouldBe expected.length.toLong
    hits.get() shouldBe values.length

  private def shape(collection: DistCollection[?]): String =
    ColumnarPlanner.lower(collection.plan).map(_.shape).getOrElse(fail("boolean plan was refused"))

  private def million: Array[Boolean] =
    Array.tabulate(1000000)(index => (index % 3) != 0)

  private def agree[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = true))
      val columnar = body
      ScarletRuntime.get.shuffle.clear()
      ScarletConf.set(saved.copy(columnarExecution = false))
      val rows = body
      columnar shouldBe rows
      columnar
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
