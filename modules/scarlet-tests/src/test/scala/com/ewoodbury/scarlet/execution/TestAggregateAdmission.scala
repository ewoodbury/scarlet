package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Checksums for the aggregate admissions.
 *
 * The witness is an int-pair scan, a key slot filter, `mapIntKey`, then `reduceByKey`. Its tag is
 * [[KernelChoice.IntHashAggregate]] and its checksum matches the row path. The long and string
 * stacks name their tags and publish a batch. A checksum alone can match after a row rerun.
 * `mapValues` to `Long` lowers. A reducer that does not add longs stays empty. A filter stack and
 * `countByKey` of that map stay empty.
 */
class TestAggregateAdmission extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "reduceByKey of a key filter then mapIntKey" should "match the row checksum" in {
    val reduced = DistCollection
      .ofIntPairs(Array(1, 2, 1, 3, 2), Array(10, 20, 30, 40, 5), 2)
      .filterKeys((key: Int) => key > 1)
      .mapIntKey((key: Int) => key + 1)
      .reduceByKey[Int, Int](_ + _)
    val described = Describe.describe(reduced.plan).getOrElse(fail("reduce did not lower"))
    described.choice shouldBe KernelChoice.IntHashAggregate
    described.root.shape shouldBe "HashAggregate(Exchange(MapKey(FilterKey(Scan))))"
    ColumnarPlanner.tryRun(reduced.plan).isDefined shouldBe true
    val checksum = CrossEngineShapes.pairChecksum(reduced.collect())
    checksum shouldBe CrossEngineShapes.pairChecksum(row(reduced.collect()))
    println(s"witness checksum $checksum tag ${described.choice.tag}")
  }

  "mapValues to Long" should "publish the widened pairs and sum long addition" in {
    val pairs = DistCollection.ofIntPairs(Array(1, 2, 1), Array(10, 20, 5), 2)
    val widened = pairs.mapValues[Int, Int, Long]((value: Int) => value.toLong)
    val stacked = Plan.FilterKeysOp(
      Plan.FilterValuesOp(widened.plan, (value: Long) => value > 0L),
      (key: Int) => key > 0,
    )
    val add = (left: Long, right: Long) => left + right
    val keep = (left: Long, _: Long) => left
    published(widened.plan, KernelChoice.IntLongMapValue, "IntLongMapValue(Scan)")
    widened.collect().toList shouldBe row(widened.collect().toList)
    val reduced = widened.reduceByKey[Int, Long](add)
    published(
      reduced.plan,
      KernelChoice.IntLongProjectSum,
      "HashAggregate(Exchange(IntLongMapValue(Scan)))",
    )
    reduced.collect().toMap shouldBe row(reduced.collect().toMap)
    ColumnarPlanner.lower(stacked).map(_.shape) shouldBe Some(
      "FilterKey(FilterValue(IntLongMapValue(Scan)))",
    )
    ColumnarPlanner.lower(Plan.CountByKeyOp(widened.plan)).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(IntLongMapValue(Scan)))",
    )
    ColumnarPlanner.lower(Plan.CountByKeyOp(stacked)) shouldBe None
    ColumnarPlanner.lower(Plan.ReduceByKeyOp(widened.plan, keep)) shouldBe None
    ColumnarPlanner.lower(Plan.ReduceByKeyOp(stacked, add)) shouldBe None
  }

  "a slot stack above long mapValues" should "match the row path" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L, 1L), Array(10L, 20L, 5L), 2)
    val stacked = source
      .map((pair: (Long, Long)) => (pair._1 + 1L, pair._2))
      .mapValues[Long, Long, Long]((value: Long) => value + 1L)
      .filterKeys((key: Long) => key > 0L)
      .filterValues((value: Long) => value > 0L)
    val counted = stacked.countByKey
    published(
      counted.plan,
      KernelChoice.CountByKey,
      "CountByKey(Exchange(FilterValue(FilterKey(MapValue(Project(Scan))))))",
    )
    counted.collect().toMap shouldBe row(counted.collect().toMap)
    val reduced = stacked.reduceByKey[Long, Long](_ + _)
    published(
      reduced.plan,
      KernelChoice.LongHashAggregate,
      "LongHashAggregate(Exchange(FilterValue(FilterKey(MapValue(Project(Scan))))))",
    )
    reduced.collect().toMap shouldBe row(reduced.collect().toMap)
  }

  "a slot stack above string mapValues" should "match the row path" in {
    val source = DistCollection(Seq("red" -> 1, "blue" -> 2, "red" -> 3), 2)
    val stacked = source
      .map((pair: (String, Int)) => (pair._1, pair._2 + 1))
      .mapValues[String, Int, Int]((value: Int) => value + 1)
      .filterKeys((key: String) => key.nonEmpty)
      .filterValues((value: Int) => value > 0)
    val counted = stacked.countByKey
    published(
      counted.plan,
      KernelChoice.CountByKey,
      "CountByKey(Exchange(FilterValue(FilterKey(MapValue(Project(Scan))))))",
    )
    counted.collect().toMap shouldBe row(counted.collect().toMap)
    val reduced = stacked.reduceByKey[String, Int](_ + _)
    published(
      reduced.plan,
      KernelChoice.StringHashAggregate,
      "StringHashAggregate(Exchange(FilterValue(FilterKey(MapValue(Project(Scan))))))",
    )
    reduced.collect().toMap shouldBe row(reduced.collect().toMap)
  }

  /** Tag and a published batch. A row checksum still matches after [[ColumnarFallback]]. */
  private def published(plan: Plan[_], tag: KernelChoice, shape: String): Unit =
    val described = Describe.describe(plan).getOrElse(fail("aggregate did not lower"))
    described.choice shouldBe tag
    described.root.shape shouldBe shape
    ColumnarPlanner.tryRun(plan).isDefined shouldBe true

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
