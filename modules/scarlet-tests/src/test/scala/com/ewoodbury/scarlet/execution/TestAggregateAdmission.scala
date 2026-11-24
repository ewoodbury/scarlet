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
 * [[KernelChoice.IntHashAggregate]] and its checksum matches the row path. `mapValues` to `Long`
 * does not lower.
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

  "mapValues to Long" should "stay on the row path" in {
    val pairs = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2).plan
    val widened = Plan.MapValuesOp(pairs, (value: Int) => value.toLong)
    val stacked = Plan.FilterKeysOp(
      Plan.FilterValuesOp(widened, (value: Long) => value > 0L),
      (key: Int) => key > 0,
    )
    val add = (left: Long, right: Long) => left + right
    ColumnarPlanner.lower(widened) shouldBe None
    ColumnarPlanner.lower(stacked) shouldBe None
    ColumnarPlanner.lower(Plan.CountByKeyOp(widened)) shouldBe None
    ColumnarPlanner.lower(Plan.CountByKeyOp(stacked)) shouldBe None
    ColumnarPlanner.lower(Plan.ReduceByKeyOp(widened, add)) shouldBe None
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
    counted.collect().toMap shouldBe row(counted.collect().toMap)
    val reduced = stacked.reduceByKey[Long, Long](_ + _)
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
    counted.collect().toMap shouldBe row(counted.collect().toMap)
    val reduced = stacked.reduceByKey[String, Int](_ + _)
    reduced.collect().toMap shouldBe row(reduced.collect().toMap)
  }

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
