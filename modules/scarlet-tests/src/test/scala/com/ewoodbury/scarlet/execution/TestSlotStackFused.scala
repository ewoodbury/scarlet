package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnBatch, CountSum, FoldedCount, HashCount, HashReduce}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Two slot filters above one `mapIntKey` run `byKeySelected`.
 *
 * [[FilterChain.selected]] still returns `None` for one map and for one slot directly above it.
 * That one-filter count stays on [[FoldedCount]]. A direct [[HashCount.byKey]] does not move
 * [[HashCount.byKeySelectedCallsForTest]]. One filter above two key maps still runs
 * [[HashCount.byKey]]: deleting [[MapIntKey.inTree]] would send it through `byKeySelected`.
 */
class TestSlotStackFused extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    HashCount.resetByKeyForTest()
    HashReduce.resetByKeyForTest()
    CountSum.resetByKeyForTest()
    FoldedCount.resetPassesForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "countByKey of two slots above mapIntKey" should "run byKeySelected" in {
    val counted = stack.countByKey
    val found = published(counted.plan)
    HashCount.byKeySelectedCallsForTest should be > 0
    HashCount.byKeyCallsForTest shouldBe 0
    FoldedCount.passesForTest shouldBe 0
    found.toMap shouldBe row(counted.collect().toMap)
    CrossEngineShapes.pairChecksum(found) shouldBe 219L
  }

  "reduceByKey of two slots above mapIntKey" should "run byKeySelected" in {
    val reduced = stack.reduceByKey[Int, Int]((left, right) => left + right)
    val found = published(reduced.plan)
    HashReduce.byKeySelectedCallsForTest should be > 0
    HashReduce.byKeyCallsForTest shouldBe 0
    found.toMap shouldBe row(reduced.collect().toMap)
    CrossEngineShapes.pairChecksum(found) shouldBe 277L
  }

  "countSumByKey of two slots above mapIntKey" should "run byKeySelected" in {
    val summed = stack.countSumByKey
    val found = published(summed.plan)
    CountSum.byKeySelectedCallsForTest should be > 0
    CountSum.byKeyCallsForTest shouldBe 0
    val grouped = found.map { case (key, count, sum) => key -> (count, sum) }.toMap
    grouped shouldBe row(
      summed
        .collect()
        .map { case (key, count, sum) =>
          key -> (count, sum)
        }
        .toMap,
    )
    CrossEngineShapes.tripleChecksum(found) shouldBe 6849L
  }

  "byKey" should "leave the selected counters at zero" in {
    val _ = HashCount.byKey(Vector.empty[ColumnBatch], 1, 1, 8)
    val add: HashReduce.IntBinOp = (left, right) => left + right
    val _ = HashReduce.byKey(Vector.empty[ColumnBatch], 1, 1, 8, add)
    val _ = CountSum.byKey(Vector.empty[ColumnBatch], 1, 1, 8)
    HashCount.byKeyCallsForTest should be > 0
    HashReduce.byKeyCallsForTest should be > 0
    CountSum.byKeyCallsForTest should be > 0
    HashCount.byKeySelectedCallsForTest shouldBe 0
    HashReduce.byKeySelectedCallsForTest shouldBe 0
    CountSum.byKeySelectedCallsForTest shouldBe 0
  }

  "one filter above mapIntKey" should "keep the mapped count off byKeySelected" in {
    val counted = oneFilter.countByKey
    val found = published(counted.plan)
    FoldedCount.passesForTest should be > 0
    HashCount.byKeySelectedCallsForTest shouldBe 0
    HashCount.byKeyCallsForTest shouldBe 0
    found.toMap shouldBe row(counted.collect().toMap)

    HashReduce.resetByKeyForTest()
    val reduced = oneFilter.reduceByKey[Int, Int]((left, right) => left + right)
    val sums = published(reduced.plan)
    HashReduce.byKeySelectedCallsForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    sums.toMap shouldBe row(reduced.collect().toMap)

    CountSum.resetByKeyForTest()
    val summed = oneFilter.countSumByKey
    val groups = published(summed.plan)
    CountSum.byKeySelectedCallsForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    groups.map { case (key, count, sum) => key -> (count, sum) }.toMap shouldBe row(
      summed.collect().map { case (key, count, sum) => key -> (count, sum) }.toMap,
    )
  }

  "one filter above two mapIntKey nodes" should "stay on byKey" in {
    val counted = pairs
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key * 2)
      .filterKeys((key: Int) => key > 2)
      .countByKey
    val found = published(counted.plan)
    HashCount.byKeyCallsForTest should be > 0
    HashCount.byKeySelectedCallsForTest shouldBe 0
    found.toMap shouldBe row(counted.collect().toMap)
  }

  /** Two key or value slots over one `mapIntKey`. Keys 1 and 2 drop. */
  private def stack: DistCollection[(Int, Int)] =
    pairs
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .filterValues((value: Int) => value >= 20)

  /** One key slot over one `mapIntKey`. The mapped fold owns this shape. */
  private def oneFilter: DistCollection[(Int, Int)] =
    pairs.mapIntKey((key: Int) => key + 1).filterKeys((key: Int) => key > 2)

  private def pairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)

  private def published[A](plan: Plan[A]): Seq[A] =
    ScarletRuntime.get.shuffle.clear()
    val parts =
      ColumnarPlanner.tryRun(plan).getOrElse(fail("columnar path did not publish a batch"))
    parts.flatMap(_.data.toSeq)

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
