package com.ewoodbury.scarlet.execution

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Owned `countByKey` against the encoded fold.
 *
 * The action hashes [[com.ewoodbury.scarlet.core.IntPairArrays]] in place, so these cases compare
 * that result with [[FoldedCount.count]] on a copied batch. Null keys and null values cannot live
 * in those arrays. [[TestCountByKey]] keeps them on the boxed path.
 */
class TestOwnedCountByKey extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    FoldedCount.resetPassesForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "an owned count" should "match the encoded fold on random key and value shapes" in {
    val random = new Random(0xc9c9)
    val parts = Vector(0, 1, 17, 8, 3, 64).map(rows => draw(random, rows))
    val two = List(
      Selection.Step.Int32(0, (key: Int) => key % 3 != 0),
      Selection.Step.Int32(1, (value: Int) => value >= 0),
    )
    val keyOnly = List(Selection.Step.Int32(0, (key: Int) => key % 2 != 0))
    val valueOnly = List(Selection.Step.Int32(1, (value: Int) => value > 0))
    List(1, 4).foreach { width =>
      same(parts, two, width)
      same(parts, keyOnly, width)
      same(parts, valueOnly, width)
      sameUnmapped(parts, Nil, width)
      sameUnmapped(parts, keyOnly, width)
    }
  }

  it should "match the encoded fold for one partition of duplicate keys" in {
    val parts = Vector((Array(4, 4, 4, 9, 4), Array(1, -2, 3, 4, 0)))
    val steps = List(Selection.Step.Int32(1, (value: Int) => value > 0))
    same(parts, steps, width = 1)
  }

  it should "match the encoded fold when one key lands on two partitions" in {
    val parts = Vector((Array(5, 6), Array(1, -1)), (Array(5, 6, 7, 5), Array(2, 3, 0, 4)))
    val steps = List(
      Selection.Step.Int32(0, (key: Int) => key != 7),
      Selection.Step.Int32(1, (value: Int) => value >= 0),
    )
    same(parts, steps, width = 4)
  }

  it should "match the encoded fold for an empty partition and a single key" in {
    val steps = List(Selection.Step.Int32(0, (key: Int) => key > 0))
    same(Vector((Array.empty[Int], Array.empty[Int])), steps, width = 4)
    same(Vector((Array(7), Array(1))), steps, width = 1)
    same(Vector((Array(7), Array(1)), (Array.empty[Int], Array.empty[Int])), steps, width = 4)
  }

  it should "publish counts that do not alias the source key array" in {
    val keys = Array(1, 2, 1, 3)
    val values = Array(4, 5, 6, 7)
    val steps = List(Selection.Step.Int32(0, (key: Int) => key > 0))
    val counted = FoldedCount
      .countOwned(Vector((keys, values)), steps, numPartitions = 1, parallelism = 1, batchSize = 8)
      .getOrElse(fail("owned count declined"))
    val batch = counted.headOption.getOrElse(fail("owned count returned no batch"))
    val column = batch.columns.headOption match
      case Some(keysOut: Int32Column) => keysOut
      case other => fail(s"expected an Int32 key column, found $other")
    column.values should not be theSameInstanceAs(keys)
    val published = column.values.toList
    keys(0) = 99
    column.values.toList shouldBe published
  }

  it should "count owned shapes in the index path's key order without encoding" in {
    // Both filters stay on: key % 3 != 0 and value >= 0. 9 and 6 are multiples of 3.
    assertOwned(Array(4, 4, 4, 9), Array(1, 2, 3, 4), 1) shouldBe Map(4 -> 3)
    assertOwned(Array(5, 6, 5, 7), Array(1, 2, 3, 4), 4) shouldBe Map(5 -> 2, 7 -> 1)
    assertOwned(Array(5, 6, 5), Array(1, 2, 3), 2) shouldBe Map(5 -> 2)
    assertOwned(Array(7), Array(1), 1) shouldBe Map(7 -> 1)
    assertOwned(Array.empty[Int], Array.empty[Int], 1) shouldBe Map.empty[Int, Int]
    assertOwned(Array.empty[Int], Array.empty[Int], 4) shouldBe Map.empty[Int, Int]
    val random = new Random(0x12345678)
    val drawn = draw(random, 200)
    val _ = assertOwned(drawn._1, drawn._2, 4)
  }

  it should "skip the value function and the encode when mapValues is peeled" in {
    val calls = new AtomicInteger
    val counted = DistCollection
      .ofIntPairs(Array(1, 2, 1), Array(3, 4, 5), 2)
      .mapValues { (value: Int) =>
        val _ = calls.incrementAndGet()
        value + 1
      }
      .countByKey
    val found = collectOwned(counted)
    calls.get shouldBe 0
    countsOf(found) shouldBe Map(1 -> 2, 2 -> 1)
  }

  it should "fold a boxed pair scan that is not an owned array" in {
    val source = DistCollection(Seq(1 -> 4, 1 -> 5, 2 -> 6), 2)
    source.plan match
      case Plan.Source(partitions, _) =>
        partitions.foreach { partition =>
          partition.data match
            case _: com.ewoodbury.scarlet.core.IntPairArrays =>
              fail("boxed pairs were stored as owned arrays")
            case _ => ()
        }
      case other => fail(s"boxed source lowered as $other")
    val counted = source.filterKeys((key: Int) => key > 0).countByKey
    FoldedCount.resetPassesForTest()
    val found = countsOf(counted.collect())
    found shouldBe Map(1 -> 2, 2 -> 1)
    found shouldBe countsOf(row(counted.collect()))
    FoldedCount.passesForTest shouldBe 1
  }

  it should "refuse an over-budget owned filter and accept an empty source at budget zero" in {
    val source = DistCollection
      .ofIntPairs(Array(1, 1, 2), Array(4, 5, 6), 2)
      .filterKeys((key: Int) => key > 0)
    List(false, true).foreach { enabled =>
      ScarletConf.set(
        originalConf.copy(
          columnarExecution = true,
          spillEnabled = enabled,
          spillBudgetBytes = 1L,
          columnarBatchSize = 8,
          threadPoolSize = 2,
          defaultShufflePartitions = 4,
        ),
      )
      ScarletRuntime.get.shuffle.clear()
      val refused = the[ClosedKernelRefusal] thrownBy source.countByKey.collect()
      refused.budget shouldBe 1L
      refused.bytes should be > refused.budget
    }

    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        spillEnabled = false,
        spillBudgetBytes = 0L,
        columnarBatchSize = 8,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    FoldedCount.resetPassesForTest()
    val empty = DistCollection
      .ofIntPairs(Array.empty[Int], Array.empty[Int], 1)
      .filterKeys((key: Int) => key > 0)
      .countByKey
    countsOf(empty.collect()) shouldBe Map.empty[Int, Int]
    FoldedCount.passesForTest shouldBe 1
  }

  private def assertOwned(
      keys: Array[Int],
      values: Array[Int],
      parts: Int,
  ): Map[Int, Int] =
    val counted = DistCollection
      .ofIntPairs(keys, values, parts)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .countByKey
    val found = collectOwned(counted)
    found shouldBe hashOrder(counted)
    val counts = countsOf(found)
    counts shouldBe countsOf(row(counted.collect()))
    counts

  private def collectOwned(counted: DistCollection[(Int, Int)]): List[(Int, Int)] =
    FoldedCount.resetPassesForTest()
    val found = ColumnMetrics.withSink(new CountingSink) {
      val rows = counted.collect().toList
      ColumnMetrics.primitiveEncodeCount shouldBe 0L
      rows
    }
    FoldedCount.passesForTest shouldBe 1
    found

  private def same(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      width: Int,
  ): Unit =
    val encoded = parts.map { (keys, values) => BatchCodec.copiedIntPairs(keys, values) }
    val viaEncode = FoldedCount
      .count(encoded, steps, width, parallelism = 2, batchSize = 8)
      .getOrElse(fail("encoded count declined"))
    val viaOwned = FoldedCount
      .countOwned(parts, steps, width, parallelism = 2, batchSize = 8)
      .getOrElse(fail("owned count declined"))
    decoded(viaOwned) shouldBe decoded(viaEncode)

  private def sameUnmapped(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      width: Int,
  ): Unit =
    val encoded = parts.map { (keys, values) => BatchCodec.copiedIntPairs(keys, values) }
    val viaEncode = FoldedCount
      .countUnmapped(encoded, steps, width, parallelism = 2, batchSize = 8)
      .getOrElse(fail("encoded unmapped count declined"))
    val viaOwned = FoldedCount
      .countOwnedUnmapped(parts, steps, width, parallelism = 2, batchSize = 8)
      .getOrElse(fail("owned unmapped count declined"))
    decoded(viaOwned) shouldBe decoded(viaEncode)

  private def hashOrder(counted: DistCollection[(Int, Int)]): List[(Int, Int)] =
    val plan = counted.plan
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("countByKey did not lower"))
    val (source, child) = (plan, op) match
      case (
            Plan.CountByKeyOp(inner),
            PhysicalOp.CountByKey(PhysicalOp.Exchange(exchangeChild)),
          ) =>
        (inner, exchangeChild)
      case _ => fail(s"countByKey lowered to ${op.shape}")
    val ordinals = ColumnUse.ordinals(plan, op)
    val selected = FilterChain
      .selected(source, child, None, preserveAbsence = false, ordinals)
      .getOrElse(fail("filter stack did not select"))
    val conf = ScarletConf.get
    decoded(
      HashCount.byKeySelected(
        selected,
        conf.defaultShufflePartitions,
        conf.threadPoolSize,
        conf.columnarBatchSize,
      ),
    )

  private def decoded(batches: Vector[ColumnBatch]): List[(Int, Int)] =
    batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)).toList

  private def countsOf(rows: Iterable[(Int, Int)]): Map[Int, Int] =
    rows.foldLeft(Map.empty[Int, Int]) { (seen, row) =>
      require(!seen.contains(row._1), s"duplicate count for ${row._1}")
      seen.updated(row._1, row._2)
    }

  private def draw(random: Random, rows: Int): (Array[Int], Array[Int]) =
    val keys = Array.tabulate(rows)(_ => random.nextInt(32) - 4)
    val values = Array.tabulate(rows)(_ => random.nextInt(21) - 5)
    (keys, values)

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
