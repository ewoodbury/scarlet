package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.HashJoin
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Inner join of wide rows on one int or long key.
 *
 * The right side is the build. A null key is dropped. A null payload stays. Hash and broadcast
 * name the same multiset. The plan is not a ledger row. Spill stays off.
 */
class TestWideJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
        spillEnabled = false,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a wide int join" should "match across partitions and keep a null payload" in {
    val left = side(
      WideInput.nullableInts("k", Array(1, 1, 0, 2, 2), Array(true, true, false, true, true)),
      WideInput.nullableInts("q", Array(10, 0, 7, 4, 8), Array(true, false, true, true, true)),
    )
    val right = DistCollection.ofColumns(
      Vector(
        WideInput.ints("k", Array(2, 2, 1, 1)),
        WideInput.nullableLongs("p", Array(5L, 0L, 100L, 7L), Array(true, false, true, true)),
      ),
      2,
    )
    val expected = List(
      Vector(Cell.Int32(1), Cell.Int32(10), Cell.Int64(100L)),
      Vector(Cell.Int32(1), Cell.Int32(10), Cell.Int64(7L)),
      Vector(Cell.Int32(1), Cell.Null, Cell.Int64(100L)),
      Vector(Cell.Int32(1), Cell.Null, Cell.Int64(7L)),
      Vector(Cell.Int32(2), Cell.Int32(4), Cell.Int64(5L)),
      Vector(Cell.Int32(2), Cell.Int32(4), Cell.Null),
      Vector(Cell.Int32(2), Cell.Int32(8), Cell.Int64(5L)),
      Vector(Cell.Int32(2), Cell.Int32(8), Cell.Null),
    )
    val fields = Vector(
      ColumnField("k", ColumnKind.Int32),
      ColumnField("q", ColumnKind.Int32),
      ColumnField("p", ColumnKind.Int64),
    )
    val hashed = left.hashJoinColumns(right, 0, 0)
    val broadcast = left.broadcastJoinColumns(right, 0, 0)
    ColumnarPlanner.lower(hashed.plan).map(_.shape) shouldBe Some("WideHashJoin(Scan,Scan)")
    ColumnarPlanner.lower(broadcast.plan).map(_.shape) shouldBe Some(
      "WideBroadcastJoin(Scan,Scan)",
    )
    HashJoin.resetKernelCallsForTest()
    val hashRows = both(hashed)
    HashJoin.kernelCallsForTest should be > 0
    hashRows.map(_.fields).distinct shouldBe List(fields)
    cells(hashRows) shouldBe cellsOf(expected)
    cells(both(broadcast)) shouldBe cellsOf(expected)
  }

  it should "join a long key the same way" in {
    val left = side(
      WideInput
        .nullableLongs("k", Array(1L, 1L, 0L, 2L, 2L), Array(true, true, false, true, true)),
      WideInput.nullableInts("q", Array(10, 0, 7, 4, 8), Array(true, false, true, true, true)),
    )
    val right = DistCollection.ofColumns(
      Vector(
        WideInput.longs("k", Array(2L, 2L, 1L, 1L)),
        WideInput.nullableLongs("p", Array(5L, 0L, 100L, 7L), Array(true, false, true, true)),
      ),
      2,
    )
    val expected = List(
      Vector(Cell.Int64(1L), Cell.Int32(10), Cell.Int64(100L)),
      Vector(Cell.Int64(1L), Cell.Int32(10), Cell.Int64(7L)),
      Vector(Cell.Int64(1L), Cell.Null, Cell.Int64(100L)),
      Vector(Cell.Int64(1L), Cell.Null, Cell.Int64(7L)),
      Vector(Cell.Int64(2L), Cell.Int32(4), Cell.Int64(5L)),
      Vector(Cell.Int64(2L), Cell.Int32(4), Cell.Null),
      Vector(Cell.Int64(2L), Cell.Int32(8), Cell.Int64(5L)),
      Vector(Cell.Int64(2L), Cell.Int32(8), Cell.Null),
    )
    cells(both(left.joinColumns(right, 0, 0))) shouldBe cellsOf(expected)
    cells(both(left.broadcastJoinColumns(right, 0, 0))) shouldBe cellsOf(expected)
  }

  it should "refuse a string key" in {
    val left = side(WideInput.strings("k", Array("a")), WideInput.ints("q", Array(1)))
    val right = side(WideInput.strings("k", Array("a")), WideInput.ints("q", Array(2)))
    val joined = left.joinColumns(right, 0, 0)
    ColumnarPlanner.lower(joined.plan) shouldBe None
    an[IllegalArgumentException] should be thrownBy joined.collect()
  }

  it should "refuse keys of different kinds" in {
    val left = side(WideInput.ints("k", Array(1)), WideInput.ints("q", Array(1)))
    val right = side(WideInput.longs("k", Array(1L)), WideInput.ints("q", Array(2)))
    val joined = left.joinColumns(right, 0, 0)
    ColumnarPlanner.lower(joined.plan) shouldBe None
    an[IllegalArgumentException] should be thrownBy joined.collect()
  }

  private def side(key: WideInput, payload: WideInput): DistCollection[WideRow] =
    DistCollection.ofColumns(Vector(key, payload), 2)

  private def both(planned: DistCollection[WideRow]): List[WideRow] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = planned.collect().toList
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.collect().toList
    cells(columnar) shouldBe cells(rows)
    columnar

  private def cells(rows: Seq[WideRow]): List[Vector[Cell]] =
    rows.map(_.cells).toList.sortBy(_.toString)

  private def cellsOf(rows: List[Vector[Cell]]): List[Vector[Cell]] =
    rows.sortBy(_.toString)
