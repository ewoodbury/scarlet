package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * The sort harness checksum matches `collect().map(key * 31 + value).sum` on the rows that action
 * sees. Columnar and row both use [[SortChecksum]].
 */
class TestSortChecksum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
        spillEnabled = true,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "the sort checksum" should "match the collected sum on a filtered int sort" in {
    val sorted = filtered(rows = 4000, nullAt = -1)
    agree(sorted)
  }

  it should "match the collected sum when a null value is dropped by the value filter" in {
    val sorted = filtered(rows = 4000, nullAt = 5)
    agree(sorted)
  }

  it should "match the collected sum on the row path" in {
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val sorted = filtered(rows = 800, nullAt = -1)
    agree(sorted)
  }

  private def agree(sorted: DistCollection[(Int, Int)]): Unit =
    val boxed = sorted.collect().map { case (key, value) => key * 31L + value }.sum
    SortChecksum.of(sorted) shouldBe boxed

  private def filtered(rows: Int, nullAt: Int): DistCollection[(Int, Int)] =
    val keys = Array.tabulate(rows)(identity)
    val values = Array.tabulate(rows)(row => row - rows / 2)
    val data =
      if (nullAt < 0) then DistCollection.ofIntPairs(keys, values, 2)
      else DistCollection(withNull(keys, values, nullAt), 2)
    data
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .sortByIntKey

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def withNull(keys: Array[Int], values: Array[Int], at: Int): Seq[(Int, Int)] =
    Seq.tabulate(keys.length) { row =>
      if (row == at) then
        Tuple2(java.lang.Integer.valueOf(keys(row)), null).asInstanceOf[(Int, Int)]
      else keys(row) -> values(row)
    }
