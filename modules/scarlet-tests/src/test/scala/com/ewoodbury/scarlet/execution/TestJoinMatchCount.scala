package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Row-path equality for an int shuffle-hash count.
 *
 * The cross-engine shape is the two filters, keys `row % 1024`, and ten probe copies of each
 * key. An owned int-pair count multiplies per-key counts. `collect` still writes the matches.
 */
class TestJoinMatchCount extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 64,
        defaultShufflePartitions = 4,
        threadPoolSize = 2,
        spillEnabled = false,
      ),
    )

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ScarletRuntime.get.shuffle.clear()
    ()

  "filter-join" should "match the row path on the cross-engine shape" in {
    val rows = 4096
    val groups = 1024
    val copies = 10
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = Array.tabulate(rows)(row => row - 2000)
    val left = DistCollection
      .ofIntPairs(keys, values, 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val width = copies * groups
    val right = DistCollection.ofIntPairs(
      Array.tabulate(width)(row => row % groups),
      Array.tabulate(width)(row => row % 100),
      2,
    )
    val joined = left.join(right)
    val columnarRows = joined.collect().toSeq
    val rowRows = row(joined.collect().toSeq)

    columnarRows should contain theSameElementsAs rowRows
    joined.count() shouldBe rowRows.size.toLong
    joined.count() shouldBe row(joined.count())
  }

  it should "count an empty build, a miss, and duplicate build keys" in {
    val present = DistCollection.ofIntPairs(Array(1, 1, 2), Array(10, 11, 20), 2)
    val empty = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
    val miss = DistCollection.ofIntPairs(Array(8, 9), Array(1, 2), 2)
    val duplicated = DistCollection.ofIntPairs(Array(1, 1), Array(100, 101), 1)

    empty.join(present).count() shouldBe 0L
    empty.join(present).count() shouldBe row(empty.join(present).count())
    present.join(empty).count() shouldBe 0L
    present.join(empty).count() shouldBe row(present.join(empty).count())
    present.join(miss).count() shouldBe 0L
    present.join(miss).count() shouldBe row(present.join(miss).count())
    present.join(duplicated).count() shouldBe 4L
    present.join(duplicated).count() shouldBe row(present.join(duplicated).count())
  }

  it should "match the cross-engine cardinality at both scales" in {
    counted(1000000) shouldBe 3330360L
    counted(10000000) shouldBe 33281390L
  }

  it should "count a null build value as a match" in {
    val left = DistCollection(
      Seq(intPair(boxedInt(1), boxedInt(10)), intPair(boxedInt(1), absentInt)),
      1,
    )
    val right = DistCollection.ofIntPairs(Array(1), Array(7), 1)
    val joined = left.join(right)

    joined.count() shouldBe 2L
    joined.count() shouldBe row(joined.count())
    joined.collect().toSeq should contain theSameElementsAs row(joined.collect().toSeq)
  }

  /** Same generator as [[CrossEngineBench]]: keys `row % 1024`, ten probe copies, two filters. */
  private def counted(rows: Int): Long =
    val groups = 1024
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcg(rows)
    val left = DistCollection
      .ofIntPairs(keys, values, 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val width = 10 * groups
    val right = DistCollection.ofIntPairs(
      Array.tabulate(width)(row => row % groups),
      Array.tabulate(width)(row => row % 100),
      2,
    )
    left.join(right).count()

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def lcg(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = 0x12345678
    while (index < count) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    generated

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
