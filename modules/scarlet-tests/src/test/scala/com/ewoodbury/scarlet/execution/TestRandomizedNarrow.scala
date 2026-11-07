package com.ewoodbury.scarlet.execution

import scala.util.Random

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Randomized differential check over the narrow ops.
 *
 * For each seed, random small inputs are run through the columnar path and the row path, and the
 * results must agree. The inputs vary the row count, the partition count, duplicate keys, negative
 * keys and values, and a small string pool. This is not a full property-based library; it is a
 * fast generator that keeps the two layouts honest while the column representation changes.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Any"))
class TestRandomizedNarrow extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get
  private val seeds = 1 to 40
  private val stringPool = Array("a", "bb", "ccc", "dddd", "eee", "")

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 2))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "narrow ops" should "agree with the row path for every seed" in {
    seeds.foreach { seed =>
      val random = new Random(seed)
      val rows = 1 + random.nextInt(300)
      val partitions = 1 + random.nextInt(4)
      val values = Array.fill(rows)(random.nextInt(2001) - 1000)
      val keys = Array.fill(rows)(random.nextInt(101) - 50)
      val texts = Array.fill(rows)(stringPool(random.nextInt(stringPool.length)))

      withClue(s"seed $seed rows $rows partitions $partitions ints: ") {
        val ints = DistCollection.ofInts(values, partitions).filter(_ % 3 != 0).map(_ * 2)
        columnar(ints) shouldBe row(ints.collect())
      }

      withClue(s"seed $seed rows $rows partitions $partitions int pairs: ") {
        val pairs = DistCollection
          .ofIntPairs(keys, values, partitions)
          .filterKeys((key: Int) => key % 3 != 0)
          .filterValues((value: Int) => value >= 0)
        columnar(pairs.reduceByKey[Int, Int](_ + _)).toMap shouldBe row(
          pairs.reduceByKey[Int, Int](_ + _).collect(),
        ).toMap
        groupedInts(columnar(pairs.groupByKey)) shouldBe groupedInts(
          row(pairs.groupByKey.collect()),
        )
        pairs.count() shouldBe rowCount(pairs.count())
      }

      withClue(s"seed $seed rows $rows partitions $partitions long pairs: ") {
        val longs = DistCollection
          .ofLongPairs(keys.map(_.toLong), values.map(_.toLong), partitions)
          .filterKeys((key: Long) => key % 3 != 0)
          .filterValues((value: Long) => value >= 0)
        columnar(longs.reduceByKey[Long, Long](_ + _)).toMap shouldBe row(
          longs.reduceByKey[Long, Long](_ + _).collect(),
        ).toMap
      }

      withClue(s"seed $seed rows $rows partitions $partitions strings: ") {
        val strings = DistCollection.ofStrings(texts, partitions).filter(_.length > 2)
        columnar(strings) shouldBe row(strings.collect())
      }

      withClue(s"seed $seed rows $rows partitions $partitions sort: ") {
        val sortable = DistCollection.ofIntPairs(keys, values, partitions).sortByIntKey
        columnar(sortable) shouldBe row(sortable.collect())
      }
    }
  }

  private def groupedInts(rows: Seq[(Int, Iterable[Int])]): Map[Int, List[Int]] =
    rows.map { case (key, values) => key -> values.toList.sorted }.toMap

  private def columnar[A](collection: DistCollection[A]): Seq[A] =
    val ran = ColumnarPlanner
      .tryRun(collection.plan)
      .getOrElse(fail(s"stayed on the row path: ${collection.plan}"))
    ran.flatMap(_.data).toList

  private def rowCount(body: => Long): Long =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def row[A](body: => Iterable[A]): Seq[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body.toList
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
