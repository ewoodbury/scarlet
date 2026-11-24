package com.ewoodbury.scarlet.execution

import scala.io.Source
import scala.util.Using

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Two slot filters above one long `mapValues` run `byKeySelected`.
 *
 * [[LongHashCount.byKey]] and [[LongHashReduce.byKey]] do not move the selected counters.
 * [[LongHashReduce.byKeySelected]] does not increment [[LongHashReduce.byKeyCallsForTest]], so a
 * blank allowlist pin does not name this route. The pins for these two labels are `byKeySelected`.
 * A checksum with a blank pin is not that route. The join labels that are blank stay blank.
 */
class TestLongSlotStack extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    LongHashCount.resetByKeyForTest()
    LongHashReduce.resetByKeyForTest()
    HashCount.resetByKeyForTest()
    HashReduce.resetByKeyForTest()
    StringHashCount.resetByKeyForTest()
    StringHashReduce.resetByKeyForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "countByKey of two slots above long mapValues" should "run byKeySelected" in {
    val counted = stack.countByKey
    val found = published(counted.plan)
    LongHashCount.byKeySelectedCallsForTest should be > 0
    LongHashCount.byKeyCallsForTest shouldBe 0
    otherSelected shouldBe 0
    otherByKey shouldBe 0
    found.toMap shouldBe row(counted.collect().toMap)
  }

  "reduceByKey of two slots above long mapValues" should "run byKeySelected" in {
    val reduced = stack.reduceByKey[Long, Long]((left, right) => left + right)
    val found = published(reduced.plan)
    LongHashReduce.byKeySelectedCallsForTest should be > 0
    LongHashReduce.byKeyCallsForTest shouldBe 0
    otherSelected shouldBe 0
    otherByKey shouldBe 0
    found.toMap shouldBe row(reduced.collect().toMap)
  }

  "byKey" should "leave the long selected counters at zero" in {
    val _ = LongHashCount.byKey(Vector.empty[ColumnBatch], 1, 1, 8)
    val plus: LongHashReduce.LongBinOp = (left, right) => left + right
    val _ = LongHashReduce.byKey(Vector.empty[ColumnBatch], 1, 1, 8, plus)
    LongHashCount.byKeyCallsForTest should be > 0
    LongHashReduce.byKeyCallsForTest should be > 0
    LongHashCount.byKeySelectedCallsForTest shouldBe 0
    LongHashReduce.byKeySelectedCallsForTest shouldBe 0
  }

  "the long slot stack pins" should "name byKeySelected" in {
    val pins = allowlistLines.filter(_.contains("long mapValues")).map(entry)
    pins shouldBe Seq("byKeySelected", "byKeySelected")
  }

  "join allowlist rows" should "stay blank" in {
    val pins = allowlistLines.filter(_.contains("join")).map(entry)
    pins should have size 6
    pins.foreach(pin => pin shouldBe "")
  }

  /** Two key or value slots over one long `mapValues`. Mapped key `-1` and values `2` and `3` drop. */
  private def stack: DistCollection[(Long, Long)] =
    pairs
      .map((pair: (Long, Long)) => (pair._1 + 1L, pair._2))
      .mapValues[Long, Long, Long]((value: Long) => value + 1L)
      .filterKeys((key: Long) => key > 0L)
      .filterValues((value: Long) => value > 3L)

  private def pairs: DistCollection[(Long, Long)] =
    DistCollection.ofLongPairs(Array(0L, 1L, 1L, -2L), Array(1L, 2L, 10L, 5L), 2)

  private def otherSelected: Int =
    HashCount.byKeySelectedCallsForTest + HashReduce.byKeySelectedCallsForTest +
      StringHashCount.byKeySelectedCallsForTest + StringHashReduce.byKeySelectedCallsForTest

  private def otherByKey: Int =
    HashCount.byKeyCallsForTest + HashReduce.byKeyCallsForTest +
      StringHashCount.byKeyCallsForTest + StringHashReduce.byKeyCallsForTest

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

  private def allowlistLines: Seq[String] =
    val stream = Option(getClass.getClassLoader.getResourceAsStream("admission-allowlist.txt"))
      .getOrElse(fail("admission-allowlist.txt is missing"))
    Using.resource(Source.fromInputStream(stream, "UTF-8")) { source =>
      source.mkString
        .replace("\r\n", "\n")
        .split("\n")
        .toSeq
        .map(_.trim)
        .filter(line => line.nonEmpty && !line.startsWith("#"))
    }

  /** The fused entry. A blank pin is empty. */
  private def entry(line: String): String =
    val fields = line.split("\t", -1)
    if (fields.length < 3) then "" else fields(2)
