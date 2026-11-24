package com.ewoodbury.scarlet.execution

import scala.io.Source
import scala.util.Using

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{
  ColumnBatch,
  HashCount,
  HashReduce,
  StringHashCount,
  StringHashReduce,
}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Two slot filters above one string `mapValues` run `byKeySelected`.
 *
 * [[StringHashCount.byKey]] and [[StringHashReduce.byKey]] do not move the selected counters. The
 * allowlist pins for these two labels are `byKeySelected`. A blank pin is not that route.
 */
class TestStringSlotStack extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    StringHashCount.resetByKeyForTest()
    StringHashReduce.resetByKeyForTest()
    HashCount.resetByKeyForTest()
    HashReduce.resetByKeyForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "countByKey of two slots above string mapValues" should "run byKeySelected" in {
    val counted = stack.countByKey
    val found = published(counted.plan)
    StringHashCount.byKeySelectedCallsForTest should be > 0
    StringHashCount.byKeyCallsForTest shouldBe 0
    HashCount.byKeyCallsForTest shouldBe 0
    HashCount.byKeySelectedCallsForTest shouldBe 0
    found.toMap shouldBe row(counted.collect().toMap)
  }

  "reduceByKey of two slots above string mapValues" should "run byKeySelected" in {
    val reduced = stack.reduceByKey[String, Int]((left, right) => left + right)
    val found = published(reduced.plan)
    StringHashReduce.byKeySelectedCallsForTest should be > 0
    StringHashReduce.byKeyCallsForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    HashReduce.byKeySelectedCallsForTest shouldBe 0
    found.toMap shouldBe row(reduced.collect().toMap)
  }

  "byKey" should "leave the string selected counters at zero" in {
    val _ = StringHashCount.byKey(Vector.empty[ColumnBatch], 1, 1, 8)
    val plus: (Int, Int) => Int = (left, right) => left + right
    val _ = StringHashReduce.byKey(Vector.empty[ColumnBatch], 1, 1, 8, plus)
    StringHashCount.byKeyCallsForTest should be > 0
    StringHashReduce.byKeyCallsForTest should be > 0
    StringHashCount.byKeySelectedCallsForTest shouldBe 0
    StringHashReduce.byKeySelectedCallsForTest shouldBe 0
  }

  "the string slot stack pins" should "name byKeySelected" in {
    val pins = allowlistLines.filter(_.contains("string mapValues")).map(entry)
    pins shouldBe Seq("byKeySelected", "byKeySelected")
  }

  /** Two key or value slots over one string `mapValues`. `""` and `b` drop. */
  private def stack: DistCollection[(String, Int)] =
    pairs
      .map((pair: (String, Int)) => (pair._1, pair._2 + 1))
      .mapValues[String, Int, Int]((value: Int) => value + 1)
      .filterKeys((key: String) => key.nonEmpty)
      .filterValues((value: Int) => value > 3)

  private def pairs: DistCollection[(String, Int)] =
    DistCollection(Seq("" -> 1, "a" -> 2, "b" -> 0, "a" -> 4), 2)

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
