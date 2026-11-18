package com.ewoodbury.scarlet.execution

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `SCARLET_TRACE_LOWERING` prints one refusal line and nothing for a plan that lowers. The flag
 * does not change the `lower` / `tryRun` result.
 */
class TestLoweringTrace extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  private val sortLine =
    "lowering refused node=SortByIntKeyOp kind=Pairs lowering=SortLowering"

  private val mapLine =
    "lowering refused node=MapOp kind=none lowering=ColumnarPlanner"

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "lowering trace" should "print one line for a refused plan and none for a plan that lowers" in {
    val kept = DistCollection(Seq(1, 2, 3, 4), 2).filter(_ > 1).plan
    val sorted = DistCollection(Seq(1 -> 2, 3 -> 4), 1)
      .reduceByKey[Int, Int](_ + _)
      .sortByIntKey
      .plan
    val widened = DistCollection(Seq(1, 2, 3), 1).map(_.toLong).plan
    val offKept = withFlag(false)(values(kept))

    withFlag(false)(ColumnarPlanner.lower(sorted)) shouldBe None
    linesOf(withFlag(false)(stderr {
      val _ = ColumnarPlanner.lower(sorted)
      val _ = ColumnarPlanner.lower(widened)
      val _ = ColumnarPlanner.lower(kept)
    })) shouldBe Vector.empty[String]

    linesOf(withFlag(true)(stderr {
      val _ = ColumnarPlanner.lower(sorted)
      val _ = ColumnarPlanner.lower(kept)
      val _ = ColumnarPlanner.lower(sorted)
    })) shouldBe Vector(sortLine, sortLine)

    linesOf(withFlag(true)(stderr {
      val _ = ColumnarPlanner.lower(widened)
    })) shouldBe Vector(mapLine)

    val left = DistCollection(Seq("a" -> 1), 1).plan
    val right = DistCollection(Seq("b" -> 2), 1).plan
    val outer = Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge))
    val outerLines = withFlag(true)(stderr { val _ = ColumnarPlanner.lower(outer) })
    linesOf(outerLines) shouldBe Vector.empty[String]
    withFlag(true)(ColumnarPlanner.lower(outer)).isDefined shouldBe true

    linesOf(withFlag(true)(stderr { val _ = values(kept) })) shouldBe Vector.empty[String]
    withFlag(true)(values(kept)) shouldBe offKept
    withFlag(true)(ColumnarPlanner.lower(sorted)) shouldBe None
    withFlag(false)(values(kept)) shouldBe offKept
  }

  it should "stay quiet when columnar execution is off" in {
    val kept = DistCollection(Seq(1, 2, 3), 1).filter(_ > 0).plan
    ScarletConf.set(originalConf.copy(columnarExecution = false, traceLowering = true))
    linesOf(stderr { val _ = ColumnarPlanner.lower(kept) }) shouldBe Vector.empty[String]
    ColumnarPlanner.lower(kept) shouldBe None
  }

  private def withFlag[A](enabled: Boolean)(body: => A): A =
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarExecution = true, traceLowering = enabled))
    try body
    finally ScarletConf.set(saved)

  private def values[A](plan: Plan[A]): Option[List[A]] =
    ColumnarPlanner.tryRun(plan).map(_.flatMap(_.data).toList)

  private def stderr(body: => Unit): String =
    val buffer = new ByteArrayOutputStream()
    val stream = new PrintStream(buffer, true, StandardCharsets.UTF_8)
    try Console.withErr(stream)(body)
    finally stream.close()
    buffer.toString(StandardCharsets.UTF_8)

  private def linesOf(text: String): Vector[String] =
    if (text.isEmpty) then Vector.empty[String]
    else
      val parts = text.split("\n", -1).toVector
      if (parts.lastOption.contains("")) then parts.dropRight(1) else parts
