package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Prefix decode for the plans `tryTake` accepts. One batch per column kind on that path, at
 * n = 1 and n = 3, compared with `decodePrefixFor`, `tryTake`, and the row-path `take`.
 *
 * A clear key bit on an int-pair sort is the split the old strict prefix rejected.
 * `decodeIntPairs` still throws. `decodePrefixFor` keeps the null and matches the row path.
 * A clear bit on a long pair and on a string pair stays null on both the product prefix and
 * the row path, because those decoders are shared. An outer join is not in `tryTake`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestDecodePrefix extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "decodePrefixFor" should "match tryTake and the row path for each take-eligible kind" in {
    agree("ints", DistCollection.ofInts(Array(10, 20, 30, 40, 50), 3))
    agree("longs", DistCollection.ofLongs(Array(10L, 20L, 30L, 40L, 50L), 3))
    agree("doubles", DistCollection.ofDoubles(Array(1.0, 2.0, 3.0, 4.0, 5.0), 3))
    agree("bools", DistCollection.ofBools(Array(true, false, true, false, true), 3))
    agree("strings", DistCollection.ofStrings(Array("a", "b", "c", "d", "e"), 3))
    agree(
      "int pairs",
      DistCollection.ofIntPairs(
        Array(4, 1, 3, 2, 0),
        Array(10, 20, 30, 40, 50),
        3,
      ),
    )
    agree("long pairs", longSort)
    agree("string pairs", stringSort)
    agree("int value desc", valueDesc)
  }

  it should "keep a null int key that the strict pair decoder rejects" in {
    // Present (1, 10) is first so the scan classifies as int pairs. The null orders as key 0
    // and is the first sorted row, ahead of the present 0.
    val sorted = intKeySort
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByIntKey(Scan)")
    val row = rowTake(sorted, 3)
    presentInt(at(row, 0), 0) shouldBe false
    storedInt(at(row, 0), 1) shouldBe 5
    presentInt(at(row, 1), 0) shouldBe true
    storedInt(at(row, 1), 0) shouldBe 0
    storedInt(at(row, 1), 1) shouldBe 2
    rejectStrictPairs(sorted)
    agree("int key sort", sorted)
  }

  it should "keep a null key on a value-desc sort that the strict pair decoder rejects" in {
    val sorted = valueDesc
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByIntValueDesc(Scan)")
    val row = rowTake(sorted, 1)
    presentInt(at(row, 0), 0) shouldBe false
    storedInt(at(row, 0), 1) shouldBe 9
    rejectStrictPairs(sorted)
    agree("int value desc", sorted)
  }

  it should "keep a clear bit on a long-pair sort" in {
    val sorted = longSort
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByLongKey(Scan)")
    val row = rowTake(sorted, 3)
    presentLong(at(row, 0), 0) shouldBe false
    storedLong(at(row, 0), 1) shouldBe 5L
    presentLong(at(row, 1), 0) shouldBe true
    storedLong(at(row, 1), 0) shouldBe 0L
    val op = ColumnarPlanner.lower(sorted.plan).getOrElse(fail("long sort did not lower"))
    val columns = interpreted(sorted.plan, op)
    val live = firstLive(columns)
    val kept = BatchCodec.decodeLongPairs(live, 1)
    presentLong(at(kept, 0), 0) shouldBe false
    agree("long pairs", sorted)
  }

  it should "keep a null string key on a string-pair sort" in {
    val sorted = stringSort
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByStringKey(Scan)")
    val row = rowTake(sorted, 1)
    absentString(at(row, 0)) shouldBe true
    agree("string pairs", sorted)
  }

  it should "refuse tryTake of a broadcast left outer join" in {
    val probe = DistCollection.ofIntPairs(Array(0, 1, 2, 3), Array(10, 11, 12, 13), 1)
    val build = DistCollection.ofIntPairs(Array(0), Array(100), 1)
    val joined = DistCollection(
      Plan.LeftOuterJoinOp(probe.plan, build.plan, Some(Plan.JoinStrategy.Broadcast)),
    )
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some("BroadcastLeftOuter(Scan, Scan)")
    // The join is not partition-preserving. Prefix decode of an outer batch is separate.
    ColumnarPlanner.tryTake(joined.plan, 1) shouldBe None
    ColumnarPlanner.tryTake(joined.plan, 3) shouldBe None
    rowTake(joined, 1).length shouldBe 1
    rowTake(joined, 3).length shouldBe 3
  }

  private def agree[A](label: String, collection: DistCollection[A]): Unit =
    val op = ColumnarPlanner.lower(collection.plan).getOrElse(fail(s"$label did not lower"))
    val columns = interpreted(collection.plan, op)
    Seq(1, 3).foreach { n =>
      val decoded = ColumnarInterpreter.decodePrefixFor(columns, n, op)
      ScarletRuntime.get.shuffle.clear()
      val taken = ColumnarPlanner
        .tryTake(collection.plan, n)
        .getOrElse(fail(s"$label missing take"))
      withClue(s"$label n=$n: ") {
        decoded.length shouldBe n
        taken.toList shouldBe decoded.toList
        rowTake(collection, n) shouldBe decoded.toList
      }
    }

  /** The decoder `decodePrefix` hardcoded. A clear bit in the first sorted row is an error. */
  private def rejectStrictPairs(sorted: DistCollection[(Int, Int)]): Unit =
    val op = ColumnarPlanner.lower(sorted.plan).getOrElse(fail("int sort did not lower"))
    val live = firstLive(interpreted(sorted.plan, op))
    val rejected = the[IllegalArgumentException] thrownBy {
      BatchCodec.decodeIntPairs(live, 1)
    }
    rejected.getMessage should include("null Int32 pair at row 0")

  private def interpreted(plan: Plan[_], op: PhysicalOp): ColumnarInterpreter.Columns =
    ScarletRuntime.get.shuffle.clear()
    ColumnarInterpreter.interpret(plan, op)

  private def firstLive(columns: ColumnarInterpreter.Columns): ColumnBatch =
    columns match
      case ColumnarInterpreter.Columns.Pairs(parts) =>
        parts.find(_.length > 0).getOrElse(fail("empty pair sort"))
      case ColumnarInterpreter.Columns.LongPairs(parts) =>
        parts.find(_.length > 0).getOrElse(fail("empty long sort"))
      case other => fail(s"expected pair batches, got $other")

  private def intKeySort: DistCollection[(Int, Int)] =
    DistCollection(
      Seq(
        intPair(boxedInt(1), boxedInt(10)),
        intPair(noInt, boxedInt(5)),
        intPair(boxedInt(4), boxedInt(1)),
        intPair(boxedInt(2), boxedInt(8)),
        intPair(boxedInt(3), boxedInt(7)),
        intPair(boxedInt(0), boxedInt(2)),
      ),
      2,
    ).sortByIntKey

  private def valueDesc: DistCollection[(Int, Int)] =
    DistCollection(
      Seq(
        intPair(boxedInt(1), boxedInt(1)),
        intPair(noInt, boxedInt(9)),
        intPair(boxedInt(2), boxedInt(3)),
        intPair(boxedInt(3), boxedInt(2)),
        intPair(boxedInt(4), boxedInt(0)),
        intPair(boxedInt(5), boxedInt(4)),
      ),
      2,
    ).sortByIntValueDesc

  private def longSort: DistCollection[(Long, Long)] =
    DistCollection(
      Seq(
        longPair(boxedLong(1L), boxedLong(10L)),
        longPair(noLong, boxedLong(5L)),
        longPair(boxedLong(4L), boxedLong(1L)),
        longPair(boxedLong(2L), boxedLong(8L)),
        longPair(boxedLong(3L), boxedLong(7L)),
        longPair(boxedLong(0L), boxedLong(2L)),
      ),
      2,
    ).sortByLongKey

  private def stringSort: DistCollection[(String, Int)] =
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("b" -> 2, nullKey(1), "a" -> 3)),
          Partition(Seq("c" -> 4, "" -> 5, "d" -> 6)),
        ),
      ),
    ).sortByStringKey

  private def at[A](rows: Seq[A], index: Int): A =
    rows.lift(index).getOrElse(fail(s"missing index $index"))

  private def rowTake[A](collection: DistCollection[A], count: Int): List[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.take(count)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def presentInt(pair: (Int, Int), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Integer => true
      case _ => false

  private def storedInt(pair: (Int, Int), ordinal: Int): Int =
    pair.productElement(ordinal) match
      case key: java.lang.Integer => key.intValue()
      case other => fail(s"expected an int component, found $other")

  private def presentLong(pair: (Long, Long), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Long => true
      case _ => false

  private def storedLong(pair: (Long, Long), ordinal: Int): Long =
    pair.productElement(ordinal) match
      case key: java.lang.Long => key.longValue()
      case other => fail(s"expected a long component, found $other")

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentString(pair: (String, Int)): Boolean = pair._1 match
    case null => true
    case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noLong: java.lang.Long = null

  private def boxedLong(value: Long): java.lang.Long = java.lang.Long.valueOf(value)
