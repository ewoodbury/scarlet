package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The all-valid scan and the bit-testing scan keep the same rows.
 *
 * A null is not a failed predicate: the predicate is not called, and the row is dropped. An
 * all-valid window that keeps every row stores `start + i`. A miss still compacts. The int probe
 * agrees with a newest-first chain walk, including a null key, an all-null probe, a miss at either
 * end, and a probe that hits on every row.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class TestDenseScan extends AnyFlatSpec with Matchers:

  "an all-valid filter" should "keep every row at start + i" in {
    val batch = BatchCodec.intPairs(Array(2, 4, 6, 8, 10), Array(1, 3, 5, 7, 9))
    val selected = Selection.applySelected(batch, 1, 3, Seq(keepKey((key: Int) => key > 0)))
    ids(selected) shouldBe Seq(1, 2, 3)
  }

  it should "drop the first row of the window and the last" in {
    val batch = BatchCodec.intPairs(Array(0, 1, 2, 3, 4, 5), Array(0, 1, 2, 3, 4, 5))
    val dropEnds = keepKey((key: Int) => key > 1 && key < 5)
    ids(Selection.applySelected(batch, 0, 6, Seq(dropEnds))) shouldBe Seq(2, 3, 4)
    val dropWindowEnds = keepKey((key: Int) => key > 1 && key < 4)
    ids(Selection.applySelected(batch, 1, 4, Seq(dropWindowEnds))) shouldBe Seq(2, 3)
  }

  it should "leave the index unchanged when the second step also keeps every row" in {
    val batch = BatchCodec.intPairs(Array(1, 2, 3, 4), Array(4, 3, 2, 1))
    val selected = Selection.applySelected(
      batch,
      0,
      4,
      Seq(keepKey((key: Int) => key > 0), keepValue((value: Int) => value > 0)),
    )
    ids(selected) shouldBe Seq(0, 1, 2, 3)
  }

  it should "drop a survivor the second step rejects, including the first and the last" in {
    val batch = BatchCodec.intPairs(Array(1, 2, 3, 4, 5), Array(1, 0, 3, 0, 5))
    val selected = Selection.applySelected(
      batch,
      0,
      5,
      Seq(keepKey((key: Int) => key > 0), keepValue((value: Int) => value > 0)),
    )
    ids(selected) shouldBe Seq(0, 2, 4)
  }

  it should "call each predicate once per tested row" in {
    val batch = BatchCodec.intPairs(Array(0, 1, 2, 3), Array(1, 2, 3, 4))
    val first = new AtomicInteger
    val second = new AtomicInteger
    val steps = Seq(
      Selection.Step.Int32(
        0,
        (key: Int) => {
          val _ = first.incrementAndGet()
          key >= 0
        },
      ),
      Selection.Step.Int32(
        1,
        (value: Int) => {
          val _ = second.incrementAndGet()
          value >= 0
        },
      ),
    )
    ids(Selection.applySelected(batch, 0, 4, steps)) shouldBe Seq(0, 1, 2, 3)
    first.get() shouldBe 4
    second.get() shouldBe 4

    first.set(0)
    second.set(0)
    val dropping = Seq(
      Selection.Step.Int32(
        0,
        (key: Int) => {
          val _ = first.incrementAndGet()
          key > 0
        },
      ),
      Selection.Step.Int32(
        1,
        (value: Int) => {
          val _ = second.incrementAndGet()
          value < 4
        },
      ),
    )
    ids(Selection.applySelected(batch, 0, 4, dropping)) shouldBe Seq(1, 2)
    first.get() shouldBe 4
    second.get() shouldBe 3
  }

  "a filter with nulls" should "match the all-valid walk on the rows both can see" in {
    val keys = Array(1, 2, 3, 4, 5, 6)
    val values = Array(10, 20, 30, 40, 50, 60)
    val dense = BatchCodec.intPairs(keys, values)
    val holed = nullable(keys, values, keyHoles = Set(0, 2, 4), valueHoles = Set.empty)
    val keep = (key: Int) => key % 2 == 0
    // The holes sit on rows the predicate rejects, so both walks keep the same ids.
    ids(Selection.applySelected(dense, 0, 6, Seq(keepKey(keep)))) shouldBe Seq(1, 3, 5)
    ids(Selection.applySelected(holed, 0, 6, Seq(keepKey(keep)))) shouldBe Seq(1, 3, 5)
    ids(Selection.applySelected(dense, 0, 6, Seq(keepKey(keep)))) shouldBe referenceIds(
      int32(dense, 0),
      0,
      6,
      keep,
    )
    ids(Selection.applySelected(holed, 0, 6, Seq(keepKey(keep)))) shouldBe referenceIds(
      int32(holed, 0),
      0,
      6,
      keep,
    )
  }

  it should "drop an all-null batch and call no predicate" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((None, None), (None, Some(1)), (None, Some(2)), (None, None)),
    )
    val calls = new AtomicInteger
    val steps = List(
      Selection.Step.Int32(
        0,
        (key: Int) => {
          val _ = calls.incrementAndGet()
          key > 0
        },
      ),
    )
    Selection.count(batch, 0, batch.length, steps) shouldBe 0L
    calls.get() shouldBe 0
    Selection.apply(batch, 0, batch.length, steps).length shouldBe 0
    calls.get() shouldBe 0
  }

  it should "skip a null inside a dense run and still test the present rows once" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(5)),
        (None, Some(9)),
        (Some(4), Some(1)),
        (Some(3), None),
        (Some(8), Some(2)),
      ),
    )
    val calls = new AtomicInteger
    val steps = List(
      Selection.Step.IntPair((key, value) => {
        val _ = calls.incrementAndGet()
        key < value
      }),
    )
    Selection.count(batch, 0, batch.length, steps) shouldBe 1L
    calls.get() shouldBe 3
    Selection.sumInt32(batch, 0, batch.length, steps, 1) shouldBe 5L
    calls.get() shouldBe 6
    decodedPairs(Selection.apply(batch, 0, batch.length, steps)) shouldBe Seq((1, 5))
  }

  it should "agree with the owned fold on a dense window, including an empty range" in {
    val keys = Array(4, 1, 8, 3, 6, 2)
    val values = Array(-2, 5, 0, 7, -4, 9)
    val batch = BatchCodec.intPairs(keys, values)
    val one = List(Selection.Step.Int32(0, (key: Int) => key % 2 == 0))
    val two = List(
      Selection.Step.Int32(0, (key: Int) => key > 0),
      Selection.Step.Int32(1, (value: Int) => value >= 0),
    )
    val pair = List(Selection.Step.IntPair((key, value) => key > value))
    val calls = new AtomicInteger
    val counted = List(
      Selection.Step.Int32(
        0,
        (key: Int) => {
          val _ = calls.incrementAndGet()
          key >= 0
        },
      ),
    )
    Selection.count(batch, 2, 0, counted) shouldBe 0L
    calls.get() shouldBe 0
    PairSourceFold.count(keys, values, 2, 0, counted) shouldBe 0L
    calls.get() shouldBe 0

    List(one, two, pair).foreach { steps =>
      Selection.count(batch, 1, 4, steps) shouldBe PairSourceFold.count(keys, values, 1, 4, steps)
      Selection.sumInt32(batch, 1, 4, steps, 1) shouldBe
        PairSourceFold.sum(keys, values, 1, 4, steps, 1)
      Selection.sumInt32(batch, 0, keys.length, steps, 0) shouldBe
        PairSourceFold.sum(keys, values, 0, keys.length, steps, 0)
    }
  }

  it should "add nothing for a null measure and still count the row" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((Some(2), None), (Some(4), Some(3)), (Some(1), Some(8)), (Some(6), None)),
    )
    val steps = List(Selection.Step.Int32(0, (key: Int) => key % 2 == 0))
    Selection.count(batch, 0, batch.length, steps) shouldBe 3L
    Selection.sumInt32(batch, 0, batch.length, steps, 1) shouldBe 3L
    Selection.sumInt32(batch, 0, batch.length, steps, 0) shouldBe 12L
  }

  "an int probe" should "match a newest-first walk on dense, null, and miss rows" in {
    val build = Seq(
      (Some(1), Some(10)),
      (Some(2), Some(20)),
      (Some(1), Some(11)),
      (Some(4), Some(40)),
    )
    val everyHit = Seq((Some(2), Some(200)), (Some(1), Some(100)), (Some(4), Some(400)))
    val missEnds = Seq((Some(9), Some(1)), (Some(1), Some(100)), (Some(8), Some(2)))
    val mixed = Seq((Some(1), Some(100)), (None, Some(7)), (Some(2), None), (Some(3), Some(3)))
    val allNull = Seq((None, Some(1)), (None, None), (None, Some(2)))
    val uniqueBuild = Seq((Some(1), Some(10)), (Some(2), Some(20)), (Some(4), Some(40)))
    val uniqueProbe = Seq((Some(4), Some(4)), (Some(1), Some(1)), (Some(2), Some(2)))

    List(everyHit, missEnds, mixed, allNull).foreach { probe =>
      val expected = oracle(build, probe)
      val joined = join(pairs(build), pairs(probe))
      inOrder(joined) shouldBe expected
      HashJoin.countInner(side(pairs(build)), side(pairs(probe))) shouldBe expected.length
    }
    val unique = join(pairs(uniqueBuild), pairs(uniqueProbe))
    inOrder(unique) shouldBe oracle(uniqueBuild, uniqueProbe)
    HashJoin.countInner(side(pairs(uniqueBuild)), side(pairs(uniqueProbe))) shouldBe 3
  }

  it should "keep a null build value on the general writer" in {
    val build = Seq((Some(1), Some(10)), (Some(1), None), (None, Some(99)))
    val probe = Seq((Some(1), Some(7)), (None, Some(8)))
    inOrder(join(pairs(build), pairs(probe))) shouldBe oracle(build, probe)
  }

  it should "write a unique hit at the probe index and compact a miss" in {
    val buildRows = (0 until 70).map(row => (Some(row), Some(row + 1)))
    val probeRows = (0 until 70).map { row =>
      if (row == 0 || row == 69) then (Some(1000 + row), Some(-row))
      else (Some(row), Some(row))
    }
    val joined = join(pairs(buildRows), pairs(probeRows))
    inOrder(joined) shouldBe oracle(buildRows, probeRows)
    joined.length shouldBe 68
    int32(joined, 0).values.length shouldBe 70
    int32(joined, 0).validity.isValid(68) shouldBe false
    int32(joined, 1).isValid(67) shouldBe true
  }

  it should "walk a chain without a validity test and keep newest-first order" in {
    val buildKeys = Array(1, 2, 1, 1)
    val buildValues = Array(10, 20, 11, 12)
    val slots = new JoinSlots(buildKeys.length, Long.MaxValue)
    buildKeys.indices.foreach(row => slots.insert(buildKeys(row), row))
    val probeKeys = Array(2, 1, 9)
    val probeValues = Array(200, 100, 300)
    val written = JoinWrite.writeDense(
      slots,
      buildKeys,
      buildValues,
      probeKeys,
      probeValues,
      probeKeys.length,
      reserved = 1,
      probeBeforeBuild = false,
    )
    inOrder(written) shouldBe Seq(
      (2, Some(20), Some(200)),
      (1, Some(12), Some(100)),
      (1, Some(11), Some(100)),
      (1, Some(10), Some(100)),
    )
    val swapped = JoinWrite.writeDense(
      slots,
      buildKeys,
      buildValues,
      probeKeys,
      probeValues,
      probeKeys.length,
      reserved = 1,
      probeBeforeBuild = true,
    )
    val probeColumn = int32(swapped, 1).values
    val buildColumn = int32(swapped, 2).values
    probeColumn(0) shouldBe 200
    buildColumn(0) shouldBe 20
    swapped.length shouldBe written.length
  }

  it should "count a present probe without a bit test and keep nulls on the sparse walk" in {
    val slots = new JoinSlots(3, Long.MaxValue)
    slots.insert(1, 0)
    slots.insert(1, 1)
    slots.insert(2, 2)
    val keys = Array(1, 3, 2)
    val present = Validity.allValid(3).words
    slots.countOutput(keys, present, 3, keepMisses = false) shouldBe 3
    slots.countOutput(keys, present, 3, keepMisses = true) shouldBe 4

    val sparse = Validity.pack(3, row => row != 0).words
    slots.countOutput(keys, sparse, 3, keepMisses = false) shouldBe 1
    slots.countOutput(keys, sparse, 3, keepMisses = true) shouldBe 3

    val none = Validity.pack(4, _ => false).words
    val absent = Array(0, 0, 0, 0)
    slots.countOutput(absent, none, 4, keepMisses = false) shouldBe 0
    slots.countOutput(absent, none, 4, keepMisses = true) shouldBe 4
    slots.countOutput(absent, none, 0, keepMisses = true) shouldBe 0
  }

  private def keepKey(predicate: Int => Boolean): Selection.Step =
    Selection.Step.Int32(0, new Selection.Int32Function(predicate))

  private def keepValue(predicate: Int => Boolean): Selection.Step =
    Selection.Step.Int32(1, new Selection.Int32Function(predicate))

  private def ids(selected: Selected): Seq[Int] =
    (0 until selected.count).map(slot => selected.rows(slot))

  private def referenceIds(
      column: Int32Column,
      start: Int,
      length: Int,
      keep: Int => Boolean,
  ): Seq[Int] =
    (start until start + length).filter { row =>
      column.isValid(row) && keep(column.values(row))
    }

  private def nullable(
      keys: Array[Int],
      values: Array[Int],
      keyHoles: Set[Int],
      valueHoles: Set[Int],
  ): ColumnBatch =
    val rows = keys.indices.map { row =>
      val key = if (keyHoles.contains(row)) None else Some(keys(row))
      val value = if (valueHoles.contains(row)) None else Some(values(row))
      (key, value)
    }
    BatchCodec.nullableIntPairs(rows)

  private def decodedPairs(batch: ColumnBatch): Seq[(Int, Int)] =
    val keys = int32(batch, 0)
    val values = int32(batch, 1)
    (0 until batch.length).map { row =>
      (keys.values(row), values.values(row))
    }

  private def pairs(rows: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    BatchCodec.nullableIntPairs(rows)

  private def side(batch: ColumnBatch): HashJoin.JoinSide =
    HashJoin.JoinSide(batch, key = 0, value = 1)

  private def join(build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    HashJoin.innerInt32(side(build), side(probe))

  private def oracle(
      build: Seq[(Option[Int], Option[Int])],
      probe: Seq[(Option[Int], Option[Int])],
  ): Seq[(Int, Option[Int], Option[Int])] =
    val chains = build.foldLeft(Map.empty[Int, List[(Int, Option[Int])]]) { (acc, row) =>
      row match
        case (Some(key), value) => acc.updated(key, (key, value) :: acc.getOrElse(key, Nil))
        case (None, _) => acc
    }
    probe.flatMap {
      case (Some(key), probeValue) =>
        chains.getOrElse(key, Nil).map { case (buildKey, buildValue) =>
          (buildKey, buildValue, probeValue)
        }
      case (None, _) => Seq.empty[(Int, Option[Int], Option[Int])]
    }

  private def inOrder(batch: ColumnBatch): Seq[(Int, Option[Int], Option[Int])] =
    val keys = int32(batch, 0)
    val buildValues = int32(batch, 1)
    val probeValues = int32(batch, 2)
    (0 until batch.length).map { row =>
      val buildValue = if (buildValues.isValid(row)) Some(buildValues.values(row)) else None
      val probeValue = if (probeValues.isValid(row)) Some(probeValues.values(row)) else None
      (keys.values(row), buildValue, probeValue)
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")
