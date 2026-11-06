package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestGroupByKey extends AnyFlatSpec with Matchers:

  private val firstKeys = Array(1, 2, -1, 4, Int.MinValue, 1, 2)
  private val firstValues = Array(10, 20, 30, 40, 50, 11, 21)
  private val secondKeys = Array(1, 4)
  private val secondValues = Array(12, 41)

  "groupByKey" should "keep first-seen keys and input value order across batches" in {
    val parts = group(partitions = 4, parallelism = 2)

    parts.length shouldBe 4
    rowsOf(parts(0)) shouldBe Seq((4, Seq(40, 41)), (Int.MinValue, Seq(50)))
    rowsOf(parts(1)) shouldBe Seq((1, Seq(10, 11, 12)))
    rowsOf(parts(2)) shouldBe Seq((2, Seq(20, 21)))
    rowsOf(parts(3)) shouldBe Seq((-1, Seq(30)))
    Math.floorMod(Int.MinValue, 4) shouldBe 0
    parts.foreach(assertBatch)
    val repeated = parts(1).valuesFor(0)
    repeated shouldBe List(10, 11, 12)
    repeated.toList shouldBe List(10, 11, 12)
  }

  it should "group one partition in first-seen order" in {
    val parts = group(partitions = 1, parallelism = 1)

    parts.length shouldBe 1
    rowsOf(parts.headOption.getOrElse(fail("missing partition"))) shouldBe Seq(
      (1, Seq(10, 11, 12)),
      (2, Seq(20, 21)),
      (-1, Seq(30)),
      (4, Seq(40, 41)),
      (Int.MinValue, Seq(50)),
    )
  }

  it should "hold one group per row when every key is distinct" in {
    val rows = 100
    val batch = BatchCodec.intPairs((0 until rows).map(key => (key, key * 2)))
    val parts = GroupByKey.int32(Vector(batch), partitions = 1, parallelism = 1)

    parts.length shouldBe 1
    parts.headOption.getOrElse(fail("missing partition")).groups shouldBe rows
    rowsOf(parts.headOption.getOrElse(fail("missing partition"))) shouldBe
      (0 until rows).map(key => (key, Seq(key * 2)))
    assertBatch(parts.headOption.getOrElse(fail("missing partition")))
  }

  it should "drop a null key and keep the other values in order" in {
    val keys = Int32Column.of(Array(1, 2, 1), Validity.pack(3, row => row != 1), 3)
    val values = Int32Column.of(Array(10, 99, 11), Validity.allValid(3), 3)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(keys, values),
      3,
    )
    val parts = GroupByKey.int32(Vector(batch), partitions = 4, parallelism = 1)

    parts.length shouldBe 4
    rowsOf(parts(1)) shouldBe Seq((1, Seq(10, 11)))
    parts.zipWithIndex.foreach { (grouped, index) =>
      if (index == 1) then grouped.groups shouldBe 1
      else grouped.groups shouldBe 0
    }
    parts.foreach(assertBatch)
  }

  it should "reject a clear value bit, including one beside a null key" in {
    val presentKey = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(Array(1, 1), Validity.allValid(2), 2),
        Int32Column.of(Array(10, 20), Validity.pack(2, row => row != 1), 2),
      ),
      2,
    )
    val nullKey = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(Array(2, 1), Validity.pack(2, row => row != 0), 2),
        Int32Column.of(Array(99, 10), Validity.pack(2, row => row != 0), 2),
      ),
      2,
    )

    val present = the[IllegalArgumentException] thrownBy {
      GroupByKey.int32(Vector(presentKey), partitions = 2, parallelism = 1)
    }
    val absentKey = the[IllegalArgumentException] thrownBy {
      GroupByKey.int32(Vector(nullKey), partitions = 2, parallelism = 1)
    }
    present.getMessage should include("clear value bit")
    absentKey.getMessage should include("clear value bit")
  }

  it should "publish an empty batch for every partition of an empty input" in {
    val missing = GroupByKey.int32(Vector.empty, partitions = 4, parallelism = 2)
    val emptyBatch = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val present = GroupByKey.int32(Vector(emptyBatch), partitions = 3, parallelism = 1)

    missing.length shouldBe 4
    present.length shouldBe 3
    (missing ++ present).foreach { batch =>
      batch.groups shouldBe 0
      batch.length shouldBe 0
      batch.offsets.toSeq shouldBe Seq(0)
      batch.values.toSeq shouldBe Seq.empty[Int]
      batch.rows shouldBe empty
      assertBatch(batch)
    }
  }

  it should "publish empty partitions beside a populated one" in {
    val batch = BatchCodec.intPairs(Seq((4, 40), (8, 80), (4, 41)))
    val parts = GroupByKey.int32(Vector(batch), partitions = 4, parallelism = 2)

    parts.length shouldBe 4
    rowsOf(parts(0)) shouldBe Seq((4, Seq(40, 41)), (8, Seq(80)))
    parts.drop(1).foreach { grouped =>
      grouped.groups shouldBe 0
      grouped.offsets.toSeq shouldBe Seq(0)
    }
  }

  it should "expose each group as a view of the values array" in {
    val batch = group(partitions = 4, parallelism = 1)(0)
    val view = batch.valuesFor(0)
    val start = batch.offsets(0)
    val before = batch.values(start)

    view shouldBe List(40, 41)
    view.toList shouldBe view.toList
    batch.values(start) = before + 7
    view(0) shouldBe before + 7
    batch.values(start) = before
    view(0) shouldBe before
    an[IllegalArgumentException] should be thrownBy batch.valuesFor(-1)
    an[IllegalArgumentException] should be thrownBy batch.valuesFor(batch.groups)
    an[IndexOutOfBoundsException] should be thrownBy view(-1)
    an[IndexOutOfBoundsException] should be thrownBy view(view.length)
  }

  "string groupByKey" should "merge unlike dictionaries by string and keep value order" in {
    val leftDictionary = dictionaryOf(leftRows)
    val rightDictionary = dictionaryOf(rightRows)
    leftDictionary.headOption shouldBe Some("")
    rightDictionary.headOption shouldBe Some(emoji)
    leftDictionary.indexOf(accent) should not be rightDictionary.indexOf(accent)
    leftDictionary.indexOf("") should not be rightDictionary.indexOf("")
    accent should not be decomposed

    val one = groupStrings(partitions = 1, parallelism = 1)
    one.length shouldBe 1
    stringRows(one.headOption.getOrElse(fail("missing partition"))) shouldBe Seq(
      ("", Seq(1, 8)),
      (accent, Seq(2, 9, 9)),
      (decomposed, Seq(3, 10)),
      (emoji, Seq(4, 7)),
      ("red", Seq(5, 6)),
    )

    val parts = groupStrings(partitions = 4, parallelism = 2)
    parts.length shouldBe 4
    val found = parts.zipWithIndex.flatMap { (batch, index) =>
      stringRows(batch).map { (key, values) =>
        Math.floorMod(StringHash.hash(key), parts.length) shouldBe index
        key -> values
      }
    }.toMap
    StringHash.hash("") shouldBe 0
    found shouldBe Map(
      "" -> Seq(1, 8),
      accent -> Seq(2, 9, 9),
      decomposed -> Seq(3, 10),
      emoji -> Seq(4, 7),
      "red" -> Seq(5, 6),
    )
    parts.foreach(assertStringBatch)
  }

  it should "drop a null key and keep an empty string" in {
    val onlyNull = BatchCodec.stringIntPairs(Seq(nullText(1), nullText(2)))
    val dropped = GroupByKey.strings(Vector(onlyNull), partitions = 2, parallelism = 1)

    dropped.length shouldBe 2
    dropped.foreach { batch =>
      batch.groups shouldBe 0
      batch.keys.dictionary.length shouldBe 0
      assertStringBatch(batch)
    }
    stringRows(groupStrings(partitions = 1, parallelism = 1).headOption.getOrElse(fail("missing")))
      .map(_._1) should contain("")
  }

  it should "reject a clear value bit, including one beside a null key" in {
    val presentKey = stringBatch(Seq("red", "blue"), Array(10, 20), row => row != 1)
    val absentKey = stringBatch(Seq(nullText(0)._1, "red"), Array(99, 10), row => row != 0)

    val present = the[IllegalArgumentException] thrownBy {
      GroupByKey.strings(Vector(presentKey), partitions = 2, parallelism = 1)
    }
    val missingKey = the[IllegalArgumentException] thrownBy {
      GroupByKey.strings(Vector(absentKey), partitions = 2, parallelism = 1)
    }
    present.getMessage should include("clear value bit")
    missingKey.getMessage should include("clear value bit")
  }

  it should "publish an empty batch for every partition of an empty input" in {
    val missing = GroupByKey.strings(Vector.empty, partitions = 4, parallelism = 2)
    val emptyBatch = BatchCodec.stringIntPairs(Seq.empty[(String, Int)])
    val present = GroupByKey.strings(Vector(emptyBatch), partitions = 3, parallelism = 1)

    missing.length shouldBe 4
    present.length shouldBe 3
    (missing ++ present).foreach { batch =>
      batch.groups shouldBe 0
      batch.length shouldBe 0
      batch.offsets.toSeq shouldBe Seq(0)
      batch.values.toSeq shouldBe Seq.empty[Int]
      batch.keys.dictionary.toSeq shouldBe Seq.empty[String]
      batch.rows shouldBe empty
      assertStringBatch(batch)
    }
  }

  it should "expose each string group as a view of the values array" in {
    val batch = groupStrings(partitions = 1, parallelism = 1).headOption.getOrElse(fail("missing"))
    val view = batch.valuesFor(0)
    val start = batch.offsets(0)
    val before = batch.values(start)

    view shouldBe List(1, 8)
    view.toList shouldBe view.toList
    batch.values(start) = before + 7
    view(0) shouldBe before + 7
    batch.values(start) = before
    view(0) shouldBe before
    an[IllegalArgumentException] should be thrownBy batch.valuesFor(-1)
    an[IllegalArgumentException] should be thrownBy batch.valuesFor(batch.groups)
    an[IndexOutOfBoundsException] should be thrownBy view(-1)
    an[IndexOutOfBoundsException] should be thrownBy view(view.length)
  }

  // `1L << 32` and `0L` share `toInt`. `1L << 32` and `1L` share `Long.hashCode`. Storing either
  // of those as the key merges two of the three groups.
  "long groupByKey" should "keep keys that share a truncated int or a hash code" in {
    val wide = 1L << 32
    wide.toInt shouldBe 0
    java.lang.Long.hashCode(wide) shouldBe java.lang.Long.hashCode(1L)
    val grouped = GroupByKey.int64(
      Vector(BatchCodec.longPairs(Seq(wide -> 10L, 0L -> 20L, 1L -> 30L))),
      partitions = 1,
      parallelism = 1,
    )

    grouped.flatMap(_.rows).map { case (key, groupedValues) =>
      key -> groupedValues.toSeq
    } shouldBe Seq(wide -> Seq(10L), 0L -> Seq(20L), 1L -> Seq(30L))
  }

  private def group(partitions: Int, parallelism: Int): Vector[GroupedIntBatch] =
    GroupByKey.int32(
      Vector(
        BatchCodec.intPairs(firstKeys.zip(firstValues)),
        BatchCodec.intPairs(secondKeys.zip(secondValues)),
      ),
      partitions,
      parallelism,
    )

  private def rowsOf(batch: GroupedIntBatch): Seq[(Int, Seq[Int])] =
    batch.rows.map { case (key, values) => (key, values.toSeq) }

  private def assertBatch(batch: GroupedIntBatch): Unit =
    val offsets = batch.offsets.toSeq
    offsets shouldBe offsets.sorted
    offsets.headOption shouldBe Some(0)
    batch.offsets(batch.groups) shouldBe batch.values.length
    batch.keys.length shouldBe batch.groups
    (0 until batch.groups).foreach { group =>
      val values = batch.valuesFor(group)
      val slice = batch.values.slice(batch.offsets(group), batch.offsets(group + 1))
      values shouldBe slice.toIndexedSeq
    }

  private val accent = "é"
  private val decomposed = "e\u0301"
  private val emoji = "😀"

  private def leftRows: Seq[(String, Int)] = Seq(
    "" -> 1,
    accent -> 2,
    decomposed -> 3,
    emoji -> 4,
    "red" -> 5,
    nullText(9),
    "red" -> 6,
  )

  private def rightRows: Seq[(String, Int)] = Seq(
    emoji -> 7,
    "" -> 8,
    accent -> 9,
    decomposed -> 10,
    accent -> 9,
  )

  private def groupStrings(partitions: Int, parallelism: Int): Vector[GroupedStringBatch] =
    GroupByKey.strings(
      Vector(BatchCodec.stringIntPairs(leftRows), BatchCodec.stringIntPairs(rightRows)),
      partitions,
      parallelism,
    )

  private def stringRows(batch: GroupedStringBatch): Seq[(String, Seq[Int])] =
    batch.rows.map { case (key, values) => (key, values.toSeq) }

  private def dictionaryOf(rows: Seq[(String, Int)]): Seq[String] =
    BatchCodec.stringIntPairs(rows).columns.toList match
      case (column: DictUtf8Column) :: _ => column.dictionary.toSeq
      case other => fail(s"expected a dictionary, found $other")

  private def stringBatch(
      texts: Seq[String],
      values: Array[Int],
      present: Int => Boolean,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.fromIterable(texts),
        Int32Column.of(values, Validity.pack(values.length, present), values.length),
      ),
      values.length,
    )

  private def assertStringBatch(batch: GroupedStringBatch): Unit =
    val offsets = batch.offsets.toSeq
    offsets shouldBe offsets.sorted
    offsets.headOption shouldBe Some(0)
    batch.offsets(batch.groups) shouldBe batch.values.length
    batch.keys.length shouldBe batch.groups
    if (batch.groups == 0) then batch.keys.dictionary.length shouldBe 0
    (0 until batch.groups).foreach { group =>
      val values = batch.valuesFor(group)
      val slice = batch.values.slice(batch.offsets(group), batch.offsets(group + 1))
      values shouldBe slice.toIndexedSeq
      val code = batch.keys.codes(group)
      (code >= 0 && code < batch.keys.dictionary.length) shouldBe true
      batch.keys.isValid(group) shouldBe true
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullText(value: Int): (String, Int) = (null: String) -> value
