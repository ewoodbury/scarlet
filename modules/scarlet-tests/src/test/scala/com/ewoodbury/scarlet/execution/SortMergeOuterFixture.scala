package com.ewoodbury.scarlet.execution

import java.nio.file.{Files, Path}

import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Shared rows and spill harness for the three outer sort-merge suites. The assertions are the
 * ones those suites already made: bit rows, decoded rows, and partition placement.
 */
object SortMergeOuterFixture extends Assertions with Matchers:

  def sortMergeLeftRows: Seq[(Int, Int)] =
    val present = (0 until 4).flatMap { part =>
      Seq(
        part -> (100 + part),
        part -> (200 + part),
        (part + 4) -> (300 + part),
        (part + 8) -> (400 + part),
      )
    }
    present :+ pair(None, Some(7))

  def sortMergeRightRows: Seq[(Int, Int)] =
    val present = (0 until 4).flatMap { part =>
      Seq(
        part -> (500 + part),
        (part + 4) -> (600 + part),
        (part + 12) -> (700 + part),
      )
    }
    present :+ pair(None, Some(9))

  /** Three present keys in each of four partitions, then one null key. */
  def spreadKeys(base: Int, nullValue: Int): Seq[(Int, Int)] =
    val present = (0 until 4).flatMap { part =>
      Seq(
        part -> (base + part),
        (part + 4) -> (base + 10 + part),
        (part + 8) -> (base + 20 + part),
      )
    }
    present :+ pair(None, Some(nullValue))

  def assertOuterBits(
      plan: Plan[_],
      absent: AbsentSide,
      expected: Seq[(Option[Int], Option[Int], Option[Int])],
      label: String,
  ): Seq[(Option[Int], Option[Int], Option[Int])] =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail(s"$label did not lower"))
    val (parts, side) = ColumnarInterpreter.interpret(plan, op) match
      case ColumnarInterpreter.Columns.Outer(batches, found) => (batches, found)
      case other => fail(s"expected an outer batch, got $other")
    side shouldBe absent
    parts.length shouldBe 4
    val fromBits = parts.flatMap(bitRows)
    val decoded = parts.flatMap(batch => canonical(BatchCodec.decodeOuter(batch, side)))
    fromBits should contain theSameElementsAs decoded
    fromBits should contain theSameElementsAs expected
    parts.zipWithIndex.foreach { (batch, part) =>
      bitRows(batch).foreach { row =>
        row._1 match
          case Some(key) => math.floorMod(key, parts.length) shouldBe part
          case None => part shouldBe 0
      }
    }
    fromBits

  /**
   * Collects `plan` after setting columnar execution. Other settings stay as the caller left
   * them. This uses the current [[ScarletConf]]. Every caller establishes the suite baseline
   * first; this does not reinstall a saved base.
   */
  private[execution] def collect(plan: Plan[_], columnar: Boolean): Seq[Any] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
    ScarletRuntime.get.shuffle.clear()
    DistCollection(plan).collect().toSeq

  /**
   * Pins the spill counters for one outer sort-merge case. The recorded directory is non-empty
   * and still present. No spill files remain in it. [[withSpill]] deletes the tree after the
   * case returns, and these counters are read before `withSpill`'s finally reset. A missing
   * directory fails here: [[SpillStore.listForTest]] returns empty for a missing path.
   */
  private[execution] def assertSpill(
      dir: Path,
      files: Long,
      passes: Long,
  ): org.scalatest.Assertion =
    SortMergeRuns.filesWrittenForTest shouldBe files
    SortMergeRuns.mergePassesForTest shouldBe passes
    SortMergeRuns.lastDirectoryForTest.nonEmpty shouldBe true
    Path.of(SortMergeRuns.lastDirectoryForTest) shouldBe dir
    Files.isDirectory(dir) shouldBe true
    SpillStore.listForTest(dir) shouldBe Vector.empty[Path]

  def withSpill[A](base: ScarletConf, budget: Long)(body: Path => A): A =
    val dir = Files.createTempDirectory("scarlet-sort-merge-outer")
    val saved = ScarletConf.get
    SortMergeRuns.resetForTest()
    ScarletConf.set(
      base.copy(
        spillEnabled = true,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        columnarBatchSize = 1,
        spillFanIn = 2,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(saved)
      SortMergeRuns.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)

  private def bitRows(batch: ColumnBatch): Seq[(Option[Int], Option[Int], Option[Int])] =
    val keys = BatchColumns.int32(batch, 0)
    val left = BatchColumns.int32(batch, 1)
    val right = BatchColumns.int32(batch, 2)
    (0 until batch.length).map { row =>
      (bit(keys, row), bit(left, row), bit(right, row))
    }

  private def bit(column: Int32Column, row: Int): Option[Int] =
    if (column.isValid(row)) Some(column.values(row)) else None

  /** `(key, left, right)` with a null key or value as `None`, so a null is not unboxed. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private[execution] def canonical(rows: Seq[Any]): Seq[(Option[Int], Option[Int], Option[Int])] =
    rows.map { row =>
      val outer = row.asInstanceOf[Product]
      val values = outer.productElement(1).asInstanceOf[Product]
      (
        cell(outer.productElement(0)),
        cell(values.productElement(0)),
        cell(values.productElement(1)),
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null", "org.wartremover.warts.Product"))
  private[execution] def cell(value: Any): Option[Int] = value match
    case null => None
    case number: Int => Some(number)
    case Some(number: Int) => Some(number)
    case None => None
    case other => fail(s"expected an int cell, found $other")

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def pair(key: Option[Int], value: Option[Int]): (Int, Int) =
    val boxedKey: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val boxedValue: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(boxedKey, boxedValue).asInstanceOf[(Int, Int)]
