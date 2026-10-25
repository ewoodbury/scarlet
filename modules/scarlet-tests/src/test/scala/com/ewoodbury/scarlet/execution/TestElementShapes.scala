package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.execution.ColumnarPlanner.ElementStep

/**
 * The int, float, and string pipelines, and the int and float sums, each match the same
 * filter/project chains of length 2, 3, and 4. This walks those chains on plain values and checks
 * the production result. A project wired to the wrong step changes the survivors. A missing arm
 * throws.
 *
 * A null is dropped when the chain has a filter, and kept in place when every step is a project.
 * The functions are not called for that null. Ints scan a morsel of a shared batch. Floats and
 * strings read a batch, so their non-zero range is the copied slice of the same window.
 */
class TestElementShapes extends AnyFlatSpec with Matchers:

  private val windowStart = 4
  private val windowLength = 4
  private val nullRows = Set(1, 5)

  private val intValues: Vector[Int] =
    Vector(5, 5, -4, 0, 1, 2, 2, 8, -1, 4, 3, -2)

  private val floatValues: Vector[Double] =
    Vector(5.0, 5.0, -4.5, 0.0, 1.5, 2.0, 2.0, 8.5, -1.0, 4.0, 3.5, -2.5)

  private val stringValues: Vector[String] =
    Vector("aa", "b", "", "d", "ee", "b", "x", "ggg", "h", "ee", "i", "jj")

  // Position 0, 1, 2, 3. The same index is a filter or a project, never both in one step.
  private val intFilters: Vector[Int => Boolean] = Vector(
    value => value > -3,
    value => value % 2 == 0,
    value => value < 7,
    value => value != 4,
  )

  private val intMaps: Vector[Int => Int] = Vector(
    value => value + 1,
    value => value * 3,
    value => value - 2,
    value => value / 2,
  )

  private val floatFilters: Vector[Double => Boolean] = Vector(
    value => value > -3.0,
    value => value % 2.0 == 0.0,
    value => value < 7.0,
    value => value != 4.0,
  )

  private val floatMaps: Vector[Double => Double] = Vector(
    value => value + 1.0,
    value => value * 3.0,
    value => value - 2.0,
    value => value / 2.0,
  )

  private val stringFilters: Vector[String => Boolean] = Vector(
    text => text.nonEmpty,
    text => text.length % 2 == 0,
    text => text.compareTo("b") != 0,
    text => !text.contains("x"),
  )

  private val stringMaps: Vector[String => String] = Vector(
    text => text + "!",
    text => text + text,
    text => text.take(1),
    text => text + "#",
  )

  private val shapes: List[List[Op]] =
    val filter = Op.Filter
    val project = Op.Project
    List(
      List(filter, filter),
      List(project, filter),
      List(filter, project),
      List(project, project),
      List(filter, filter, filter),
      List(project, filter, filter),
      List(filter, project, filter),
      List(project, project, filter),
      List(filter, filter, project),
      List(project, filter, project),
      List(filter, project, project),
      List(project, project, project),
      List(filter, filter, filter, filter),
      List(project, filter, filter, filter),
      List(filter, project, filter, filter),
      List(project, project, filter, filter),
      List(filter, filter, project, filter),
      List(project, filter, project, filter),
      List(filter, project, project, filter),
      List(project, project, project, filter),
      List(filter, filter, filter, project),
      List(project, filter, filter, project),
      List(filter, project, filter, project),
      List(project, project, filter, project),
      List(filter, filter, project, project),
      List(project, filter, project, project),
      List(filter, project, project, project),
      List(project, project, project, project),
    )

  "element shapes" should "match a scalar reference and be accepted at every pipeline site" in {
    shapes should have size 28
    shapes.count(_.length == 2) shouldBe 4
    shapes.count(_.length == 3) shouldBe 8
    shapes.count(_.length == 4) shouldBe 16
    shapes.map(shapeName).distinct should have size shapes.length
    shapes.forall(_.size <= intFilters.size) shouldBe true
    windowStart % windowLength shouldBe 0
    intValues.length should be > windowStart + windowLength
    floatValues.length shouldBe intValues.length
    stringValues.length shouldBe intValues.length
    nullRows.forall(index => index >= 0 && index < intValues.length) shouldBe true
    nullRows.exists(index => index >= windowStart && index < windowStart + windowLength) shouldBe true

    val denseInts = intValues.map(value => Some(value))
    val nullableInts = withNulls(intValues)
    val denseFloats = floatValues.map(value => Some(value))
    val nullableFloats = withNulls(floatValues)
    val denseStrings = stringValues.map(text => Some(text))
    val intBatch = BatchCodec.ints(intValues)
    val nullableIntBatch = BatchCodec.nullableInts(nullableInts)
    val floatBatch = BatchCodec.float64s(floatValues)
    val nullableFloatBatch = BatchCodec.nullableFloat64s(nullableFloats)
    val stringBatch = BatchCodec.strings(stringValues)

    shapes.foreach { shape =>
      val name = shapeName(shape)
      val intPlan = elementSteps(shape, intFilters, intMaps)
      val floatPlan = elementSteps(shape, floatFilters, floatMaps)
      val stringPlan = elementSteps(shape, stringFilters, stringMaps)
      withClue(s"$name dense full ") {
        checkInts(denseInts, fullMorsel(intBatch), shape, intPlan)
      }
      withClue(s"$name dense range ") {
        checkInts(windowOf(denseInts), rangedMorsel(intBatch), shape, intPlan)
      }
      withClue(s"$name nullable full ") {
        checkInts(nullableInts, fullMorsel(nullableIntBatch), shape, intPlan)
      }
      withClue(s"$name nullable range ") {
        checkInts(windowOf(nullableInts), rangedMorsel(nullableIntBatch), shape, intPlan)
      }
      withClue(s"$name float full ") {
        checkFloats(denseFloats, floatBatch, shape, floatPlan)
      }
      withClue(s"$name float range ") {
        checkFloats(windowOf(denseFloats), sliceWindow(floatBatch), shape, floatPlan)
      }
      withClue(s"$name nullable float full ") {
        checkFloats(nullableFloats, nullableFloatBatch, shape, floatPlan)
      }
      withClue(s"$name nullable float range ") {
        checkFloats(windowOf(nullableFloats), sliceWindow(nullableFloatBatch), shape, floatPlan)
      }
      withClue(s"$name string full ") {
        checkStrings(denseStrings, stringBatch, shape, stringPlan)
      }
      withClue(s"$name string range ") {
        checkStrings(windowOf(denseStrings), sliceWindow(stringBatch), shape, stringPlan)
      }
    }
  }

  private def checkInts(
      rows: Vector[Option[Int]],
      morsel: Morsel,
      shape: List[Op],
      plan: List[ElementStep],
  ): Unit =
    val expected = reference(rows, shape, intFilters, intMaps)
    val collected = ElementPipeline.ints(morsel, plan)
    collected.length shouldBe expected.length
    BatchCodec.decodeNullableInts(collected) shouldBe expected
    ElementSum.sumInts(morsel, plan) shouldBe sumInts(expected)

  private def checkFloats(
      rows: Vector[Option[Double]],
      batch: ColumnBatch,
      shape: List[Op],
      plan: List[ElementStep],
  ): Unit =
    val expected = reference(rows, shape, floatFilters, floatMaps)
    val collected = ElementPipeline.floats(batch, plan)
    collected.length shouldBe expected.length
    BatchCodec.decodeNullableFloat64s(collected) shouldBe expected
    ElementSum.sumFloats(batch, plan) shouldBe sumDoubles(expected)

  private def checkStrings(
      rows: Vector[Option[String]],
      batch: ColumnBatch,
      shape: List[Op],
      plan: List[ElementStep],
  ): Unit =
    val expected = reference(rows, shape, stringFilters, stringMaps)
    val collected = ElementPipeline.strings(batch, plan)
    collected.length shouldBe expected.length
    BatchCodec.decodeNullableStrings(collected) shouldBe expected

  private def reference[A](
      rows: Vector[Option[A]],
      shape: List[Op],
      filters: Vector[A => Boolean],
      maps: Vector[A => A],
  ): Vector[Option[A]] =
    val dropsNull = shape.contains(Op.Filter)
    rows.foldLeft(Vector.empty[Option[A]]) { (kept, cell) =>
      cell match
        case None =>
          if dropsNull then kept else kept :+ None
        case Some(value) =>
          foldSteps(value, shape, filters, maps) match
            case None => kept
            case survivor => kept :+ survivor
    }

  private def foldSteps[A](
      start: A,
      shape: List[Op],
      filters: Vector[A => Boolean],
      maps: Vector[A => A],
  ): Option[A] =
    shape.zip(filters).zip(maps).foldLeft(Option(start)) { (current, tagged) =>
      val ((op, predicate), mapper) = tagged
      current match
        case None => None
        case Some(value) =>
          op match
            case Op.Filter =>
              if predicate(value) then Some(value) else None
            case Op.Project =>
              Some(mapper(value))
    }

  private def elementSteps[A](
      shape: List[Op],
      filters: Vector[A => Boolean],
      maps: Vector[A => A],
  ): List[ElementStep] =
    shape.zip(filters).zip(maps).map { tagged =>
      val ((op, predicate), mapper) = tagged
      op match
        case Op.Filter => ElementStep.Filter(predicate)
        case Op.Project => ElementStep.Project(mapper)
    }

  private def sumInts(rows: Vector[Option[Int]]): Long =
    rows.foldLeft(0L) { (total, cell) =>
      cell match
        case Some(value) => total + value.toLong
        case None => total
    }

  private def sumDoubles(rows: Vector[Option[Double]]): Double =
    rows.foldLeft(0.0) { (total, cell) =>
      cell match
        case Some(value) => total + value
        case None => total
    }

  private def withNulls[A](values: Vector[A]): Vector[Option[A]] =
    values.zipWithIndex.map { (value, index) =>
      if nullRows.contains(index) then None else Some(value)
    }

  private def windowOf[A](rows: Vector[A]): Vector[A] =
    rows.slice(windowStart, windowStart + windowLength)

  private def fullMorsel(batch: ColumnBatch): Morsel =
    Morsel(batch, 0, batch.length)

  private def rangedMorsel(batch: ColumnBatch): Morsel =
    Morsel(batch, windowStart, windowLength)

  private def sliceWindow(batch: ColumnBatch): ColumnBatch =
    val slices = ColumnBatches.slice(batch, windowLength)
    val skipped = windowStart / windowLength
    slices.drop(skipped).headOption match
      case Some(piece) =>
        piece.length shouldBe windowLength
        piece
      case None =>
        fail(s"no slice at offset $skipped")

  private def shapeName(shape: List[Op]): String =
    shape.map {
      case Op.Filter => "F"
      case Op.Project => "P"
    }.mkString

  private enum Op:
    case Filter
    case Project
