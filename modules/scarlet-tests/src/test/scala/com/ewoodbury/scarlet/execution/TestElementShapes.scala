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
 * The functions are not called for that null. Ints, floats, and strings scan a morsel of a shared
 * batch. A float sum still reads the copied slice of that window.
 *
 * Each filter rejects one witness for every chain that uses that position, including position 3.
 * The witness peels down to a sentinel. A keeper stays small and is shifted instead, so the
 * project positions remain visible. The 28 dense survivor sequences are pairwise distinct.
 */
class TestElementShapes extends AnyFlatSpec with Matchers:

  private val windowStart = 4
  private val windowLength = 4
  private val nullRows = Set(1, 5)

  // A value at or above the floor is a witness layer. A map peels one layer. Anything smaller is
  // shifted, and the shifts have unique subset sums, so the keeper names the project positions.
  private val witnessFloor = 5000
  private val witnessLayer = 10000
  private val intShifts: Vector[Int] = Vector(1, 10, 100, 1000)
  private val intSentinels: Vector[Int] = Vector(-1, -2, -3, -4)
  private val stringMarks: Vector[String] = Vector("A", "B", "C", "D")
  private val stringSentinels: Vector[String] = Vector("", "s1", "b", "x")

  // Keeper, then a passer on null index 1, then witnesses. The passer on null index 5 sits in the
  // window. Witnesses stay off the null slots: one row per (filter position, peel count).
  private val intValues: Vector[Int] =
    val witnesses = intSentinels.zipWithIndex.flatMap { (sentinel, position) =>
      (0 to position).map(peels => sentinel + peels * witnessLayer)
    }
    placeRows(2, 3, 4, witnesses)

  private val floatValues: Vector[Double] =
    intValues.map(_.toDouble)

  private val stringValues: Vector[String] =
    val witnesses = stringSentinels.zipWithIndex.flatMap { (sentinel, position) =>
      (0 to position).map(peels => (">" * peels) + sentinel)
    }
    placeRows("k", "m", "n", witnesses)

  // Position 0, 1, 2, 3. The same index is a filter or a project, never both in one step.
  // Each filter rejects its sentinel and nothing else the keeper can become.
  private val intFilters: Vector[Int => Boolean] =
    intSentinels.map(sentinel => (value: Int) => value != sentinel)

  private val intMaps: Vector[Int => Int] =
    intShifts.map(shift => (value: Int) => peelOrShift(value, shift))

  private val floatFilters: Vector[Double => Boolean] =
    intSentinels.map(sentinel => (value: Double) => value != sentinel.toDouble)

  private val floatMaps: Vector[Double => Double] =
    intShifts.map(shift => (value: Double) => peelOrShiftDouble(value, shift.toDouble))

  private val stringFilters: Vector[String => Boolean] = Vector(
    text => text.nonEmpty,
    text => text.compareTo("s1") != 0,
    text => text.compareTo("b") != 0,
    text => !text.contains("x"),
  )

  private val stringMaps: Vector[String => String] =
    stringMarks.map { mark => (text: String) =>
      if text.startsWith(">") then text.drop(1) else text + mark
    }

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
    // Full dense column. Different project positions change the keeper, and a longer chain that
    // only adds filters drops a witness the shorter chain kept.
    assertDistinctSurvivors(intValues, intFilters, intMaps)
    assertDistinctSurvivors(floatValues, floatFilters, floatMaps)
    assertDistinctSurvivors(stringValues, stringFilters, stringMaps)
    assertEachFilterRejects(intValues, intFilters, intMaps)
    assertEachFilterRejects(floatValues, floatFilters, floatMaps)
    assertEachFilterRejects(stringValues, stringFilters, stringMaps)

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
        checkFloats(denseFloats, fullMorsel(floatBatch), floatBatch, shape, floatPlan)
      }
      withClue(s"$name float range ") {
        checkFloats(
          windowOf(denseFloats),
          rangedMorsel(floatBatch),
          sliceWindow(floatBatch),
          shape,
          floatPlan,
        )
      }
      withClue(s"$name nullable float full ") {
        checkFloats(
          nullableFloats,
          fullMorsel(nullableFloatBatch),
          nullableFloatBatch,
          shape,
          floatPlan,
        )
      }
      withClue(s"$name nullable float range ") {
        checkFloats(
          windowOf(nullableFloats),
          rangedMorsel(nullableFloatBatch),
          sliceWindow(nullableFloatBatch),
          shape,
          floatPlan,
        )
      }
      withClue(s"$name string full ") {
        checkStrings(denseStrings, fullMorsel(stringBatch), shape, stringPlan)
      }
      withClue(s"$name string range ") {
        checkStrings(windowOf(denseStrings), rangedMorsel(stringBatch), shape, stringPlan)
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
      morsel: Morsel,
      sumBatch: ColumnBatch,
      shape: List[Op],
      plan: List[ElementStep],
  ): Unit =
    val expected = reference(rows, shape, floatFilters, floatMaps)
    val collected = ElementPipeline.floats(morsel, plan)
    collected.length shouldBe expected.length
    BatchCodec.decodeNullableFloat64s(collected) shouldBe expected
    ElementSum.sumFloats(sumBatch, plan) shouldBe sumDoubles(expected)

  private def checkStrings(
      rows: Vector[Option[String]],
      morsel: Morsel,
      shape: List[Op],
      plan: List[ElementStep],
  ): Unit =
    val expected = reference(rows, shape, stringFilters, stringMaps)
    val collected = ElementPipeline.strings(morsel, plan)
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

  // Three witnesses precede the in-window passer: position 0, and both peels of position 1.
  private def placeRows[A](
      keeper: A,
      earlyPasser: A,
      latePasser: A,
      witnesses: Vector[A],
  ): Vector[A] =
    Vector(keeper, earlyPasser) ++ witnesses.take(3) ++ Vector(latePasser) ++ witnesses.drop(3)

  private def peelOrShift(value: Int, shift: Int): Int =
    if value >= witnessFloor then value - witnessLayer else value + shift

  private def peelOrShiftDouble(value: Double, shift: Double): Double =
    if value >= witnessFloor.toDouble then value - witnessLayer.toDouble else value + shift

  private def assertDistinctSurvivors[A](
      values: Vector[A],
      filters: Vector[A => Boolean],
      maps: Vector[A => A],
  ): Unit =
    val rows = values.map(value => Some(value))
    val survivors = shapes.map(shape => reference(rows, shape, filters, maps))
    survivors.distinct should have size shapes.length

  private def assertEachFilterRejects[A](
      values: Vector[A],
      filters: Vector[A => Boolean],
      maps: Vector[A => A],
  ): Unit =
    shapes.foreach { shape =>
      shape.zip(filters).zipWithIndex.foreach { (tagged, index) =>
        val (op, predicate) = tagged
        op match
          case Op.Project => ()
          case Op.Filter =>
            val prefix = shape.take(index)
            val rejects = values.exists { value =>
              foldSteps(value, prefix, filters, maps).exists(seen => !predicate(seen))
            }
            withClue(s"${shapeName(shape)} filter $index ") {
              rejects shouldBe true
            }
      }
    }

  private def shapeName(shape: List[Op]): String =
    shape.map {
      case Op.Filter => "F"
      case Op.Project => "P"
    }.mkString

  private enum Op:
    case Filter
    case Project
