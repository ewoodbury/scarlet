package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntPairArrays, Plan}

import ColumnarInterpreter.{Columns, interpretAt, pairsOf}

/**
 * Int `countSumByKey`.
 *
 * An owned int-pair scan, including a stack of int key and value slots and a scan with no filter,
 * is summed from those arrays. The predicates run inside the scan. The arrays are aliased and not
 * copied, and no survivor index is built. One [[Plan.MapIntKeyOp]] over that source is summed the
 * same way: the function runs once on each present survivor and is not inspected, and the pair
 * batch is not published. A null key is not passed in. Encoded morsels under that one map,
 * including a null value, observe into the same table. One pair [[Plan.MapOp]] whose filters are
 * int slots, or absent, is summed from that project's input morsels. The interpreter's adapter
 * runs on every row, including a null key and a null value, and is not inspected. A null projected
 * key is dropped. A null projected value is counted and not added. The pair batch is not
 * published, and [[CountSum.byKey]] is not called. One [[Plan.FilterKeysOp]] or
 * [[Plan.FilterValuesOp]] directly above that project applies the slot to the projected pair and
 * still does not publish. A key predicate is not called on a null projected key. A value predicate
 * is not called on a null projected value. A key slot that keeps a present key counts a null value
 * and does not add it. A second slot, a whole-row filter, a second map, and a long or string slot
 * stay on the published path. A whole-row step, a non-int slot, a boxed row the encoder throws on,
 * and any other pair child are interpreted, then aggregated. That path partial-aggregates survivor
 * ids, including a stack of one, and [[CountSum.byKey]] sees the published pairs. A null key is
 * dropped. A null value is counted and not added. The exchange sees dense partials of
 * `(key, count, sum)`.
 */
private[execution] object CountSumArms:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    child.kind match
      case PhysicalOp.Kind.Pairs =>
        val aggregated = foldedProjected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
          width,
          workers,
          batchSize,
        ).orElse(
          foldedAbove(
            source,
            child,
            selectedPartition,
            preserveAbsence,
            ordinals,
            width,
            workers,
            batchSize,
          ),
        ).orElse(
          foldedMapped(
            source,
            child,
            selectedPartition,
            preserveAbsence,
            ordinals,
            width,
            workers,
            batchSize,
          ),
        ).orElse(folded(source, child, ordinals, width, workers, batchSize)) match
          case Some(summed) => summed
          case None =>
            FilterChain.selected(
              source,
              child,
              selectedPartition,
              preserveAbsence,
              ordinals,
            ) match
              case Some(parts) => CountSum.byKeySelected(parts, width, workers, batchSize)
              case None =>
                CountSum.byKey(
                  pairsOf(
                    interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
                  ),
                  width,
                  workers,
                  batchSize,
                )
        Columns.CountSums(aggregated)
      case other =>
        throw new IllegalStateException(
          s"countSumByKey column is $other, expected int pairs",
        )

  /**
   * One pair [[Plan.MapOp]]. The input of the project is interpreted, then each morsel is summed
   * through [[IntPairProject.visitCountSums]]. `None` when a second map sits under the filters, or
   * when a step is not an int slot, including a whole-row predicate. A null key and a null value
   * are passed to the adapter. A null projected key is dropped. A null projected value is counted
   * and not added. [[CountSum.byKey]] is not called. The function is not inspected.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def foldedProjected(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    (source, child) match
      case (Plan.MapOp(inner, fn), PhysicalOp.Project(under)) =>
        val (underPlan, _) = PredicatePlan.childOf(inner, under)
        if (pastMap(underPlan)) then None
        else
          slotSteps(inner, under, ordinals) match
            case None => None
            case Some(steps) =>
              val adapted = IntPairProject.adapt(fn)
              ownedPairs(inner, under) match
                case Some(parts) =>
                  Some(
                    CountSum.byKeyOwnedProjected(
                      parts,
                      steps,
                      adapted,
                      width,
                      workers,
                      batchSize,
                    ),
                  )
                case None =>
                  val columns =
                    try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
                    catch
                      case _: NullPointerException =>
                        throw new ColumnarFallback(
                          "countSumByKey source has a null pair component",
                        )
                  columns match
                    case Columns.Pairs(batches) =>
                      Some(
                        ProjectedPairCountSum.sum(batches, adapted, width, workers, batchSize),
                      )
                    case _ =>
                      throw new IllegalStateException(
                        "pair project count-sum expected int pairs",
                      )
      case _ => None

  /**
   * One key or value slot directly above one int-pair project. The project's input is summed, then
   * the slot sees the projected pair. `None` when a second map sits under the project, when the
   * step is not one int key or value, or when a filter under the project is not an int slot. A
   * null key and a null value are passed to the adapter. A null projected key is dropped and the
   * key predicate is not called. A null projected value is counted and not added when a key slot
   * keeps the row, and a value predicate is not called on it. [[CountSum.byKey]] is not called.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def foldedAbove(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    val matched = (source, child) match
      case (
            Plan.FilterKeysOp(Plan.MapOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.Project(under), PhysicalOp.Slot.Key),
          ) if intPairs(child) =>
        Some((inner, under, fn))
      case (
            Plan.FilterValuesOp(Plan.MapOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.Project(under), PhysicalOp.Slot.Value),
          ) if intPairs(child) =>
        Some((inner, under, fn))
      case _ => None
    matched.flatMap { (inner, under, fn) =>
      if (deeperProject(inner, under)) then None
      else
        oneIntSlot(source, child).flatMap { step =>
          slotSteps(inner, under, ordinals).map { steps =>
            val adapted = IntPairProject.adapt(fn)
            ownedPairs(inner, under) match
              case Some(parts) =>
                CountSum.byKeyOwnedAbove(
                  parts,
                  steps,
                  adapted,
                  step,
                  width,
                  workers,
                  batchSize,
                )
              case None =>
                val columns =
                  try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
                  catch
                    case _: NullPointerException =>
                      throw new ColumnarFallback(
                        "countSumByKey source has a null pair component",
                      )
                columns match
                  case Columns.Pairs(batches) =>
                    ProjectedAboveCountSum.sum(batches, adapted, step, width, workers, batchSize)
                  case _ =>
                    throw new IllegalStateException(
                      "pair project count-sum expected int pairs",
                    )
          }
        }
    }

  /** An int-pair filter. A long or string project is not this arm. */
  private def intPairs(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Pairs => true
    case _ => false

  /** A second map or another project under the pair map. Filters between them are peeled first. */
  private def deeperProject(plan: Plan[_], op: PhysicalOp): Boolean =
    val (underPlan, underOp) = PredicatePlan.childOf(plan, op)
    val physical = underOp match
      case PhysicalOp.Project(_) | PhysicalOp.MapKey(_) | PhysicalOp.MapValue(_) |
          PhysicalOp.Pipeline(_, _) =>
        true
      case _ => false
    pastMap(underPlan) || physical

  /** One int key or value on the projected pair. A row step and a second slot are `None`. */
  private def oneIntSlot(source: Plan[_], child: PhysicalOp): Option[Selection.Step] =
    PredicatePlan.compile(source, child).flatMap { compiled =>
      PredicatePlan.selectionSteps(compiled) match
        case Some(Vector(step @ Selection.Step.Int32(column, _))) if column == 0 || column == 1 =>
          Some(step)
        case _ => None
    }

  /**
   * One [[Plan.MapIntKeyOp]] over an int-slot stack, or over a scan with no filter. The filters
   * stay on the source. Owned arrays alias the scan. Anything else is encoded and scanned per
   * morsel. `None` when a second map sits under the filters, or when a step is not an int slot.
   * [[CountSum.byKey]] is not called.
   */
  private def foldedMapped(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    (source, child) match
      case (Plan.MapIntKeyOp(inner, fn), PhysicalOp.MapKey(mapped)) =>
        val (underPlan, underOp) = PredicatePlan.childOf(inner, mapped)
        if (pastMap(underPlan)) then None
        else
          slotSteps(inner, mapped, ordinals).flatMap { steps =>
            ownedPairs(underPlan, underOp) match
              case Some(parts) =>
                Some(CountSum.byKeyOwnedMapped(parts, steps, fn, width, workers, batchSize))
              case None =>
                encoded(underPlan, underOp, selectedPartition, preserveAbsence, ordinals).map {
                  batches =>
                    CountSum.byKeyMapped(batches, steps, fn, width, workers, batchSize)
                }
          }
      case _ => None

  /**
   * Owned arrays under an int slot stack, or under a scan with no filter. `None` leaves the
   * survivor index in place. A whole-row step, a non-pair atom, and a boxed partition are `None`.
   */
  private def folded(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    ownedPairs(source, child).flatMap { parts =>
      slotSteps(source, child, ordinals).map { steps =>
        CountSum.byKeyOwned(parts, steps, width, workers, batchSize)
      }
    }

  /** Fully present int pairs. Anything else, including one boxed partition, stays on encode. */
  private def ownedPairs(
      plan: Plan[_],
      op: PhysicalOp,
  ): Option[Vector[(Array[Int], Array[Int])]] =
    val (_, under) = PredicatePlan.childOf(plan, op)
    under match
      case PhysicalOp.Scan(PhysicalOp.Kind.Pairs, partitions) =>
        partitions.foldLeft(Option(Vector.empty[(Array[Int], Array[Int])])) { (acc, partition) =>
          (acc, partition.data) match
            case (Some(built), pairs: IntPairArrays) =>
              Some(built :+ ((pairs.keys, pairs.values)))
            case _ => None
        }
      case _ => None

  /**
   * Int key and value slots in source order. `Some(Nil)` is a scan with no filter. `None` when a
   * step is not an int slot, including a whole-row predicate.
   */
  private def slotSteps(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(source, child) match
      case None => Some(Nil)
      case Some(compiled) =>
        PredicatePlan.selectionSteps(PredicatePlan.encoded(compiled, ordinals)) match
          case Some(steps) if steps.nonEmpty && admits(steps) => Some(steps.toList)
          case _ => None

  /** A map under the filters is interpreted. The steps would see its output. */
  private def pastMap(plan: Plan[_]): Boolean = plan match
    case Plan.MapOp(_, _) | Plan.MapValuesOp(_, _) | Plan.MapIntKeyOp(_, _) |
        Plan.FlatMapOp(_, _) | Plan.FlatMapValuesOp(_, _) | Plan.MapPartitionsOp(_, _) =>
      true
    case _ => false

  /**
   * Source morsels under the map. A sorted batch with a null key stays on the interpreted path:
   * the pair kernel would drop a row the row path keeps.
   */
  private def encoded(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Option[Vector[ColumnBatch]] =
    try
      interpretAt(plan, op, selectedPartition, preserveAbsence, ordinals) match
        case Columns.Pairs(batches) =>
          if (dropsNullKey(op, batches)) then None else Some(batches)
        case _ => None
    catch case _: NullPointerException => None

  private def dropsNullKey(op: PhysicalOp, batches: Vector[ColumnBatch]): Boolean =
    op match
      case _: PhysicalOp.SortByIntKey => FoldedCount.absentIntKey(batches)
      case _ => false

  private def admits(steps: Vector[Selection.Step]): Boolean =
    steps.forall {
      case Selection.Step.Int32(column, _) => column == 0 || column == 1
      case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
          Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
          Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
        false
    }
