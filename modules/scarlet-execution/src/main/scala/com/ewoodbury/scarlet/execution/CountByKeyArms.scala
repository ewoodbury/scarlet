package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * Int, long, and string `countByKey`.
 *
 * An int-pair stack of key and value slot atoms is counted by [[FoldedCount]]: the predicates run
 * inside the key scan and no survivor index is built. An owned int-pair scan is hashed from those
 * arrays. The encoded copy remains for a boxed row, a null, a sort, and a budget miss. One
 * [[Plan.MapValuesOp]] above that source is skipped when its child is not a map: the keys are
 * unchanged, so the scan reads the source key column and does not run the value function.
 * [[Plan.MapIntKeyOp]] changes the key. Int-slot filters under that one map are counted from the
 * source: the function runs once on each present survivor and is not inspected. A null key is
 * dropped and is not passed in. One key or value slot above that map is applied after the
 * function. A key slot sees the mapped key and is not called on a null. A value slot sees the
 * original value. The mapped pair batch is not published. A [[Plan.MapOp]] that is a pair
 * [[PhysicalOp.Project]] is counted from that project's input morsels. The interpreter's adapter
 * runs on every row, including a null key, and only the projected key is stored. The pair batch is
 * not published. The function is not inspected. A filter above an int project, a flatMap, and a
 * mapPartitions stay on the interpreted path. A long-pair [[PhysicalOp.Project]] uses
 * [[LongPairProject]] the same way and does not publish the pair batch. One key or value slot
 * above that long project is counted from the project's input: filters under the project stay a
 * survivor index, and the slot is applied to the projected key. A null projected value still
 * counts. A second slot, a whole-row filter above the project, and a second map stay interpreted.
 * [[LongHashCount.byKey]] counts a long scan or filter and does not see that project or that slot.
 * A string-pair [[PhysicalOp.Project]] is interpreted into a dictionary column and an int column,
 * then [[StringHashCount.byKey]] hashes that dictionary. The pairs are not decoded. One key or
 * value filter above that project publishes both columns, and the same [[StringHashCount.byKey]]
 * hashes those batches. A second slot does not lower. No filters, a whole-row or string step, a
 * non-int slot, and a null component the encoder throws on stay on the old path. That path
 * partial-counts survivor ids, including one row, key, or value filter. A scan and a non-pair
 * child stay on the dense count. The value column is not read on that path, so a null value still
 * counts. A long count publishes [[Columns.LongCounts]]. A string count publishes
 * [[Columns.StringPairs]]. The stored count is an `Int`. A null string key is dropped and `""` is
 * kept.
 */
private[execution] object CountByKeyArms:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    child.kind match
      case PhysicalOp.Kind.Pairs =>
        val counted = tryFold(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
          width,
          workers,
          batchSize,
        ) match
          case Some(folded) => folded
          case None =>
            countProjected(
              source,
              child,
              selectedPartition,
              preserveAbsence,
              ordinals,
              width,
              workers,
              batchSize,
            ) match
              case Some(projected) => projected
              case None =>
                countedFromBatches(
                  source,
                  child,
                  selectedPartition,
                  preserveAbsence,
                  ordinals,
                  width,
                  workers,
                  batchSize,
                )
        Columns.Pairs(counted)
      case PhysicalOp.Kind.LongPairs =>
        val counted = countAboveProjectedLong(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
          width,
          workers,
          batchSize,
        ) match
          case Some(above) => above
          case None =>
            countProjectedLong(
              source,
              child,
              selectedPartition,
              preserveAbsence,
              ordinals,
              width,
              workers,
              batchSize,
            ) match
              case Some(projected) => projected
              case None =>
                FilterChain.selected(
                  source,
                  child,
                  selectedPartition,
                  preserveAbsence,
                  ordinals,
                ) match
                  case Some(parts) => LongHashCount.byKeySelected(parts, width, workers, batchSize)
                  case None =>
                    val batches = longPairsOf(
                      interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
                    )
                    LongHashCount.byKey(batches, width, workers, batchSize)
        Columns.LongCounts(counted)
      case PhysicalOp.Kind.StringPairs =>
        // A project child is already dictionary columns. byKey hashes those codes and does not
        // decode the pairs. A null int is not a fallback. The value column is dropped first.
        val counted = FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) => StringHashCount.byKeySelected(parts, width, workers, batchSize)
          case None =>
            val batches = StringPairWalk.batches(
              interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
            )
            StringHashCount.byKey(batches, width, workers, batchSize)
        Columns.StringPairs(counted)
      case other =>
        throw new IllegalStateException(
          s"countByKey column is $other, expected int, long, or string pairs",
        )

  /**
   * Present keys [[columns]] publishes for an int-pair source. Each occupied slot of the count
   * table is one row. A null key is absent here.
   */
  private[execution] def groupRows(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Long =
    columns(source, child, None, false, ordinals) match
      case Columns.Pairs(batches) => ColumnReduce.count(batches)
      case other =>
        throw new IllegalStateException(
          s"distinct key count expected int pairs, found $other",
        )

  /**
   * True when [[tryFold]] has a source scan, or a pair project is counted without its batch. That
   * is a non-empty int slot stack whose child is not a map, one [[Plan.MapValuesOp]] whose child
   * is not a map, one [[Plan.MapIntKeyOp]] whose filters are int slots and whose child is not a
   * map, or one pair [[Plan.MapOp]]. A value or row filter above the value map, a second map under
   * [[Plan.MapIntKeyOp]], and a dense scan with no map are false. Budget, a null sort key, and a
   * null the encoder throws are decided in [[columns]], which returns to the index path.
   */
  private[execution] def folds(source: Plan[_], child: PhysicalOp, ordinals: Set[Int]): Boolean =
    foldTarget(source, child, ordinals).nonEmpty || mappedKey(
      source,
      child,
      ordinals,
    ).nonEmpty || mappedAbove(source, child, ordinals).nonEmpty || pairProject(
      source,
      child,
    ).nonEmpty

  private def tryFold(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    val above = mappedAbove(source, child, ordinals).flatMap { (plan, op, steps, fn, slot) =>
      ownedPairs(plan, op) match
        case Some(parts) =>
          MappedAboveCount.countOwned(parts, steps, slot, fn, width, workers, batchSize)
        case None =>
          scanned(plan, op, selectedPartition, preserveAbsence, ordinals).flatMap {
            (under, batches) =>
              if (sortDropsNullKey(under, batches)) then None
              else MappedAboveCount.count(batches, steps, slot, fn, width, workers, batchSize)
          }
    }
    above
      .orElse {
        val mapped = mappedKey(source, child, ordinals).flatMap { (plan, op, steps, fn) =>
          ownedPairs(plan, op) match
            case Some(parts) =>
              FoldedCount.countOwnedMapped(parts, steps, fn, width, workers, batchSize)
            case None =>
              scanned(plan, op, selectedPartition, preserveAbsence, ordinals).flatMap {
                (under, batches) =>
                  if (sortDropsNullKey(under, batches)) then None
                  else FoldedCount.countMapped(batches, steps, fn, width, workers, batchSize)
              }
        }
        mapped
      }
      .orElse {
        foldTarget(source, child, ordinals).flatMap { (plan, op, steps) =>
          ownedPairs(plan, op) match
            case Some(parts) => countOwned(parts, steps, width, workers, batchSize)
            case None =>
              scanned(plan, op, selectedPartition, preserveAbsence, ordinals).flatMap {
                (under, batches) =>
                  if (sortDropsNullKey(under, batches)) then None
                  else countEncoded(batches, steps, width, workers, batchSize)
              }
        }
      }

  /**
   * One [[Plan.MapIntKeyOp]] over an int-slot filter stack. The filters stay on the source. The
   * function is the witness and is not read here. `None` when a second map sits under the filters,
   * or when the steps are not int slots.
   */
  private def mappedKey(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step], Int => Int)] =
    (source, child) match
      case (Plan.MapIntKeyOp(inner, fn), PhysicalOp.MapKey(mapped)) =>
        val (underPlan, underOp) = PredicatePlan.childOf(inner, mapped)
        if (pastMap(underPlan)) then None
        else admitted(inner, mapped, ordinals).map(steps => (underPlan, underOp, steps, fn))
      case _ => None

  /**
   * One key or value slot above one [[Plan.MapIntKeyOp]]. Steps under the map stay on the source
   * and may be empty. The slot above the map is applied after the key function. `None` when a
   * second map sits under the filters, or when a step is not an int slot. Neither function is
   * read.
   */
  private def mappedAbove(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step], Int => Int, Selection.Step)] =
    (source, child) match
      case (
            Plan.FilterKeysOp(Plan.MapIntKeyOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.MapKey(mapped), PhysicalOp.Slot.Key),
          ) =>
        aboveOf(inner, mapped, fn, source, child, ordinals)
      case (
            Plan.FilterValuesOp(Plan.MapIntKeyOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.MapKey(mapped), PhysicalOp.Slot.Value),
          ) =>
        aboveOf(inner, mapped, fn, source, child, ordinals)
      case _ => None

  private def aboveOf(
      inner: Plan[_],
      mapped: PhysicalOp,
      fn: Int => Int,
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step], Int => Int, Selection.Step)] =
    val (underPlan, underOp) = PredicatePlan.childOf(inner, mapped)
    if (pastMap(underPlan)) then None
    else
      underSlots(inner, mapped, ordinals).flatMap { steps =>
        oneSlot(source, child, ordinals).map { slot =>
          (underPlan, underOp, steps, fn, slot)
        }
      }

  /** Steps under the map. `Some(Nil)` when the child is not a filter. */
  private def underSlots(
      plan: Plan[_],
      op: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(plan, op) match
      case None => Some(Nil)
      case Some(compiled) =>
        slotList(compiled, ordinals).filter(steps => FoldedCount.admitsUnmapped(steps))

  /** The one slot above the map. */
  private def oneSlot(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[Selection.Step] =
    PredicatePlan.compile(source, child).flatMap { compiled =>
      slotList(compiled, ordinals) match
        case Some(step :: Nil) => Some(step)
        case _ => None
    }

  /**
   * One pair [[Plan.MapOp]]. The input of the project is interpreted, then each morsel is counted
   * through [[IntPairProject.visitKeys]]. `None` when this child is not that project. A null key
   * is passed to the adapter. The value cell is not stored.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def countProjected(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    pairProject(source, child).map { (inner, under, fn) =>
      val adapted = IntPairProject.adapt(fn)
      val columns =
        try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
        catch
          case _: NullPointerException =>
            throw new ColumnarFallback("countByKey source has a null pair component")
      columns match
        case Columns.Pairs(batches) =>
          ProjectedPairCount.count(batches, adapted, width, workers, batchSize)
        case _ =>
          throw new IllegalStateException("pair project count expected int pairs")
    }

  /**
   * One key or value slot directly above one long-pair project. The project's input is counted
   * through [[LongPairProject.visitFiltered]]. Filters under the project are a survivor index from
   * [[FilterChain.selected]]: the ids are visited and the source columns are not compacted. `None`
   * when a second map sits under the project, when the step is not one long key or value, or when
   * those under-filters do not yield an index. A null key and a null value are passed to the
   * adapter. A null projected key is dropped and the key predicate is not called. A null projected
   * value still counts when the slot keeps the key, and a value predicate is not called on it.
   * [[LongHashCount.byKey]] is not called.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def countAboveProjectedLong(
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
          ) if longPairs(child) =>
        Some((inner, under, fn))
      case (
            Plan.FilterValuesOp(Plan.MapOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.Project(under), PhysicalOp.Slot.Value),
          ) if longPairs(child) =>
        Some((inner, under, fn))
      case _ => None
    matched.flatMap { (inner, under, fn) =>
      if (deeperMap(inner, under)) then None
      else
        oneLongSlot(source, child).flatMap { step =>
          val adapted = LongPairProject.adapt(fn)
          PredicatePlan.compile(inner, under) match
            case None =>
              countedScan(
                inner,
                under,
                adapted,
                step,
                selectedPartition,
                preserveAbsence,
                ordinals,
                width,
                workers,
                batchSize,
              )
            case Some(_) =>
              indexedUnder(
                inner,
                under,
                adapted,
                step,
                selectedPartition,
                preserveAbsence,
                ordinals,
                width,
                workers,
                batchSize,
              )
        }
    }

  /** The project sits on a scan. There is no survivor index to apply. */
  private def countedScan(
      plan: Plan[_],
      op: PhysicalOp,
      adapted: LongPairProject.LongPairMap,
      step: Selection.Step,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    val columns =
      try interpretAt(plan, op, selectedPartition, preserveAbsence, ordinals)
      catch
        case _: NullPointerException =>
          throw new ColumnarFallback("countByKey source has a null pair component")
    columns match
      case Columns.LongPairs(batches) =>
        Some(
          ProjectedLongPairCount.countFiltered(
            batches,
            adapted,
            step,
            width,
            workers,
            batchSize,
          ),
        )
      case _ =>
        throw new IllegalStateException("pair project count expected long pairs")

  /**
   * Filters under the project. `None` when the stack does not yield a survivor index, so the
   * caller does not compact those columns.
   */
  private def indexedUnder(
      plan: Plan[_],
      op: PhysicalOp,
      adapted: LongPairProject.LongPairMap,
      step: Selection.Step,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    val selected =
      try FilterChain.selected(plan, op, selectedPartition, preserveAbsence, ordinals)
      catch
        case _: NullPointerException =>
          throw new ColumnarFallback("countByKey source has a null pair component")
    selected.map { parts =>
      ProjectedLongPairCount.countSelected(parts, adapted, step, width, workers, batchSize)
    }

  /** One long key or value on the projected pair. A row step and a second slot are `None`. */
  private def oneLongSlot(source: Plan[_], child: PhysicalOp): Option[Selection.Step] =
    PredicatePlan.compile(source, child).flatMap { compiled =>
      PredicatePlan.selectionSteps(compiled) match
        case Some(Vector(step @ Selection.Step.Int64(column, _))) if column == 0 || column == 1 =>
          Some(step)
        case _ => None
    }

  /** A second map under the project. Filters between the maps are peeled first. */
  private def deeperMap(plan: Plan[_], op: PhysicalOp): Boolean =
    val (underPlan, underOp) = PredicatePlan.childOf(plan, op)
    val planned = underPlan match
      case Plan.MapOp(_, _) | Plan.MapValuesOp(_, _) | Plan.MapIntKeyOp(_, _) |
          Plan.FlatMapOp(_, _) | Plan.FlatMapValuesOp(_, _) | Plan.MapPartitionsOp(_, _) =>
        true
      case _ => false
    val physical = underOp match
      case PhysicalOp.Project(_) | PhysicalOp.MapKey(_) | PhysicalOp.MapValue(_) |
          PhysicalOp.Pipeline(_, _) =>
        true
      case _ => false
    planned || physical

  /** Long pairs, including one slot above a long-pair project. */
  private def longPairs(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.LongPairs => true
    case _ => false

  /**
   * One long-pair [[Plan.MapOp]]. The input of the project is interpreted, then each morsel is
   * counted through [[LongPairProject.visitKeys]]. `None` when this child is not that project. A
   * filter above it has its own arm. A null key is passed to the adapter. The value cell is not
   * stored. [[LongHashCount.byKey]] is not called.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def countProjectedLong(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    pairProject(source, child).map { (inner, under, fn) =>
      val adapted = LongPairProject.adapt(fn)
      val columns =
        try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
        catch
          case _: NullPointerException =>
            throw new ColumnarFallback("countByKey source has a null pair component")
      columns match
        case Columns.LongPairs(batches) =>
          ProjectedLongPairCount.count(batches, adapted, width, workers, batchSize)
        case _ =>
          throw new IllegalStateException("pair project count expected long pairs")
    }

  /** The plan under a pair project and the function the interpreter would adapt. */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def pairProject(
      source: Plan[_],
      child: PhysicalOp,
  ): Option[(Plan[_], PhysicalOp, Any)] =
    (source, child) match
      case (Plan.MapOp(inner, fn), PhysicalOp.Project(under)) => Some((inner, under, fn))
      case _ => None

  /**
   * Index path, then a full interpret. A boxed pair source whose first row witnesses `Kind.Pairs`
   * can still hold a null component later. The scan keeps that null absent, and this count does
   * not read the value column, so a null value still counts. The catch stays for a null the
   * selection itself throws.
   */
  private def countedFromBatches(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    val selectedParts =
      try
        FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        )
      catch
        case _: NullPointerException =>
          throw new ColumnarFallback("countByKey source has a null pair component")
    selectedParts match
      case Some(parts) => HashCount.byKeySelected(parts, width, workers, batchSize)
      case None =>
        val batches =
          try pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals))
          catch
            case _: NullPointerException =>
              throw new ColumnarFallback("countByKey source has a null pair component")
        HashCount.byKey(batches, width, workers, batchSize)

  /** Fully present int pairs. Anything else, including one boxed partition, stays on encode. */
  private def ownedPairs(
      plan: Plan[_],
      op: PhysicalOp,
  ): Option[Vector[(Array[Int], Array[Int])]] =
    import com.ewoodbury.scarlet.core.IntPairArrays

    (plan, op) match
      case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
        val owned = partitions.map { partition =>
          partition.data match
            case pairs: IntPairArrays => Some((pairs.keys, pairs.values))
            case _ => None
        }
        if (owned.forall(_.isDefined)) then Some(owned.flatten.toVector) else None
      case _ => None

  private def countOwned(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    if (steps.isEmpty) then FoldedCount.countOwnedUnmapped(parts, steps, width, workers, batchSize)
    else FoldedCount.countOwned(parts, steps, width, workers, batchSize)

  private def countEncoded(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    if (steps.isEmpty) then FoldedCount.countUnmapped(batches, steps, width, workers, batchSize)
    else FoldedCount.count(batches, steps, width, workers, batchSize)

  /**
   * Plan under the filters, the physical child to scan, and the steps that apply to that scan. One
   * [[Plan.MapValuesOp]] is removed when its child is not a map and every filter above it is an
   * int key slot. The steps under the map stay, in source order, ahead of those key slots.
   */
  private def foldTarget(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step])] =
    val (underPlan, underOp) = PredicatePlan.childOf(source, child)
    preserved(underPlan, underOp, source, child, ordinals).orElse(
      direct(underPlan, underOp, source, child, ordinals),
    )

  private def direct(
      underPlan: Plan[_],
      underOp: PhysicalOp,
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step])] =
    if (pastMap(underPlan)) then None
    else admitted(source, child, ordinals).map(steps => (underPlan, underOp, steps))

  private def preserved(
      underPlan: Plan[_],
      underOp: PhysicalOp,
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step])] =
    keyPreserving(underPlan, underOp).flatMap { (basePlan, baseOp) =>
      keySteps(source, child, ordinals).flatMap { above =>
        stacked(basePlan, baseOp, above, ordinals)
      }
    }

  /** One `mapValues` whose child is not itself a map. The function is not read. */
  private def keyPreserving(plan: Plan[_], op: PhysicalOp): Option[(Plan[_], PhysicalOp)] =
    (plan, op) match
      case (Plan.MapValuesOp(inner, _), PhysicalOp.MapValue(mapped)) if !pastMap(inner) =>
        Some((inner, mapped))
      case _ => None

  /**
   * Int key slots above the map, nearest the source first. `Some(Nil)` when nothing sits above the
   * map. `None` when a step would see the mapped value.
   */
  private def keySteps(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(source, child) match
      case None => Some(Nil)
      case Some(compiled) =>
        slotList(compiled, ordinals).filter(steps =>
          steps.forall(keySlot) && FoldedCount.admits(steps),
        )

  private def stacked(
      basePlan: Plan[_],
      baseOp: PhysicalOp,
      above: List[Selection.Step],
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step])] =
    val (deepPlan, deepOp) = PredicatePlan.childOf(basePlan, baseOp)
    if (pastMap(deepPlan)) then None
    else
      underSteps(basePlan, baseOp, ordinals).map { below =>
        (deepPlan, deepOp, below ++ above)
      }

  /** Steps under the map. `Some(Nil)` when the child is not a filter. */
  private def underSteps(
      plan: Plan[_],
      op: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(plan, op) match
      case None => Some(Nil)
      case Some(compiled) =>
        slotList(compiled, ordinals).filter(steps => FoldedCount.admits(steps))

  private def admitted(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(source, child).flatMap { compiled =>
      slotList(compiled, ordinals).filter(steps => FoldedCount.admits(steps))
    }

  private def slotList(
      compiled: PredicatePlan.Compiled,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.selectionSteps(PredicatePlan.encoded(compiled, ordinals)).map(_.toList)

  /** Column 0 only. A value or row step sees the mapped value, so the map has to run. */
  private def keySlot(step: Selection.Step): Boolean = step match
    case Selection.Step.Int32(column, _) => column == 0
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      false

  /** A map under the filters is interpreted by the index path. The steps would see its output. */
  private def pastMap(plan: Plan[_]): Boolean = plan match
    case Plan.MapOp(_, _) | Plan.MapValuesOp(_, _) | Plan.MapIntKeyOp(_, _) |
        Plan.FlatMapOp(_, _) | Plan.FlatMapValuesOp(_, _) | Plan.MapPartitionsOp(_, _) =>
      true
    case _ => false

  private def scanned(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Option[(PhysicalOp, Vector[ColumnBatch])] =
    try
      interpretAt(plan, op, selectedPartition, preserveAbsence, ordinals) match
        case Columns.Pairs(batches) => Some((op, batches))
        case _ => None
    catch case _: NullPointerException => None

  /**
   * The pair-row kernel drops a clear key bit. The row path keeps that row, so a sorted batch with
   * a null key stays on the index path, which falls back before the kernel.
   */
  private def sortDropsNullKey(op: PhysicalOp, batches: Vector[ColumnBatch]): Boolean =
    op match
      case _: PhysicalOp.SortByIntKey => FoldedCount.absentIntKey(batches)
      case _ => false
