package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * `reduceByKey` for int pairs, long pairs, and string pairs.
 *
 * A slot-filter stack on an int or string child partial-reduces survivor ids, including a stack of
 * one. A long key, value, or row stack folds those predicates into the partial table and does not
 * build the index. An owned long-pair source is read in place. A boxed source is still encoded,
 * which is what keeps a null key. An int-pair row filter and a whole-row string filter still use
 * the index. The exchange sees dense partials. An int-pair [[PhysicalOp.Project]] is reduced from
 * that project's input morsels. The interpreter's adapter runs on every row, and a present
 * projected key and value are inserted into the [[HashReduce]] table. The pair batch is not
 * published, and [[HashReduce.byKey]] is not called. The function is not inspected. A null
 * projected key is dropped. A null projected value is dropped and does not create a group. One
 * [[Plan.FilterKeysOp]] or [[Plan.FilterValuesOp]] directly above that project applies the slot to
 * the projected pair and still does not publish. A key predicate is not called on a null projected
 * key. A value predicate is not called on a null projected value. A present partner on the other
 * column is still tested, and a null partner does not create a group. A second slot, a whole-row
 * filter, and a second map stay on the published selection path. Long and string slot-above are
 * not this arm. An int [[PhysicalOp.MapKey]] is reduced from that map's source morsels. Each
 * present key is passed to the `Int => Int` witness. A null key is not passed and is not inserted.
 * A null value is dropped and does not create a group. That pair batch is not published, and
 * [[HashReduce.byKey]] is not called. One [[Plan.FilterKeysOp]] or [[Plan.FilterValuesOp]]
 * directly above that map applies the slot after the witness and still does not publish. A null
 * input key is not passed to the witness or to a key predicate. A value predicate still sees that
 * row. A null mapped key is dropped. A null value does not create a group. A second slot, a
 * whole-row filter, and a second map stay off this arm. A long-pair [[PhysicalOp.Project]] is
 * reduced from that project's input morsels. The interpreter's adapter runs on every row, and a
 * present projected key and value are inserted into the [[LongHashReduce]] table. A null projected
 * key is dropped. A null projected value stays a null group until another value arrives, and a
 * later combine unboxes that null to `0`. The pair batch is not published, and
 * [[LongHashReduce.byKey]] is not called. The function is not inspected. A filter above that
 * project is not this arm. A string project stays on its own arm. A scan and a non-pair child stay
 * on the dense reduce. The `None` path is the only one that interprets a child the fold did not
 * already scan.
 */
private[execution] object ReduceArms:

  def intByKey[V](
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: (V, V) => V,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // Named erasure boundary. Grouped pair reduction declares `(Int, Int) => Int`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
    // A slot-filter stack scans survivor ids, including one filter. An int key map, one int slot
    // above that map, an int-pair project, and one int slot above that project each reduce from
    // the input morsels and do not publish. The `None` path interprets any other child.
    val binOp: HashReduce.IntBinOp = (left: Int, right: Int) => op(left, right)
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val reduced = reduceAboveMapped(
      source,
      child,
      binOp,
      selectedPartition,
      preserveAbsence,
      ordinals,
      width,
      workers,
      batchSize,
    ).orElse(
      reduceMapped(
        source,
        child,
        binOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
        width,
        workers,
        batchSize,
      ),
    ).orElse(
      reduceProjected(
        source,
        child,
        binOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
        width,
        workers,
        batchSize,
      ),
    ).orElse(
      reduceAboveProject(
        source,
        child,
        binOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
        width,
        workers,
        batchSize,
      ),
    ).getOrElse {
      FilterChain.selected(
        source,
        child,
        selectedPartition,
        preserveAbsence,
        ordinals,
      ) match
        case Some(parts) =>
          HashReduce.byKeySelected(parts, width, workers, batchSize, binOp)
        case None =>
          HashReduce.byKey(
            pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
            width,
            workers,
            batchSize,
            binOp,
          )
    }
    Columns.Pairs(reduced)

  /**
   * One [[Plan.MapIntKeyOp]]. The source under the key map is interpreted, then each morsel is
   * reduced through [[MappedKeyReduce]]. `None` when this child is not that map. A null key is not
   * passed to the witness. A null value is dropped and does not create a group.
   * [[HashReduce.byKey]] is not called. [[MapIntKey.columns]] is not called.
   */
  private def reduceMapped(
      source: Plan[_],
      child: PhysicalOp,
      binOp: HashReduce.IntBinOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    (source, child) match
      case (Plan.MapIntKeyOp(inner, fn), PhysicalOp.MapKey(under)) if intPairs(child) =>
        val columns =
          try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
          catch
            case _: NullPointerException =>
              throw new ColumnarFallback("reduceByKey source has a null pair component")
        columns match
          case Columns.Pairs(batches) if batches.nonEmpty =>
            Some(MappedKeyReduce.reduce(batches, fn, width, workers, batchSize, binOp))
          case Columns.Pairs(_) =>
            throw new IllegalStateException("columnar pairs have no partitions")
          case _ =>
            throw new IllegalStateException("mapped key reduce expected int pairs")
      case _ => None

  /**
   * One key or value slot directly above one [[Plan.MapIntKeyOp]]. The map's input is interpreted,
   * then each morsel is reduced through [[MappedAboveReduce]]. `None` when a second map sits under
   * the key map, or when the step is not one int key or value. A null input key is not passed to
   * the witness or to a key predicate. A value predicate still sees that row. A null mapped key is
   * dropped. A null value does not create a group. [[HashReduce.byKey]] is not called.
   * [[MapIntKey.columns]] is not called.
   */
  private def reduceAboveMapped(
      source: Plan[_],
      child: PhysicalOp,
      binOp: HashReduce.IntBinOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    val matched = (source, child) match
      case (
            Plan.FilterKeysOp(Plan.MapIntKeyOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.MapKey(under), PhysicalOp.Slot.Key),
          ) if intPairs(child) =>
        Some((inner, under, fn))
      case (
            Plan.FilterValuesOp(Plan.MapIntKeyOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.MapKey(under), PhysicalOp.Slot.Value),
          ) if intPairs(child) =>
        Some((inner, under, fn))
      case _ => None
    matched.flatMap { (inner, under, fn) =>
      if (deeperMap(inner, under)) then None
      else
        oneIntSlot(source, child).map { step =>
          val columns =
            try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
            catch
              case _: NullPointerException =>
                throw new ColumnarFallback("reduceByKey source has a null pair component")
          columns match
            case Columns.Pairs(batches) if batches.nonEmpty =>
              MappedAboveReduce.reduce(batches, fn, step, width, workers, batchSize, binOp)
            case Columns.Pairs(_) =>
              throw new IllegalStateException("columnar pairs have no partitions")
            case _ =>
              throw new IllegalStateException("mapped key reduce expected int pairs")
        }
    }

  /**
   * One pair [[Plan.MapOp]]. The input of the project is interpreted, then each morsel is reduced
   * through [[IntPairProject.visitPairs]]. `None` when this child is not that project. A null key
   * and a null value are passed to the adapter. A null projected key is dropped. A null projected
   * value is dropped and does not create a group. [[HashReduce.byKey]] is not called.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def reduceProjected(
      source: Plan[_],
      child: PhysicalOp,
      binOp: HashReduce.IntBinOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    (source, child) match
      case (Plan.MapOp(inner, fn), PhysicalOp.Project(under)) if intPairs(child) =>
        val adapted = IntPairProject.adapt(fn)
        val columns =
          try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
          catch
            case _: NullPointerException =>
              throw new ColumnarFallback("reduceByKey source has a null pair component")
        columns match
          case Columns.Pairs(batches) if batches.nonEmpty =>
            Some(
              ProjectedPairReduce.reduce(batches, adapted, width, workers, batchSize, binOp),
            )
          case Columns.Pairs(_) =>
            throw new IllegalStateException("columnar pairs have no partitions")
          case _ =>
            throw new IllegalStateException("pair project reduce expected int pairs")
      case _ => None

  /**
   * One key or value slot directly above one int-pair project. The project's input is interpreted,
   * then each morsel is reduced through [[IntPairProject.visitFiltered]]. `None` when a second map
   * sits under the project, or when the step is not one int key or value. A null key and a null
   * value are passed to the adapter. A null projected key is dropped and the key predicate is not
   * called. A null projected value is dropped and does not create a group, and the value predicate
   * is not called. [[HashReduce.byKey]] is not called.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def reduceAboveProject(
      source: Plan[_],
      child: PhysicalOp,
      binOp: HashReduce.IntBinOp,
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
      if (deeperMap(inner, under)) then None
      else
        oneIntSlot(source, child).map { step =>
          val adapted = IntPairProject.adapt(fn)
          val columns =
            try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
            catch
              case _: NullPointerException =>
                throw new ColumnarFallback("reduceByKey source has a null pair component")
          columns match
            case Columns.Pairs(batches) if batches.nonEmpty =>
              ProjectedAboveReduce.reduce(
                batches,
                adapted,
                step,
                width,
                workers,
                batchSize,
                binOp,
              )
            case Columns.Pairs(_) =>
              throw new IllegalStateException("columnar pairs have no partitions")
            case _ =>
              throw new IllegalStateException("pair project reduce expected int pairs")
        }
    }

  /** One int key or value on the projected pair. A row step and a second slot are `None`. */
  private def oneIntSlot(source: Plan[_], child: PhysicalOp): Option[Selection.Step] =
    PredicatePlan.compile(source, child).flatMap { compiled =>
      PredicatePlan.selectionSteps(compiled) match
        case Some(Vector(step @ Selection.Step.Int32(column, _))) if column == 0 || column == 1 =>
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

  /**
   * An int-pair project. A long project has its own arm. A string project stays on the row arm.
   */
  private def intPairs(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Pairs => true
    case _ => false

  /** A long-pair project. A filter above that project is not this arm. */
  private def longPairs(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.LongPairs => true
    case _ => false

  /**
   * One long-pair [[Plan.MapOp]]. The input of the project is interpreted, then each morsel is
   * reduced through [[LongPairProject.visitPairs]]. `None` when this child is not that project,
   * including a filter above it. A null key and a null value are passed to the adapter. A null
   * projected key is dropped. A null projected value is a null group. [[LongHashReduce.byKey]] is
   * not called.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def reduceProjectedLong(
      source: Plan[_],
      child: PhysicalOp,
      binOp: LongHashReduce.LongBinOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    (source, child) match
      case (Plan.MapOp(inner, fn), PhysicalOp.Project(under)) if longPairs(child) =>
        val adapted = LongPairProject.adapt(fn)
        val columns =
          try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
          catch
            case _: NullPointerException =>
              throw new ColumnarFallback("reduceByKey source has a null pair component")
        columns match
          case Columns.LongPairs(batches) if batches.nonEmpty =>
            Some(
              ProjectedLongPairReduce.reduce(batches, adapted, width, workers, batchSize, binOp),
            )
          case Columns.LongPairs(_) =>
            throw new IllegalStateException("columnar pairs have no partitions")
          case _ =>
            throw new IllegalStateException("pair project reduce expected long pairs")
      case _ => None

  def longByKey[V](
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: (V, V) => V,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // Named erasure boundary. Grouped long-pair reduction declares `(Long, Long) => Long`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val op = reduceFunc.asInstanceOf[(Long, Long) => Long]
    // A long-pair project reduces from the input morsels and does not publish. A long key, value,
    // or row stack folds the predicates into the partial table. The index path remains for a
    // stack the fold does not admit. The `None` path is the only one that interprets a child the
    // fold did not already scan. The same plan function keeps one adapter, so a retained partial
    // table accepts the next collect.
    val binOp = longBinOp(op)
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val reduced = reduceProjectedLong(
      source,
      child,
      binOp,
      selectedPartition,
      preserveAbsence,
      ordinals,
      width,
      workers,
      batchSize,
    ).orElse(
      foldLong(
        source,
        child,
        binOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
        width,
        workers,
        batchSize,
      ),
    ).getOrElse {
      FilterChain.selected(
        source,
        child,
        selectedPartition,
        preserveAbsence,
        ordinals,
      ) match
        case Some(parts) =>
          LongHashReduce.byKeySelected(parts, width, workers, batchSize, binOp)
        case None =>
          LongHashReduce.byKey(
            longPairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
            width,
            workers,
            batchSize,
            binOp,
          )
    }
    Columns.LongPairs(reduced)

  /**
   * Scan the child under the filter stack once and fold `steps` into the partial table. `None`
   * when the stack is not a long key, value, or row selection. The caller then keeps the index
   * path, which interprets that child itself. An owned long-pair scan is not encoded first.
   */
  private def foldLong(
      source: Plan[_],
      child: PhysicalOp,
      binOp: LongHashReduce.LongBinOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    longSteps(source, child, ordinals).map { steps =>
      val (underPlan, underOp) = PredicatePlan.childOf(source, child)
      val batches = sharedLongPairs(underPlan, underOp, selectedPartition, ordinals).getOrElse(
        longPairsOf(
          interpretAt(underPlan, underOp, selectedPartition, preserveAbsence, ordinals),
        ),
      )
      FoldedLongReduce.reduce(batches, steps, width, workers, batchSize, binOp)
    }

  /**
   * Owned long-pair partitions for an in-place fold. `None` when the child is not that scan, a
   * selector asked for one partition, or the demand is a single column. A boxed partition stays on
   * the encoder.
   */
  private def sharedLongPairs(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      ordinals: Set[Int],
  ): Option[Vector[ColumnBatch]] =
    if (selectedPartition.nonEmpty) then None
    else if (ordinals.size == 1 && (ordinals.contains(0) || ordinals.contains(1))) then None
    else
      (plan, op) match
        case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, _)) =>
          SourceEncode.shareOwnedLongPairs(partitions)
        case _ => None

  /**
   * One adapter per plan function. A fresh lambda on every collect would miss the retained table,
   * because reuse requires the same operator instance. The plan holds `op`, so the weak key dies
   * with the plan. The adapter does not keep the key alive.
   */
  private val longOpLock = new Object
  private val longOps =
    new java.util.WeakHashMap[(Long, Long) => Long, LongHashReduce.LongBinOp]

  private def longBinOp(op: (Long, Long) => Long): LongHashReduce.LongBinOp =
    longOpLock.synchronized {
      Option(longOps.get(op)).getOrElse {
        val created: LongHashReduce.LongBinOp = (left, right) => op(left, right)
        longOps.put(op, created)
        created
      }
    }

  /** Long selection steps, in source order. Empty and non-long stacks are `None`. */
  private def longSteps(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(source, child).flatMap { compiled =>
      PredicatePlan
        .selectionSteps(PredicatePlan.encoded(compiled, ordinals))
        .map(_.toList)
        .filter(FoldedLongReduce.admits)
    }

  def stringByKey[V](
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: (V, V) => V,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // Named erasure boundary. Grouped string-pair reduction declares `(Int, Int) => Int`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    FilterChain.selected(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    ) match
      case Some(parts) =>
        StringPairWalk.refuseSelectedNullValues(parts)
        Columns.StringPairs(StringHashReduce.byKeySelected(parts, width, workers, batchSize, op))
      case None =>
        val batches = StringPairWalk.batches(
          interpretAt(
            source,
            child,
            selectedPartition,
            preserveAbsence,
            ordinals,
          ),
        )
        StringPairWalk.refuseNullValues(batches)
        Columns.StringPairs(StringHashReduce.byKey(batches, width, workers, batchSize, op))
