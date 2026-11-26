package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.IntJoinTriplePredicate
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt}

/**
 * `countByKey` of a triple project whose schema is `[Int32, Int64]`.
 *
 * An int inner hash join under that project counts each probe hit under the projected key.
 * [[com.ewoodbury.scarlet.columnar.JoinWrite]] is not called, and [[HashCount.byKey]] is not
 * called. Partials are finished by [[HashCount.exchange]], the same exchange [[HashCount.byKey]]
 * uses after its partial combine. The map may replace the join key, so that second exchange stays.
 * One triple filter between the map and a hinted join is applied on those hits, and the match
 * columns are still not written. One key or value filter above that project applies the slot on
 * the probe and still does not write the match columns. A null projected key is dropped and the
 * key predicate is not called. The function bodies are not inspected. Any other project still
 * interprets the child, projects each morsel with [[TripleToIntLong.project]], then
 * [[HashCount.byKey]] counts ordinal 0. A join on that path still writes its match columns. A key
 * or value slot without that hashed join throws [[ColumnarFallback]] before a count is published.
 * A result that is not `(Int, Long)` throws [[ColumnarFallback]] from the offer or the project,
 * before a count is published. [[com.ewoodbury.scarlet.execution.JoinProbeAggregate]] is a
 * different plan and is not built here.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
  ),
)
private[execution] object IntLongCount:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // The long value is unread, so this is the int-pair count of the child. The function does
    // not run. A triple project stays on the match below.
    widenedKeys(source, child) match
      case Some((input, under)) =>
        CountByKeyArms.columns(input, under, selectedPartition, preserveAbsence, ordinals)
      case None =>
        countedProject(source, child, selectedPartition, preserveAbsence, ordinals)

  private def widenedKeys(source: Plan[_], child: PhysicalOp): Option[(Plan[_], PhysicalOp)] =
    (source, child) match
      case (Plan.MapValuesOp(input, _), PhysicalOp.IntLongMapValue(under)) =>
        Some((input, under))
      case _ => None

  private def countedProject(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    matched(source, child) match
      case Some(found) =>
        val absence = preserveAbsence || found.project.requestsTripleAbsence
        val adapted = TripleToIntLong.adapt(found.mapper)
        probedJoin(found.input, found.inputOp) match
          case Some(hit) =>
            Columns.Pairs(counted(hit, absence, ordinals, adapted, found.slot))
          case None if found.slot.isEmpty =>
            Columns.Pairs(
              HashCount.byKey(
                project(
                  tripleBatches(
                    interpretAt(
                      found.input,
                      found.inputOp,
                      selectedPartition,
                      absence,
                      ordinals,
                    ),
                  ),
                  adapted,
                ),
                ColumnarPlanner.shuffleWidth,
                ColumnarPlanner.parallelism,
                ColumnarPlanner.batchSize,
              ),
            )
          case None =>
            throw new ColumnarFallback("int-long countByKey expected a hashed join under the slot")
      case None =>
        throw new ColumnarFallback("int-long countByKey expected a triple project")

  private final case class Counted(
      input: Plan[_],
      inputOp: PhysicalOp,
      mapper: Any,
      project: PhysicalOp.Project,
      slot: Option[TripleToIntLong.IntLongSlot],
  )

  private def matched(source: Plan[_], child: PhysicalOp): Option[Counted] = (source, child) match
    case (Plan.MapOp(input, mapper), project @ PhysicalOp.Project(inputOp)) =>
      Some(Counted(input, inputOp, mapper, project, None))
    case (
          Plan.FilterKeysOp(Plan.MapOp(input, mapper), predicate),
          PhysicalOp.Filter(project @ PhysicalOp.Project(inputOp), PhysicalOp.Slot.Key),
        ) =>
      Some(
        Counted(
          input,
          inputOp,
          mapper,
          project,
          Some(TripleToIntLong.IntLongSlot.Key(predicate.asInstanceOf[Int => Boolean])),
        ),
      )
    case (
          Plan.FilterValuesOp(Plan.MapOp(input, mapper), predicate),
          PhysicalOp.Filter(project @ PhysicalOp.Project(inputOp), PhysicalOp.Slot.Value),
        ) =>
      Some(
        Counted(
          input,
          inputOp,
          mapper,
          project,
          Some(TripleToIntLong.IntLongSlot.Value(predicate.asInstanceOf[Long => Boolean])),
        ),
      )
    case _ => None

  /** Int inner hash join of two exchanges. Sort-merge and broadcast are already rewritten. */
  private def counted(
      hit: ProbeHit,
      absence: Boolean,
      ordinals: Set[Int],
      adapted: TripleToIntLong.TripleMap,
      slot: Option[TripleToIntLong.IntLongSlot],
  ): Vector[ColumnBatch] =
    hit.keep match
      case Some(_) if slot.nonEmpty =>
        throw new ColumnarFallback("int-long countByKey applies one filter")
      case Some(predicate) =>
        JoinArms.projectCount(
          hit.left,
          hit.right,
          hit.leftOp,
          hit.rightOp,
          absence,
          ordinals,
          adapted,
          predicate,
          hit.dropNull,
        )
      case None =>
        JoinArms.projectCount(
          hit.left,
          hit.right,
          hit.leftOp,
          hit.rightOp,
          absence,
          ordinals,
          adapted,
          slot,
        )

  /**
   * Hash join of two exchanges, or one triple filter above that join.
   *
   * The filter is only peeled for a hinted sort-merge or broadcast. A shuffle-hash filter still
   * interprets the child. The predicate body is not read: the slot selects the column, and a
   * whole-row filter is called as a triple. A second filter is not this shape.
   */
  private def probedJoin(input: Plan[_], inputOp: PhysicalOp): Option[ProbeHit] =
    input match
      case Plan.FilterOp(Plan.JoinOp(left, right, strategy), predicate)
          if JoinLowering.hintedInnerHashed(strategy) =>
        filteredHit(left, right, predicate, inputOp)
      case Plan.JoinOp(left, right, strategy)
          if JoinLowering.acceptsShuffleHash(strategy) ||
            JoinLowering.hintedInnerHashed(strategy) =>
        hashHit(left, right, inputOp)
      case _ => None

  private def hashHit(left: Plan[_], right: Plan[_], inputOp: PhysicalOp): Option[ProbeHit] =
    inputOp match
      case PhysicalOp.HashJoin(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ) =>
        Some(ProbeHit(left, right, leftOp, rightOp, None, false))
      case _ => None

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def filteredHit(
      left: Plan[_],
      right: Plan[_],
      predicate: Any,
      inputOp: PhysicalOp,
  ): Option[ProbeHit] =
    inputOp match
      case PhysicalOp.Filter(
            PhysicalOp.HashJoin(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
            slot,
          ) =>
        tripleKeep(predicate, slot).map { kept =>
          ProbeHit(left, right, leftOp, rightOp, Some(kept.predicate), kept.dropNull)
        }
      case _ => None

  private final case class ProbeHit(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      keep: Option[PairKernel.IntTriplePredicate],
      dropNull: Boolean,
  )

  private final case class Kept(predicate: PairKernel.IntTriplePredicate, dropNull: Boolean)

  /**
   * Named erasure boundary. The physical slot chose the function. A whole-row filter is
   * `(Int, (Int, Int)) => Boolean`; the cast is not a call. A slot filter is an
   * [[IntJoinTriplePredicate]] and `keep` reads one int. The bodies are not inspected.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
    ),
  )
  private def tripleKeep(predicate: Any, slot: PhysicalOp.Slot): Option[Kept] =
    slot match
      case PhysicalOp.Slot.TripleRow =>
        val keep = predicate.asInstanceOf[((Int, (Int, Int))) => Boolean]
        val adapted: PairKernel.IntTriplePredicate =
          (key, left, right) => keep((key, (left, right)))
        Some(Kept(adapted, dropNull = true))
      case PhysicalOp.Slot.TripleKey => columnKeep(predicate, 0)
      case PhysicalOp.Slot.TripleLeftValue => columnKeep(predicate, 1)
      case PhysicalOp.Slot.TripleRightValue => columnKeep(predicate, 2)
      case PhysicalOp.Slot.Element | PhysicalOp.Slot.Row | PhysicalOp.Slot.Key |
          PhysicalOp.Slot.Value =>
        None

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def columnKeep(predicate: Any, column: Int): Option[Kept] =
    predicate match
      case recognized: IntJoinTriplePredicate[?] =>
        val adapted: Option[PairKernel.IntTriplePredicate] = column match
          case 0 => Some((key, _, _) => recognized.keep(key))
          case 1 => Some((_, left, _) => recognized.keep(left))
          case 2 => Some((_, _, right) => recognized.keep(right))
          case _ => None
        adapted.map(keep => Kept(keep, dropNull = false))
      case _ => None

  /** One projected batch per morsel. [[HashCount.byKey]] drops the value column. */
  private def project(
      batches: Vector[ColumnBatch],
      mapper: TripleToIntLong.TripleMap,
  ): Vector[ColumnBatch] =
    val groups = batches.map(batch => Morsels.of(batch, ColumnarPlanner.batchSize))
    MorselScheduler.map(groups.flatten, ColumnarPlanner.parallelism) { (_, morsel) =>
      TripleToIntLong.project(morsel.batch, morsel.start, morsel.length, mapper)
    }

  private def tripleBatches(columns: Columns): Vector[ColumnBatch] =
    columns match
      case Columns.Triples(parts) => parts
      case _ => throw new ColumnarFallback("int-long countByKey expected triples")
