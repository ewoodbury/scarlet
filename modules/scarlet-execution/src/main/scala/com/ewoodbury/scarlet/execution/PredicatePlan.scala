package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.api.IntJoinTriplePredicate
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

/**
 * One compiled filter stack.
 *
 * [[compile]] walks consecutive filters and stops at a map, the same cut as [[FilterChain]]. The
 * cast reads the erased function once and does not call it. [[applyAtom]] runs one existing kernel
 * on the given range. It does not take a [[Compiled]], so an encoder cannot apply the predicate a
 * second time. [[selectionSteps]] returns steps for a pair, triple, string-pair, or long-pair slot
 * stack. An element stack is `None`, and the caller chains [[applyAtom]], one morsel visit per
 * atom. A long row atom is not a slot step. [[pairSteps]] is the keep vector for a pair stack. It
 * is not a second compile, and it is not [[selectionSteps]].
 *
 * Named erasure boundary. The physical slot chose the function. A wrong erased type throws
 * [[ColumnarFallback]], and [[ColumnarPlanner.tryRun]] returns `None`. A cast that fails while a
 * predicate runs is still a plain `ClassCastException`, and that catch treats it as the same
 * fallback.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] object PredicatePlan:

  /**
   * A Scala 3 enum already defines `ordinal` as the case index. This hierarchy keeps `ordinal` as
   * the column the kernel reads.
   */
  sealed trait Atom
  object Atom:

    final case class Int32(ordinal: Int, keep: ColumnKernel.Int32Predicate) extends Atom
    final case class Int64(ordinal: Int, keep: ColumnKernel.Int64Predicate) extends Atom
    final case class Float64(ordinal: Int, keep: ColumnKernel.Float64Predicate) extends Atom
    final case class Bool(ordinal: Int, keep: ColumnKernel.BoolPredicate) extends Atom
    final case class Utf8(ordinal: Int, keep: ColumnKernel.Utf8Predicate) extends Atom
    final case class Utf8Literals(ordinal: Int, groups: Vector[Vector[String]]) extends Atom
    final case class PairRow(keep: PairKernel.IntPairPredicate) extends Atom
    final case class TripleRow(keep: PairKernel.IntTriplePredicate) extends Atom
    final case class LongPairRow(keep: LongPairKernel.LongPairPredicate) extends Atom
    final case class LongPairKey(keep: ColumnKernel.Int64Predicate) extends Atom
    final case class LongPairValue(ordinal: Int, keep: ColumnKernel.Int64Predicate) extends Atom
    final case class PairKey(keep: ColumnKernel.Int32Predicate) extends Atom
    final case class PairValue(ordinal: Int, keep: ColumnKernel.Int32Predicate) extends Atom

    /** A whole-row `(String, Int)` predicate. It is not a slot atom, so it does not compact. */
    case object StringRow extends Atom

  final case class Compiled(atoms: Vector[Atom])

  /** `None` when `op` is not a filter stack. A mistyped predicate throws instead. */
  def compile(plan: Plan[_], op: PhysicalOp): Option[Compiled] =
    val found = layers(plan, op)
    if (found.isEmpty) then None
    else
      // A lone element filter keeps the old cast-only path: the cast succeeds and the kernel
      // raises on first use. The reflection check belongs to a stack, where a wrong atom would
      // otherwise run earlier user predicates before it fails.
      val strict = found.length > 1
      Some(Compiled(found.map(layer => toAtom(op.kind, layer, strict)).toVector))

  /**
   * One element predicate, classified from the function it declares. `None` means the declaration
   * is not an element filter. Pair and triple slots stay on [[compile]].
   */
  def filterAtom(predicate: Any, ordinal: Int): Option[Atom] =
    if (ColumnarPlanner.dictionaryStringLiterals(predicate).isDefined) then
      Some(stringAtom(ordinal, predicate))
    else if (declaresSpecialized(predicate, "apply$mcZI$sp", classOf[Int], classOf[Boolean])) then
      Some(int32Atom(ordinal, predicate))
    else if (declaresSpecialized(predicate, "apply$mcZJ$sp", classOf[Long], classOf[Boolean])) then
      Some(int64Atom(ordinal, predicate))
    else if (declaresSpecialized(predicate, "apply$mcZD$sp", classOf[Double], classOf[Boolean]))
    then Some(float64Atom(ordinal, predicate))
    else if (acceptsBoolean(predicate)) then Some(boolAtom(ordinal, predicate))
    else if (specializedApply(predicate)) then None
    else Some(stringAtom(ordinal, predicate))

  /**
   * One kernel on `[start, start + length)`. The predicate was adapted when the atom was built.
   */
  def applyAtom(batch: ColumnBatch, start: Int, length: Int, atom: Atom): ColumnBatch =
    atom match
      case Atom.Int32(_, keep: JoinColumn) =>
        PairKernel.filterTriple(batch, start, length, keep.onTriple)
      case Atom.Int32(ordinal, keep) =>
        ColumnKernel.filterInt32(batch, ordinal, start, length, keep)
      case Atom.Int64(ordinal, keep) =>
        ColumnKernel.filterInt64(batch, ordinal, start, length, keep)
      case Atom.Float64(ordinal, keep) =>
        ColumnKernel.filterFloat64(batch, ordinal, start, length, keep)
      case Atom.Bool(ordinal, keep) =>
        ColumnKernel.filterBool(batch, ordinal, start, length, keep)
      case Atom.Utf8(ordinal, keep) =>
        ColumnKernel.filterUtf8(batch, ordinal, start, length, keep)
      case Atom.Utf8Literals(ordinal, groups) =>
        ColumnKernel.filterUtf8ByValues(batch, ordinal, start, length, oneGroup(groups))
      case Atom.PairRow(keep) =>
        PairKernel.filter(batch, start, length, keep)
      case Atom.TripleRow(keep) =>
        PairKernel.filterTripleRow(batch, start, length, keep)
      case Atom.LongPairRow(keep) =>
        LongPairKernel.filter(batch, start, length, keep)
      case Atom.LongPairKey(keep) =>
        ColumnKernel.filterInt64(batch, encodedOrdinal(batch, 0), start, length, keep)
      case Atom.LongPairValue(ordinal, keep) =>
        ColumnKernel.filterInt64(batch, encodedOrdinal(batch, ordinal), start, length, keep)
      case Atom.PairKey(keep) =>
        ColumnKernel.filterInt32(batch, encodedOrdinal(batch, 0), start, length, keep)
      case Atom.PairValue(ordinal, keep) =>
        ColumnKernel.filterInt32(batch, encodedOrdinal(batch, ordinal), start, length, keep)
      case Atom.StringRow =>
        throw new ColumnarFallback("string pair row filter does not lower")

  /**
   * Pair, triple, string-pair, and long-pair slot steps. An element atom, including an int filter
   * at ordinal 0, is `None`, so that stack is not one [[Selection]] pass. A long key is
   * [[Selection.Step.Int64]] at ordinal 0. A long value is [[Selection.Step.Int64]] at the
   * compiled ordinal. Those atoms are not pair atoms, and [[encoded]] does not rewrite them. A
   * long row atom is not a slot step, so that stack is `None`. A string-pair key is
   * [[Selection.Step.Utf8]]. A string-pair value is [[Selection.Step.Int32]], the same step a pair
   * value already uses. A whole-row atom is not a slot atom, so that stack is `None`.
   */
  def selectionSteps(compiled: Compiled): Option[Vector[Selection.Step]] =
    if (compiled.atoms.isEmpty) then None
    else if (compiled.atoms.forall(isPairAtom)) then Some(compiled.atoms.map(pairSelection))
    else if (compiled.atoms.forall(isLongSlotAtom)) then Some(compiled.atoms.map(longSelection))
    else if (compiled.atoms.forall(isTripleAtom)) then Some(compiled.atoms.map(tripleSelection))
    else if (compiled.atoms.forall(isStringPairAtom)) then
      Some(compiled.atoms.map(stringPairSelection))
    else None

  /**
   * A one-column encode stores the demanded side at index 0. [[compile]] records logical ordinal 1
   * on [[Atom.PairValue]]. The root ordinal set decided the encode, and it is not on the atom. Any
   * other set keeps the compiled ordinals.
   */
  def encoded(compiled: Compiled, ordinals: Set[Int]): Compiled =
    if (ordinals.size == 1) then
      Compiled(compiled.atoms.map {
        case Atom.PairValue(_, keep) => Atom.PairValue(0, keep)
        case atom => atom
      })
    else compiled

  /**
   * Pair keep steps in source order. `None` unless every atom is a pair keep, so the vector is
   * only [[PairStep.KeepKey]], [[PairStep.KeepValue]], and [[PairStep.KeepRow]]. The predicate was
   * cast when the atom was built. This does not cast it again and does not run it. Interpreting
   * the filtered plan and also passing the vector to `sumByKey` applies it twice.
   */
  def pairSteps(compiled: Compiled): Option[Vector[PairStep]] =
    val seed: Option[Vector[PairStep]] = Some(Vector.empty[PairStep])
    val gathered = compiled.atoms.foldLeft(seed) { (acc, atom) =>
      (acc, atom) match
        case (Some(steps), Atom.PairKey(keep)) =>
          Some(steps :+ PairStep.KeepKey(value => keep(value)))
        case (Some(steps), Atom.PairValue(_, keep)) =>
          Some(steps :+ PairStep.KeepValue(value => keep(value)))
        case (Some(steps), Atom.PairRow(keep)) =>
          Some(steps :+ PairStep.KeepRow((key, value) => keep(key, value)))
        case _ => None
    }
    gathered.filter(_.nonEmpty)

  /**
   * Keep steps for a contiguous pair-filter prefix, and the plan under that prefix.
   *
   * The physical child only carries [[PhysicalOp.Kind.Pairs]] so [[compile]] can read the slots.
   * It is not interpreted. The returned plan is [[childOf]]'s source.
   */
  private[execution] def pairKeeps(plan: Plan[_]): (Plan[_], Vector[PairStep]) =
    val op = pairStack(plan)
    val compiled = compile(plan, op).getOrElse(missingStack)
    val steps = pairSteps(compiled).getOrElse(missingStack)
    val (child, _) = childOf(plan, op)
    (child, steps)

  /**
   * The atom of the filter this arm matched, which is the outermost one. A stack below it is
   * interpreted by the fold's own `interpret(source, child)` call, so it is not applied here.
   */
  private[execution] def int32Filter(plan: Plan[_], op: PhysicalOp): ColumnKernel.Int32Predicate =
    outerAtom(plan, op) match
      case Atom.Int32(_, keep) => keep
      case _ => missingFilter

  /** The outermost atom. A mistyped predicate throws [[ColumnarFallback]] from [[compile]]. */
  private[execution] def int64Filter(plan: Plan[_], op: PhysicalOp): ColumnKernel.Int64Predicate =
    outerAtom(plan, op) match
      case Atom.Int64(_, keep) => keep
      case _ => missingFilter

  /** The outermost atom. A mistyped predicate throws [[ColumnarFallback]] from [[compile]]. */
  private[execution] def float64Filter(
      plan: Plan[_],
      op: PhysicalOp,
  ): ColumnKernel.Float64Predicate =
    outerAtom(plan, op) match
      case Atom.Float64(_, keep) => keep
      case _ => missingFilter

  /** Dictionary groups in source order. `None` unless every atom is a literal group. */
  def literalGroups(compiled: Compiled): Option[Vector[Vector[String]]] =
    val seed: Option[Vector[Vector[String]]] = Some(Vector.empty[Vector[String]])
    val gathered = compiled.atoms.foldLeft(seed) { (acc, atom) =>
      (acc, atom) match
        case (Some(groups), Atom.Utf8Literals(_, next)) => Some(groups ++ next)
        case _ => None
    }
    gathered.filter(_.nonEmpty)

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean = one(plan, op).isDefined

  /**
   * Plan and operator under the filter stack. A plan that is not a filter is returned as itself.
   */
  def childOf(plan: Plan[_], op: PhysicalOp): (Plan[_], PhysicalOp) =
    @tailrec
    def loop(currentPlan: Plan[_], currentOp: PhysicalOp): (Plan[_], PhysicalOp) =
      one(currentPlan, currentOp) match
        case Some((source, child, _)) => loop(source, child)
        case None => (currentPlan, currentOp)

    loop(plan, op)

  /** Slot of the filter nearest the source. `None` when there is no filter. */
  def openingSlot(plan: Plan[_], op: PhysicalOp): Option[PhysicalOp.Slot] =
    layers(plan, op).headOption.map(_.slot)

  /** [[ColumnarFallback]] for a batch this stack does not compact. The slot picks the message. */
  def unsupported(slot: PhysicalOp.Slot): Nothing =
    throw new ColumnarFallback(slotMessage(slot))

  private def outerAtom(plan: Plan[_], op: PhysicalOp): Atom = compile(plan, op) match
    case Some(Compiled(atoms)) if atoms.nonEmpty => atoms.last
    case _ => missingFilter

  private def pairStack(plan: Plan[_]): PhysicalOp = plan match
    case Plan.FilterOp(source, _) =>
      PhysicalOp.Filter(pairStackChild(source), PhysicalOp.Slot.Row)
    case Plan.FilterKeysOp(source, _) =>
      PhysicalOp.Filter(pairStackChild(source), PhysicalOp.Slot.Key)
    case Plan.FilterValuesOp(source, _) =>
      PhysicalOp.Filter(pairStackChild(source), PhysicalOp.Slot.Value)
    case _ => missingStack

  private def pairStackChild(plan: Plan[_]): PhysicalOp = plan match
    case Plan.FilterOp(_, _) | Plan.FilterKeysOp(_, _) | Plan.FilterValuesOp(_, _) =>
      pairStack(plan)
    case _ => PhysicalOp.Scan(PhysicalOp.Kind.Pairs, Seq.empty)

  // Both are fallback signals: the planner catches ClassCastException, whose named form is
  // ColumnarFallback, and returns None, which sends the plan to the row path.
  private def missingFilter: Nothing =
    throw new ColumnarFallback("element filter did not compile")

  private def missingStack: Nothing =
    throw new ColumnarFallback("pair filter stack has no predicate")

  private final class JoinColumn(recognized: IntJoinTriplePredicate[?], ordinal: Int)
      extends ColumnKernel.Int32Predicate:

    val onTriple: PairKernel.IntTriplePredicate = ordinal match
      case 0 => (key, _, _) => recognized.keep(key)
      case 1 => (_, left, _) => recognized.keep(left)
      case 2 => (_, _, right) => recognized.keep(right)
      case _ => throw new IllegalStateException("triple filter lost its slot predicate")

    def apply(value: Int): Boolean = recognized.keep(value)

  private final case class Layer(predicate: Any, slot: PhysicalOp.Slot)

  private def layers(plan: Plan[_], op: PhysicalOp): List[Layer] =
    @tailrec
    def loop(currentPlan: Plan[_], currentOp: PhysicalOp, acc: List[Layer]): List[Layer] =
      one(currentPlan, currentOp) match
        case Some((source, child, layer)) => loop(source, child, layer :: acc)
        case None => acc

    loop(plan, op, Nil)

  private def one(plan: Plan[_], op: PhysicalOp): Option[(Plan[_], PhysicalOp, Layer)] =
    (plan, op) match
      case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, slot)) =>
        Some((source, child, Layer(predicate, slot)))
      case (Plan.FilterKeysOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Key)) =>
        Some((source, child, Layer(predicate, PhysicalOp.Slot.Key)))
      case (
            Plan.FilterValuesOp(source, predicate),
            PhysicalOp.Filter(child, PhysicalOp.Slot.Value),
          ) =>
        Some((source, child, Layer(predicate, PhysicalOp.Slot.Value)))
      case _ => None

  private def toAtom(kind: PhysicalOp.Kind, layer: Layer, strict: Boolean): Atom =
    (kind, layer.slot) match
      case (PhysicalOp.Kind.Ints, PhysicalOp.Slot.Element) =>
        int32Atom(0, layer.predicate, strict)
      case (PhysicalOp.Kind.Longs, PhysicalOp.Slot.Element) =>
        int64Atom(0, layer.predicate, strict)
      case (PhysicalOp.Kind.Floats, PhysicalOp.Slot.Element) =>
        float64Atom(0, layer.predicate, strict)
      case (PhysicalOp.Kind.Bools, PhysicalOp.Slot.Element) =>
        boolAtom(0, layer.predicate, strict)
      case (PhysicalOp.Kind.Strings, PhysicalOp.Slot.Element) =>
        stringAtom(0, layer.predicate, strict)
      case (PhysicalOp.Kind.Pairs, PhysicalOp.Slot.Key) => pairKey(layer.predicate)
      case (PhysicalOp.Kind.Pairs, PhysicalOp.Slot.Value) => pairValue(layer.predicate)
      case (PhysicalOp.Kind.Pairs, PhysicalOp.Slot.Row) => pairRow(layer.predicate)
      case (PhysicalOp.Kind.LongPairs, PhysicalOp.Slot.Row) => longPairRow(layer.predicate)
      case (PhysicalOp.Kind.LongPairs, PhysicalOp.Slot.Key) => longPairKey(layer.predicate)
      case (PhysicalOp.Kind.LongPairs, PhysicalOp.Slot.Value) => longPairValue(layer.predicate)
      case (PhysicalOp.Kind.LongPairs, slot) => unsupported(slot)
      case (PhysicalOp.Kind.StringPairs, PhysicalOp.Slot.Key) =>
        stringAtom(0, layer.predicate, strict)
      case (PhysicalOp.Kind.StringPairs, PhysicalOp.Slot.Value) =>
        pairValue(layer.predicate)
      case (PhysicalOp.Kind.StringPairs, PhysicalOp.Slot.Row) =>
        Atom.StringRow
      case (PhysicalOp.Kind.Triples, PhysicalOp.Slot.TripleKey) => triple(0, layer.predicate)
      case (PhysicalOp.Kind.Triples, PhysicalOp.Slot.TripleLeftValue) =>
        triple(1, layer.predicate)
      case (PhysicalOp.Kind.Triples, PhysicalOp.Slot.TripleRightValue) =>
        triple(2, layer.predicate)
      case (PhysicalOp.Kind.Triples, PhysicalOp.Slot.TripleRow) => tripleRow(layer.predicate)
      case (PhysicalOp.Kind.LongTriples, slot) => unsupported(slot)
      case (_, PhysicalOp.Slot.Element) =>
        throw new IllegalStateException(elementMessage)
      case (_, PhysicalOp.Slot.Row | PhysicalOp.Slot.Key | PhysicalOp.Slot.Value) =>
        throw new IllegalStateException(pairMessage)
      case (
            _,
            PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
            PhysicalOp.Slot.TripleRightValue | PhysicalOp.Slot.TripleRow,
          ) =>
        layer.predicate match
          case _: IntJoinTriplePredicate[?] => throw new IllegalStateException(tripleMessage)
          case _ => throw new IllegalStateException("triple filter lost its slot predicate")

  private def int32Atom(ordinal: Int, predicate: Any, strict: Boolean = true): Atom =
    if (strict) then requireSpecialized(predicate, "apply$mcZI$sp", classOf[Int], classOf[Boolean])
    val keep = predicate.asInstanceOf[Int => Boolean]
    val adapted: ColumnKernel.Int32Predicate = value => keep(value)
    Atom.Int32(ordinal, adapted)

  private def int64Atom(ordinal: Int, predicate: Any, strict: Boolean = true): Atom =
    if (strict) then
      requireSpecialized(predicate, "apply$mcZJ$sp", classOf[Long], classOf[Boolean])
    val keep = predicate.asInstanceOf[Long => Boolean]
    val adapted: ColumnKernel.Int64Predicate = value => keep(value)
    Atom.Int64(ordinal, adapted)

  private def float64Atom(ordinal: Int, predicate: Any, strict: Boolean = true): Atom =
    if (strict) then
      requireSpecialized(predicate, "apply$mcZD$sp", classOf[Double], classOf[Boolean])
    val keep = predicate.asInstanceOf[Double => Boolean]
    val adapted: ColumnKernel.Float64Predicate = value => keep(value)
    Atom.Float64(ordinal, adapted)

  private def boolAtom(ordinal: Int, predicate: Any, strict: Boolean = true): Atom =
    if (strict && !acceptsBoolean(predicate)) then rejectPredicate(predicate)
    val keep = predicate.asInstanceOf[Boolean => Boolean]
    val adapted: ColumnKernel.BoolPredicate = value => keep(value)
    Atom.Bool(ordinal, adapted)

  private def stringAtom(ordinal: Int, predicate: Any, strict: Boolean = true): Atom =
    ColumnarPlanner.dictionaryStringLiterals(predicate) match
      case Some(literals) => Atom.Utf8Literals(ordinal, Vector(literals))
      case None =>
        // Asymmetry with the primitive atoms is deliberate: a primitive-specialized `apply`
        // proves the function is not a `String => Boolean`. The primitive builders require that
        // proof; a string filter rejects it.
        if (strict && specializedApply(predicate)) then rejectPredicate(predicate)
        val keep = predicate.asInstanceOf[String => Boolean]
        val adapted: ColumnKernel.Utf8Predicate = value => keep(value)
        Atom.Utf8(ordinal, adapted)

  private def pairKey(predicate: Any): Atom =
    // The cast succeeds for any `Function1`. A long function throws when the kernel calls it.
    val keep = predicate.asInstanceOf[Int => Boolean]
    val adapted: ColumnKernel.Int32Predicate = value => keep(value)
    Atom.PairKey(adapted)

  private def pairValue(predicate: Any): Atom =
    val keep = predicate.asInstanceOf[Int => Boolean]
    val adapted: ColumnKernel.Int32Predicate = value => keep(value)
    Atom.PairValue(1, adapted)

  private def pairRow(predicate: Any): Atom =
    // `(Int, Int) => Boolean` boxes the pair. The adapter is built once, not per row.
    val keep = predicate.asInstanceOf[((Int, Int)) => Boolean]
    val adapted: PairKernel.IntPairPredicate = (key, value) => keep((key, value))
    Atom.PairRow(adapted)

  private def tripleRow(predicate: Any): Atom =
    // `(Int, (Int, Int)) => Boolean` boxes the row. The adapter is built once, not per row.
    // The cast succeeds for any `Function1`. A wrong function throws when the kernel calls it.
    val keep = predicate.asInstanceOf[((Int, (Int, Int))) => Boolean]
    val adapted: PairKernel.IntTriplePredicate = (key, left, right) => keep((key, (left, right)))
    Atom.TripleRow(adapted)

  private def longPairRow(predicate: Any): Atom =
    // The adapter is built once, not per row, but `(Long, Long) => Boolean` still boxes the two
    // longs and the pair on every call. That boundary is the same one `pairRow` pays.
    // The cast succeeds for any `Function1`. A wrong function throws when the kernel calls it.
    val keep = predicate.asInstanceOf[((Long, Long)) => Boolean]
    val adapted: LongPairKernel.LongPairPredicate = (key, value) => keep((key, value))
    Atom.LongPairRow(adapted)

  private def longPairKey(predicate: Any): Atom =
    // The cast succeeds for any `Function1`. A wrong function throws when the kernel calls it.
    val keep = predicate.asInstanceOf[Long => Boolean]
    val adapted: ColumnKernel.Int64Predicate = value => keep(value)
    Atom.LongPairKey(adapted)

  private def longPairValue(predicate: Any): Atom =
    val keep = predicate.asInstanceOf[Long => Boolean]
    val adapted: ColumnKernel.Int64Predicate = value => keep(value)
    Atom.LongPairValue(1, adapted)

  private def triple(ordinal: Int, predicate: Any): Atom =
    val recognized = predicate match
      case found: IntJoinTriplePredicate[?] => found
      case _ => throw new IllegalStateException("triple filter lost its slot predicate")
    Atom.Int32(ordinal, new JoinColumn(recognized, ordinal))

  /** Key or value only. A [[Atom.LongPairRow]] is not a slot step. */
  private def isLongSlotAtom(atom: Atom): Boolean = atom match
    case Atom.LongPairKey(_) | Atom.LongPairValue(_, _) => true
    case Atom.PairRow(_) | Atom.PairKey(_) | Atom.PairValue(_, _) | Atom.TripleRow(_) |
        Atom.LongPairRow(_) | Atom.Int32(_, _) | Atom.Int64(_, _) | Atom.Float64(_, _) |
        Atom.Bool(_, _) | Atom.Utf8(_, _) | Atom.Utf8Literals(_, _) | Atom.StringRow =>
      false

  private def isPairAtom(atom: Atom): Boolean = atom match
    case Atom.PairRow(_) | Atom.PairKey(_) | Atom.PairValue(_, _) => true
    case Atom.TripleRow(_) | Atom.LongPairRow(_) | Atom.LongPairKey(_) | Atom.LongPairValue(_, _) |
        Atom.Int32(_, _) | Atom.Int64(_, _) | Atom.Float64(_, _) | Atom.Bool(_, _) |
        Atom.Utf8(_, _) | Atom.Utf8Literals(_, _) | Atom.StringRow =>
      false

  private def isTripleAtom(atom: Atom): Boolean = atom match
    case Atom.Int32(_, _: JoinColumn) => true
    case Atom.TripleRow(_) | Atom.Int32(_, _) | Atom.Int64(_, _) | Atom.Float64(_, _) |
        Atom.Bool(_, _) | Atom.Utf8(_, _) | Atom.Utf8Literals(_, _) | Atom.PairRow(_) |
        Atom.LongPairRow(_) | Atom.LongPairKey(_) | Atom.LongPairValue(_, _) | Atom.PairKey(_) |
        Atom.PairValue(_, _) | Atom.StringRow =>
      false

  /** Key [[Selection.Step.Utf8]] or value [[Selection.Step.Int32]]. A row atom is not one. */
  private def isStringPairAtom(atom: Atom): Boolean = atom match
    case Atom.Utf8(_, _) | Atom.Utf8Literals(_, _) | Atom.PairValue(_, _) => true
    case Atom.Int32(_, _) | Atom.Int64(_, _) | Atom.Float64(_, _) | Atom.Bool(_, _) |
        Atom.PairRow(_) | Atom.TripleRow(_) | Atom.LongPairRow(_) | Atom.LongPairKey(_) |
        Atom.LongPairValue(_, _) | Atom.PairKey(_) | Atom.StringRow =>
      false

  private def longSelection(atom: Atom): Selection.Step = atom match
    case Atom.LongPairKey(keep) => Selection.Step.Int64(0, keep)
    case Atom.LongPairValue(ordinal, keep) => Selection.Step.Int64(ordinal, keep)
    case Atom.PairRow(_) | Atom.PairKey(_) | Atom.PairValue(_, _) | Atom.TripleRow(_) |
        Atom.LongPairRow(_) | Atom.Int32(_, _) | Atom.Int64(_, _) | Atom.Float64(_, _) |
        Atom.Bool(_, _) | Atom.Utf8(_, _) | Atom.Utf8Literals(_, _) | Atom.StringRow =>
      throw new IllegalStateException("long pair selection expected a key or a value")

  private def pairSelection(atom: Atom): Selection.Step = atom match
    case Atom.PairKey(keep) => Selection.Step.Int32(0, keep)
    case Atom.PairValue(ordinal, keep) => Selection.Step.Int32(ordinal, keep)
    case Atom.PairRow(keep) => Selection.Step.IntPair(keep)
    case Atom.TripleRow(_) | Atom.LongPairRow(_) | Atom.LongPairKey(_) | Atom.LongPairValue(_, _) |
        Atom.Int32(_, _) | Atom.Int64(_, _) | Atom.Float64(_, _) | Atom.Bool(_, _) |
        Atom.Utf8(_, _) | Atom.Utf8Literals(_, _) | Atom.StringRow =>
      throw new IllegalStateException(pairMessage)

  private def tripleSelection(atom: Atom): Selection.Step = atom match
    case Atom.Int32(ordinal, keep) => Selection.Step.IntTriple(ordinal, keep)
    case Atom.TripleRow(_) | Atom.Int64(_, _) | Atom.Float64(_, _) | Atom.Bool(_, _) |
        Atom.Utf8(_, _) | Atom.Utf8Literals(_, _) | Atom.PairRow(_) | Atom.LongPairRow(_) |
        Atom.LongPairKey(_) | Atom.LongPairValue(_, _) | Atom.PairKey(_) | Atom.PairValue(_, _) |
        Atom.StringRow =>
      throw new IllegalStateException(tripleMessage)

  private def stringPairSelection(atom: Atom): Selection.Step = atom match
    case Atom.Utf8(ordinal, keep) => Selection.Step.Utf8(ordinal, keep)
    case Atom.Utf8Literals(ordinal, groups) =>
      val keep: ColumnKernel.Utf8Predicate = value => groups.exists(_.contains(value))
      Selection.Step.Utf8(ordinal, keep)
    case Atom.PairValue(ordinal, keep) => Selection.Step.Int32(ordinal, keep)
    case Atom.Int32(_, _) | Atom.Int64(_, _) | Atom.Float64(_, _) | Atom.Bool(_, _) |
        Atom.PairRow(_) | Atom.TripleRow(_) | Atom.LongPairRow(_) | Atom.LongPairKey(_) |
        Atom.LongPairValue(_, _) | Atom.PairKey(_) | Atom.StringRow =>
      throw new IllegalStateException("string pair selection expected a key or a value")

  /** A one-column encode stores the demanded ordinal at index 0. */
  private def encodedOrdinal(batch: ColumnBatch, ordinal: Int): Int =
    if (batch.columns.length == 1) then 0 else ordinal

  private def oneGroup(groups: Vector[Vector[String]]): Vector[String] = groups match
    case Vector(literals) => literals
    case _ => throw new IllegalStateException("a literal atom has one group")

  private val elementMessage =
    "columnar element filter expects ints, longs, floats, booleans, or strings"

  private val pairMessage = "columnar op expected int pairs"

  private[execution] val tripleMessage =
    "columnar op expected int triples"

  /** An outer batch carries an absent side, so the row predicate cannot read it as a triple. */
  private[execution] val outerMessage =
    "columnar filter does not support an outer-join batch"

  private def slotMessage(slot: PhysicalOp.Slot): String = slot match
    case PhysicalOp.Slot.Element => elementMessage
    case PhysicalOp.Slot.Row | PhysicalOp.Slot.Key | PhysicalOp.Slot.Value => pairMessage
    case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
        PhysicalOp.Slot.TripleRightValue | PhysicalOp.Slot.TripleRow =>
      tripleMessage

  private def requireSpecialized(
      predicate: Any,
      specialized: String,
      argument: Class[_],
      result: Class[_],
  ): Unit =
    if (!declaresSpecialized(predicate, specialized, argument, result)) then
      rejectPredicate(predicate)

  private def declaresSpecialized(
      predicate: Any,
      specialized: String,
      argument: Class[_],
      result: Class[_],
  ): Boolean =
    predicate.getClass.getDeclaredMethods.exists { method =>
      !method.isBridge &&
      method.getName == specialized &&
      method.getParameterCount == 1 &&
      method.getParameterTypes()(0).eq(argument) &&
      method.getReturnType.eq(result)
    }

  /** A primitive specialized `apply` is the implementation. A bridge does not count. */
  private def specializedApply(predicate: Any): Boolean =
    predicate.getClass.getDeclaredMethods.exists { method =>
      method.getName.startsWith("apply$mc") && !method.isBridge
    }

  private def acceptsBoolean(predicate: Any): Boolean =
    !specializedApply(predicate) && predicate.getClass.getDeclaredMethods.exists(booleanApply)

  private def booleanApply(method: java.lang.reflect.Method): Boolean =
    !method.isBridge &&
      method.getName == "apply" &&
      method.getParameterCount == 1 &&
      (primitiveBoolean(method) || erasedBoolean(method) || boxedBoolean(method))

  private def primitiveBoolean(method: java.lang.reflect.Method): Boolean =
    method.getParameterTypes()(0).eq(classOf[Boolean]) &&
      method.getReturnType.eq(classOf[Boolean])

  /** A lambda declares `apply(Object)` and returns a primitive boolean. */
  private def erasedBoolean(method: java.lang.reflect.Method): Boolean =
    method.getParameterTypes()(0).eq(classOf[Object]) &&
      method.getReturnType.eq(classOf[Boolean])

  private def boxedBoolean(method: java.lang.reflect.Method): Boolean =
    method.getParameterTypes()(0).eq(classOf[Object]) &&
      method.getReturnType.eq(classOf[java.lang.Boolean])

  private def rejectPredicate(predicate: Any): Nothing =
    throw new ColumnarFallback(s"columnar filter predicate is ${predicate.getClass.getName}")
