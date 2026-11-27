package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

/**
 * One stderr line for a lowering that returns `None`.
 *
 * [[ColumnarPlanner.lower]] calls [[traced]] only when `ScarletConf.traceLowering` is set. Every
 * other call returns before a string is built. A successful `lower` does not reach [[note]]. A
 * speculative `None` that a later arm recovers is dropped: [[traced]] prints only when the whole
 * plan stays empty, and it prints one line.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object LoweringTrace:

  private final class Slot:
    var line: String = ""

  private val slots: ThreadLocal[Slot] = ThreadLocal.withInitial(() => new Slot)

  /** Run `run` and print the refusal line when it returns `None`. The flag is already on. */
  def traced(plan: Plan[_])(run: Plan[_] => Option[PhysicalOp]): Option[PhysicalOp] =
    val slot = slots.get()
    slot.line = ""
    try
      val result = run(plan)
      if (result.isEmpty) then Console.err.println(shown(slot.line, plan))
      result
    finally slot.line = ""

  /** Direct refusal addressed by the plan-node name. The name is a constant. */
  def note(node: String, kind: PhysicalOp.Kind, lowering: String): Unit =
    if (ScarletConf.get.traceLowering) then record(node, kindName(kind), lowering)

  /** Two children lowered and their kinds do not make a kernel. */
  def note(
      node: Plan[_],
      left: PhysicalOp.Kind,
      right: PhysicalOp.Kind,
      lowering: String,
  ): Unit =
    if (ScarletConf.get.traceLowering) then record(nodeName(node), pairKind(left, right), lowering)

  /** Two children lowered and their kinds do not make a kernel. The node name is a constant. */
  def note(
      node: String,
      left: PhysicalOp.Kind,
      right: PhysicalOp.Kind,
      lowering: String,
  ): Unit =
    if (ScarletConf.get.traceLowering) then record(node, pairKind(left, right), lowering)

  /** Refusal with no physical kind in scope. */
  def note(node: Plan[_], lowering: String): Unit =
    if (ScarletConf.get.traceLowering) then record(nodeName(node), "none", lowering)

  /** Propagation. Keeps a child's note. Records `lowering` only when nothing else has. */
  def noteIfEmpty(node: Plan[_], lowering: String): Unit =
    if (ScarletConf.get.traceLowering && slots.get().line.isEmpty) then
      record(nodeName(node), "none", lowering)

  /** Propagation. The node name is a constant. */
  def noteIfEmpty(node: String, lowering: String): Unit =
    if (ScarletConf.get.traceLowering && slots.get().line.isEmpty) then
      record(node, "none", lowering)

  private def shown(noted: String, plan: Plan[_]): String =
    if (noted.nonEmpty) then noted else fallback(plan)

  private def fallback(plan: Plan[_]): String =
    s"lowering refused node=${nodeName(plan)} kind=none lowering=ColumnarPlanner"

  private def record(node: String, kind: String, lowering: String): Unit =
    slots.get().line = s"lowering refused node=$node kind=$kind lowering=$lowering"

  private def pairKind(left: PhysicalOp.Kind, right: PhysicalOp.Kind): String =
    s"${kindName(left)}/${kindName(right)}"

  private def kindName(kind: PhysicalOp.Kind): String = kind match
    case PhysicalOp.Kind.Ints => "Ints"
    case PhysicalOp.Kind.Longs => "Longs"
    case PhysicalOp.Kind.Floats => "Floats"
    case PhysicalOp.Kind.Pairs => "Pairs"
    case PhysicalOp.Kind.Triples => "Triples"
    case PhysicalOp.Kind.Strings => "Strings"
    case PhysicalOp.Kind.Bools => "Bools"
    case PhysicalOp.Kind.LongPairs => "LongPairs"
    case PhysicalOp.Kind.LongCounts => "LongCounts"
    case PhysicalOp.Kind.LongTriples => "LongTriples"
    case PhysicalOp.Kind.StringPairs => "StringPairs"
    case PhysicalOp.Kind.StringTriples => "StringTriples"
    case PhysicalOp.Kind.GroupedInts => "GroupedInts"
    case PhysicalOp.Kind.GroupedStrings => "GroupedStrings"
    case PhysicalOp.Kind.GroupedLongs => "GroupedLongs"
    case PhysicalOp.Kind.LongOuter => "LongOuter"
    case PhysicalOp.Kind.StringOuter => "StringOuter"
    case PhysicalOp.Kind.CountSums => "CountSums"
    case PhysicalOp.Kind.LongCountSums => "LongCountSums"
    case PhysicalOp.Kind.StringCountSums => "StringCountSums"
    case PhysicalOp.Kind.IntLongSums => "IntLongSums"
    case PhysicalOp.Kind.GroupedIntLongs => "GroupedIntLongs"
    case PhysicalOp.Kind.IntLongTriples => "IntLongTriples"
    case PhysicalOp.Kind.IntLongOuter => "IntLongOuter"

  private def nodeName(plan: Plan[_]): String = plan match
    case product: Product => product.productPrefix
