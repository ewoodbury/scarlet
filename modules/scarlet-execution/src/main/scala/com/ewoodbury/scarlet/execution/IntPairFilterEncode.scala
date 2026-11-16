package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntPairArrays, Partition, Plan}

/**
 * Copies an owned int-pair scan and applies its slot filters in that copy.
 *
 * [[JoinArms]] otherwise encodes every row, builds a survivor index, and gathers through the
 * index. An [[IntPairArrays]] partition has no null, so the copy writes only the rows the slots
 * keep, in source order, as an all-valid pair batch. The hash build then reads that key column. A
 * boxed partition, a missing column, or a step that is not an int slot stays on
 * [[FilterChain.selected]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object IntPairFilterEncode:

  /**
   * Dense survivor batches, one per source partition, or `None` when this encode does not apply.
   */
  def tryBatches(
      plan: Plan[_],
      op: PhysicalOp,
      ordinals: Set[Int],
  ): Option[Vector[ColumnBatch]] =
    if (!ordinals.contains(0) || !ordinals.contains(1)) then None
    else
      op.kind match
        case PhysicalOp.Kind.Pairs =>
          slotsOf(plan, op, ordinals) match
            case None => None
            case Some(slots) =>
              owned(plan, op).map { parts =>
                encode(parts, gate(slots))
              }
        case _ => None

  /**
   * Inner-join cardinality of two owned int-pair sides. A slot filter is applied while the keys
   * are counted. Values are read only by the predicate and are not copied. `None` when either side
   * is boxed, null, or not an int slot stack on an owned scan: the caller keeps the exchange.
   */
  def tryCount(
      leftPlan: Plan[_],
      leftOp: PhysicalOp,
      rightPlan: Plan[_],
      rightOp: PhysicalOp,
      ordinals: Set[Int],
  ): Option[Long] =
    (countSide(leftPlan, leftOp, ordinals), countSide(rightPlan, rightOp, ordinals)) match
      case (Some(build), Some(probe)) => Some(JoinKeyCount.multiply(build, probe))
      case _ => None

  private def countSide(
      plan: Plan[_],
      op: PhysicalOp,
      ordinals: Set[Int],
  ): Option[JoinKeyCount.Table] =
    if (!ordinals.contains(0) || !ordinals.contains(1)) then None
    else
      op.kind match
        case PhysicalOp.Kind.Pairs =>
          owned(plan, op).flatMap { parts =>
            classify(plan, op, ordinals).map(mode => accumulate(parts, mode))
          }
        case _ => None

  private def classify(plan: Plan[_], op: PhysicalOp, ordinals: Set[Int]): Option[Mode] =
    PredicatePlan.compile(plan, op) match
      case None =>
        (plan, op) match
          case (_: Plan.Source[_], PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) => Some(Mode.All)
          case _ => None
      case Some(compiled) =>
        PredicatePlan.selectionSteps(PredicatePlan.encoded(compiled, ordinals)) match
          case Some(steps) if steps.nonEmpty =>
            pack(steps).map(slots => Mode.Filtered(gate(slots)))
          case _ => None

  private def accumulate(parts: Vector[IntPairArrays], mode: Mode): JoinKeyCount.Table =
    val work = jobs(parts, ColumnarPlanner.batchSize)
    if (work.isEmpty) then JoinKeyCount.fresh()
    else
      val workers = ColumnarPlanner.parallelism
      val slots = math.min(math.max(workers, 1), work.length)
      val tables = Array.tabulate(slots)(_ => JoinKeyCount.fresh())
      val _ = MorselScheduler.mapByThread(work, workers) { (id, job) =>
        fill(job, mode, tables(id))
        0
      }
      JoinKeyCount.merge(tables.toVector)

  private def fill(job: Job, mode: Mode, table: JoinKeyCount.Table): Unit =
    mode match
      case Mode.All => fillAll(job, table)
      case Mode.Filtered(gate) =>
        gate match
          case fast: KeyThenValue =>
            fillKept(job, fast.keepKey, fast.keepValue, table)
          case other: Generic =>
            fillGenericKeys(job, other.slots, table)

  private def fillAll(job: Job, table: JoinKeyCount.Table): Unit =
    val keys = job.keys
    var row = job.start
    val end = job.end
    while (row < end) {
      table.addOne(keys(row))
      row += 1
    }

  private def fillKept(
      job: Job,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      table: JoinKeyCount.Table,
  ): Unit =
    val keys = job.keys
    val values = job.values
    var row = job.start
    val end = job.end
    while (row < end) {
      val key = keys(row)
      if (keepKey(key) && keepValue(values(row))) then table.addOne(key)
      row += 1
    }

  private def fillGenericKeys(
      job: Job,
      slots: Array[Slot],
      table: JoinKeyCount.Table,
  ): Unit =
    val keys = job.keys
    val values = job.values
    var row = job.start
    val end = job.end
    while (row < end) {
      val key = keys(row)
      if (passes(slots, key, values(row))) then table.addOne(key)
      row += 1
    }

  private enum Mode:
    case All
    case Filtered(gate: Gate)

  private def slotsOf(
      plan: Plan[_],
      op: PhysicalOp,
      ordinals: Set[Int],
  ): Option[Array[Slot]] =
    PredicatePlan.compile(plan, op) match
      case None => None
      case Some(compiled) =>
        PredicatePlan.selectionSteps(PredicatePlan.encoded(compiled, ordinals)) match
          case Some(steps) if steps.nonEmpty => pack(steps)
          case _ => None

  private def pack(steps: Vector[Selection.Step]): Option[Array[Slot]] =
    val ordered = steps.toArray
    val slots = new Array[Slot](ordered.length)
    var index = 0
    var valid = true
    while (valid && index < ordered.length) {
      ordered(index) match
        case Selection.Step.Int32(column, predicate) if column == 0 || column == 1 =>
          slots(index) = new Slot(column, callOf(predicate))
          index += 1
        case _ =>
          valid = false
    }
    if (valid) Some(slots) else None

  private def callOf(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        (value: Int) => fallback(value)

  private def owned(plan: Plan[_], op: PhysicalOp): Option[Vector[IntPairArrays]] =
    val (_, child) = PredicatePlan.childOf(plan, op)
    child match
      case PhysicalOp.Scan(PhysicalOp.Kind.Pairs, partitions) if partitions.nonEmpty =>
        takeOwned(partitions)
      case _ => None

  private def takeOwned(partitions: Seq[Partition[_]]): Option[Vector[IntPairArrays]] =
    val count = partitions.size
    val found = new Array[IntPairArrays](count)
    var index = 0
    var owned = true
    val iterator = partitions.iterator
    while (owned && iterator.hasNext) {
      iterator.next().data match
        case pairs: IntPairArrays =>
          found(index) = pairs
          index += 1
        case _ => owned = false
    }
    if (owned && index == count) Some(found.toVector) else None

  private def gate(slots: Array[Slot]): Gate =
    if (slots.length == 2 && slots(0).column == 0 && slots(1).column == 1) then
      new KeyThenValue(slots(0).keep, slots(1).keep)
    else new Generic(slots)

  private def encode(parts: Vector[IntPairArrays], gate: Gate): Vector[ColumnBatch] =
    val batchSize = ColumnarPlanner.batchSize
    val workers = ColumnarPlanner.parallelism
    val work = jobs(parts, batchSize)
    val chunks = MorselScheduler.map(work, workers)((_, job) => filter(job, gate))
    stitch(parts.length, chunks)

  private def jobs(parts: Vector[IntPairArrays], batchSize: Int): Vector[Job] =
    val sources = parts.toArray
    val found = new Array[Job](jobCount(sources, batchSize))
    var part = 0
    var index = 0
    while (part < sources.length) {
      val pairs = sources(part)
      val rows = pairs.keys.length
      if (rows == 0) then
        found(index) = new Job(part, 0, 0, pairs.keys, pairs.values)
        index += 1
      else
        var start = 0
        while (start < rows) {
          val end = math.min(start + batchSize, rows)
          found(index) = new Job(part, start, end, pairs.keys, pairs.values)
          index += 1
          start = end
        }
      part += 1
    }
    found.toVector

  private def jobCount(parts: Array[IntPairArrays], batchSize: Int): Int =
    var part = 0
    var count = 0
    while (part < parts.length) {
      val rows = parts(part).keys.length
      count += (if (rows == 0) then 1 else (rows + batchSize - 1) / batchSize)
      part += 1
    }
    count

  private def filter(job: Job, gate: Gate): Chunk =
    val width = job.end - job.start
    if (width == 0) then new Chunk(job.part, Array.empty[Int], Array.empty[Int], 0)
    else
      val keys = new Array[Int](width)
      val values = new Array[Int](width)
      val count = gate match
        case fast: KeyThenValue =>
          fillKeyValue(job, fast.keepKey, fast.keepValue, keys, values)
        case other: Generic =>
          fillGeneric(job, other.slots, keys, values)
      new Chunk(job.part, keys, values, count)

  private def fillKeyValue(
      job: Job,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      outKeys: Array[Int],
      outValues: Array[Int],
  ): Int =
    val keys = job.keys
    val values = job.values
    var row = job.start
    val end = job.end
    var count = 0
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (keepKey(key) && keepValue(value)) then
        outKeys(count) = key
        outValues(count) = value
        count += 1
      row += 1
    }
    count

  private def fillGeneric(
      job: Job,
      slots: Array[Slot],
      outKeys: Array[Int],
      outValues: Array[Int],
  ): Int =
    val keys = job.keys
    val values = job.values
    var row = job.start
    val end = job.end
    var count = 0
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (passes(slots, key, value)) then
        outKeys(count) = key
        outValues(count) = value
        count += 1
      row += 1
    }
    count

  private def passes(slots: Array[Slot], key: Int, value: Int): Boolean =
    var index = 0
    var valid = true
    while (valid && index < slots.length) {
      val slot = slots(index)
      val sample = if (slot.column == 0) key else value
      valid = slot.keep(sample)
      index += 1
    }
    valid

  private def stitch(partCount: Int, chunks: Vector[Chunk]): Vector[ColumnBatch] =
    val pieces = chunks.toArray
    val batches = new Array[ColumnBatch](partCount)
    var part = 0
    var index = 0
    while (part < partCount) {
      val start = index
      var total = 0
      while (index < pieces.length && pieces(index).part == part) {
        total += pieces(index).count
        index += 1
      }
      batches(part) = publish(pieces, start, index, total)
      part += 1
    }
    batches.toVector

  private def publish(chunks: Array[Chunk], from: Int, until: Int, total: Int): ColumnBatch =
    val single = until == from + 1 && chunks(from).count == chunks(from).keys.length
    val keys =
      if (single) then chunks(from).keys
      else
        val copied = new Array[Int](total)
        copyInto(chunks, from, until, copied, keys = true)
        copied
    val values =
      if (single) then chunks(from).values
      else
        val copied = new Array[Int](total)
        copyInto(chunks, from, until, copied, keys = false)
        copied
    pair(keys, values, total)

  private def copyInto(
      chunks: Array[Chunk],
      from: Int,
      until: Int,
      into: Array[Int],
      keys: Boolean,
  ): Unit =
    var cursor = 0
    var index = from
    while (index < until) {
      val chunk = chunks(index)
      val source = if (keys) then chunk.keys else chunk.values
      System.arraycopy(source, 0, into, cursor, chunk.count)
      cursor += chunk.count
      index += 1
    }

  private def pair(keys: Array[Int], values: Array[Int], rows: Int): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.allValid(rows), rows),
        Int32Column.of(values, Validity.allValid(rows), rows),
      ),
      rows,
    )

  private final class Job(
      val part: Int,
      val start: Int,
      val end: Int,
      val keys: Array[Int],
      val values: Array[Int],
  )

  private final class Chunk(
      val part: Int,
      val keys: Array[Int],
      val values: Array[Int],
      val count: Int,
  )

  private final class Slot(val column: Int, val keep: Int => Boolean)

  private sealed trait Gate

  private final class KeyThenValue(val keepKey: Int => Boolean, val keepValue: Int => Boolean)
      extends Gate

  private final class Generic(val slots: Array[Slot]) extends Gate
