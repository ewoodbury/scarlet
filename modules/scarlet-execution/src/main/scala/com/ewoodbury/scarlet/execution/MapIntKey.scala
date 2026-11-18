package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import com.ewoodbury.scarlet.columnar.ColumnKernel

/**
 * Columnar `mapIntKey`.
 *
 * The plan node is the witness that the function is `Int => Int`. This does not inspect it. A null
 * key stays null, the stored payload stays `0`, and the function is not called.
 * [[ColumnKernel.projectInt32]] is that rule. The value column is copied through.
 */
private[execution] object MapIntKey:

  private val published = new AtomicInteger

  /** Successful [[columns]] calls since the last [[resetPublishedForTest]]. */
  private[execution] def publishedForTest: Int = published.get

  /** Clears [[publishedForTest]]. */
  private[execution] def resetPublishedForTest(): Unit =
    published.set(0)

  /**
   * True when `op` still carries a key map. A parent that would publish that null key as a pair
   * declines. `keys` directly above the map projects column 0 and is not this decline.
   * [[JoinProbeAggregate]] does not decline either: the exchange drops the key and the sum does
   * not publish it. One key or value filter immediately above the map is [[slotAbove]], not a
   * decline.
   */
  def inTree(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.MapKey => true
    case other => other.children.exists(inTree)

  /**
   * One key or value slot whose child is a single [[PhysicalOp.MapKey]]. A second slot, a row
   * filter, and a project are not this shape. The predicate is not read.
   */
  def slotAbove(child: PhysicalOp, slot: PhysicalOp.Slot): Boolean = (child, slot) match
    case (_: PhysicalOp.MapKey, PhysicalOp.Slot.Key) => true
    case (_: PhysicalOp.MapKey, PhysicalOp.Slot.Value) => true
    case _ => false

  /**
   * True when a filter already sits on a key map. Another key map above that filter would publish
   * the filtered key. Consecutive key maps, with no filter between them, are false.
   */
  def filtered(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Filter(child, _) => inTree(child) || filtered(child)
    case other => other.children.exists(filtered)

  /**
   * Collect keeps a null key or value. A key or value filter above the map does too: the filter
   * drops a null in its own column and leaves the other column's clear bit.
   */
  def keepsNull(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.MapKey => true
    case PhysicalOp.Filter(child, _) => inTree(child)
    case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def columns(input: ColumnarInterpreter.Columns, mapper: Any): ColumnarInterpreter.Columns =
    val fn = mapper.asInstanceOf[Int => Int]
    val projected = ColumnarInterpreter.Columns.Pairs(
      ColumnarInterpreter.onEachMorsel(ColumnarInterpreter.pairsOf(input)) { morsel =>
        ColumnKernel.projectInt32(
          morsel.batch,
          0,
          morsel.start,
          morsel.length,
          key => fn(key),
        )
      },
    )
    val _ = published.incrementAndGet()
    projected
