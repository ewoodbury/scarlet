package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * Decode boundary for one map directly above `groupByKey`.
 *
 * The child is a grouped int, string, or long batch. [[ColumnarInterpreter.decodeFor]] turns that
 * batch into `(K, Iterable[V])` once, and the logical function runs on those rows. The function
 * stays on [[Plan.MapOp]] and is not inspected. This is not a columnar map. `values` and `take` do
 * not use this node. Int `keys` stays the distinct-key scan. String and long `keys` stay on the
 * row path. A second map, or a filter above the map, stays on the row path.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] object GroupedDecode:

  /**
   * `GroupedDecode` around a lowered group. Any other base, including a group that does not lower,
   * stays on the row path. `lowerNode` runs only for a [[Plan.GroupByKeyOp]].
   */
  def lower(
      base: Plan[_],
      lowerNode: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] = base match
    case _: Plan.GroupByKeyOp[?, ?] =>
      lowerNode(base).collect { case group: PhysicalOp.GroupByKey =>
        PhysicalOp.GroupedDecode(group)
      }
    case _ => None

  /**
   * One group execution. `child` is the lowered [[PhysicalOp.GroupByKey]]. The decoded value side
   * is the grouped batch's `Iterable[V]` view.
   */
  def partitions(plan: Plan[_], child: PhysicalOp): Seq[Partition[Any]] = plan match
    case Plan.MapOp(source, mapper) =>
      val applyRow = function(mapper)
      val decoded = ColumnarInterpreter.decodeFor(
        ColumnarInterpreter.interpret(source, child),
        child,
      )
      decoded.map { part =>
        Partition(applyRows(rowsOf(part), applyRow))
      }
    case _ =>
      throw new IllegalStateException("grouped decode is a map directly above groupByKey")

  /** Row count after the same single decode. A map does not change the number of groups. */
  def count(plan: Plan[_], child: PhysicalOp): Long =
    partitions(plan, child).foldLeft(0L)((total, part) => total + part.data.size.toLong)

  /**
   * Named erasure boundary. The logical function is `I => A` where `I` is the decoded group row.
   * `Function1` erases that argument, so the cast is the call site and does not inspect `mapper`.
   */
  private def function(mapper: Any): Any => Any =
    mapper.asInstanceOf[Any => Any]

  /** Named erasure boundary. [[ColumnarInterpreter.decodeFor]] returns `Partition[_]`. */
  private def rowsOf(part: Partition[_]): Iterable[Any] =
    part.data.asInstanceOf[Iterable[Any]]

  private def applyRows(rows: Iterable[Any], applyRow: Any => Any): Vector[Any] =
    rows.iterator.map(applyRow).toVector
