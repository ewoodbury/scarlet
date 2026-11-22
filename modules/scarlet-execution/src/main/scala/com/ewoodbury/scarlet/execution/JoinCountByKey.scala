package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * `countByKey` of an int inner hash join, with no user map.
 *
 * Owned int-pair sides publish `build.stored(k) * probe.stored(k)` from the tables
 * [[IntPairFilterEncode.tryTables]] already fills. Those products are not summed. Any other pair
 * of sides offers the join key through [[JoinArms.projectCount]].
 * [[com.ewoodbury.scarlet.columnar.JoinWrite]] is not called. A filter above the join is a
 * different plan.
 */
private[execution] object JoinCountByKey:

  def columns(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    IntPairFilterEncode.tryTables(left, leftOp, right, rightOp, ordinals) match
      case Some((build, probe)) => Columns.Pairs(Vector(pairs(build, probe)))
      case None =>
        Columns.Pairs(
          JoinArms.projectCount(
            left,
            right,
            leftOp,
            rightOp,
            preserveAbsence,
            ordinals,
          ),
        )

  /** One `(key, count)` batch. An empty overlap publishes an empty pair batch. */
  private def pairs(build: JoinKeyCount.Table, probe: JoinKeyCount.Table): ColumnBatch =
    val (keys, counts) = JoinKeyCount.productPairs(build, probe)
    val rows = keys.length
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.allValid(rows), rows),
        Int32Column.of(counts, Validity.allValid(rows), rows),
      ),
      rows,
    )
