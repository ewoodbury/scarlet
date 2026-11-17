package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.ColumnKernel

/**
 * Columnar `mapIntKey`.
 *
 * The plan node is the witness that the function is `Int => Int`. This does not inspect it. A null
 * key stays null, the stored payload stays `0`, and the function is not called.
 * [[ColumnKernel.projectInt32]] is that rule. The value column is copied through.
 */
private[execution] object MapIntKey:

  /**
   * True when `op` still carries a key map. A parent that would publish that null key declines.
   */
  def inTree(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.MapKey => true
    case other => other.children.exists(inTree)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def columns(input: ColumnarInterpreter.Columns, mapper: Any): ColumnarInterpreter.Columns =
    val fn = mapper.asInstanceOf[Int => Int]
    ColumnarInterpreter.Columns.Pairs(
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
