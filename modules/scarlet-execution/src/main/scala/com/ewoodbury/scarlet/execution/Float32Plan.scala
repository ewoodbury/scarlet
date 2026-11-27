package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * One float32 filter or one `Float => Float` map.
 *
 * Two or more steps stay on the row path. [[ProjectBinding.elementKind]] stays false, so this
 * column does not enter the double pipeline.
 */
private[execution] object Float32Steps:

  def lower(child: PhysicalOp, steps: List[ColumnarPlanner.ElementStep]): Option[PhysicalOp] =
    steps match
      case List(ColumnarPlanner.ElementStep.Filter(_)) =>
        FilterBinding.fixed(child, PhysicalOp.Slot.Element)
      case List(ColumnarPlanner.ElementStep.Project(mapper))
          if FunctionWitness.declaredElement(child.kind, mapper) =>
        ProjectBinding.project(child)
      case _ =>
        None

/**
 * Projects a batch carried by [[ColumnarInterpreter.Columns.Floats]].
 *
 * A `Float32` schema runs the float32 kernel. Any other schema keeps the double kernel. The
 * declared function was checked before this cast.
 */
private[execution] object FloatProject:

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def run(parts: Vector[ColumnBatch], mapper: Any)(
      visit: Vector[ColumnBatch] => (Morsel => ColumnBatch) => Vector[ColumnBatch],
  ): ColumnarInterpreter.Columns =
    val float32 = parts.exists(_.schema.headOption.contains(LogicalType.Float32))
    if (float32) then
      val fn = mapper.asInstanceOf[Float => Float]
      ColumnarInterpreter.Columns.Floats(
        visit(parts)(morsel =>
          Float32Kernel.project(
            morsel.batch,
            morsel.start,
            morsel.length,
            value => fn(value),
          ),
        ),
      )
    else
      val fn = mapper.asInstanceOf[Double => Double]
      ColumnarInterpreter.Columns.Floats(visit(parts) { morsel =>
        ColumnKernel.projectFloat64(
          morsel.batch,
          0,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      })

/** Decodes a float32 batch carried by [[ColumnarInterpreter.Columns.Floats]] as `Float`. */
private[execution] object Float32Decode:

  def partitions(columns: ColumnarInterpreter.Columns, op: PhysicalOp): Seq[Partition[_]] =
    val preserve = deduped(op)
    batches(columns).map(batch => Partition(Float32Codec.decode(batch, batch.length, preserve)))

  def prefix(columns: ColumnarInterpreter.Columns, limit: Int, op: PhysicalOp): Seq[Any] =
    if (limit <= 0) then Seq.empty[Any]
    else
      val preserve = deduped(op)
      val parts = batches(columns)
      ColumnarPlanner.takeChunks(
        limit,
        parts.iterator.map { batch => (remaining: Int) =>
          Float32Codec.decode(batch, remaining, preserve)
        },
      )

  private def deduped(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.Dedupe => true
    case _ => false

  private def batches(columns: ColumnarInterpreter.Columns): Vector[ColumnBatch] =
    ColumnarInterpreter.batchesOf(columns)

/** Sum of a lowered float32 plan. Null slots are skipped. The total is a `Float`. */
private[execution] object Float32Sum:

  def of(plan: Plan[_], op: PhysicalOp): Float =
    Float32Kernel.sum(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)))
