package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * Execute a triple map that publishes `(Int, Long)`, and a `reduceByKey` of long addition over
 * that map.
 *
 * The function is called once per present triple. Two primitive columns are written. Any other
 * result, or an absent triple component, is [[com.ewoodbury.scarlet.columnar.ColumnarFallback]]
 * before the batch is published. A [[PhysicalOp.HashJoin]] child is already partitioned by the
 * key, so the aggregate does not exchange again. Any other child is shuffled, then each bucket is
 * summed. Morsel partials fold through [[HashAggregate.mergeSumPartials]]. The join that produced
 * the triples still writes its match columns.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
private[execution] object IntLongPairs:

  def partitions(plan: Plan[_], op: PhysicalOp): Seq[Partition[Any]] =
    batches(plan, op).map(batch => Partition[Any](JoinProbeAggregate.decodeSums(batch)))

  def project(columns: ColumnarInterpreter.Columns, mapper: Any): Vector[ColumnBatch] =
    val fn = adapt(mapper)
    ColumnarInterpreter.onEachMorsel(ColumnarInterpreter.triplesOf(columns)) { morsel =>
      PairKernel.mapToIntLong(morsel.batch, morsel.start, morsel.length, fn)
    }

  /** Sum the projected pairs. `source` is the map under `reduceByKey`. */
  def aggregate(source: Plan[_], child: PhysicalOp): Vector[ColumnBatch] =
    val (project, shuffle) = child match
      case PhysicalOp.Exchange(inner) => (inner, true)
      case other => (other, false)
    val pairs = ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, project))
    val buckets = if (shuffle) then scatter(pairs) else pairs
    buckets.map(sumBatch)

  private def batches(plan: Plan[_], op: PhysicalOp): Vector[ColumnBatch] = op match
    case PhysicalOp.Project(_, out) if PhysicalOp.isIntLong(out) =>
      ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op))
    case PhysicalOp.HashAggregate(child) if PhysicalOp.isIntLong(child.schema) =>
      plan match
        case Plan.ReduceByKeyOp(source, _) => aggregate(source, child)
        case _ =>
          throw new IllegalStateException("int-long aggregate is a reduceByKey")
    case _ =>
      throw new IllegalStateException("int-long decode is a project or a hash aggregate")

  private def scatter(pairs: Vector[ColumnBatch]): Vector[ColumnBatch] =
    if (pairs.isEmpty) then pairs
    else
      HashExchange.partitionAll(
        pairs,
        0,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
      )

  private def sumBatch(batch: ColumnBatch): ColumnBatch =
    val slices = ColumnBatches.slice(batch, ColumnarPlanner.batchSize)
    val partials = MorselScheduler.map(slices, ColumnarPlanner.parallelism) { (_, slice) =>
      HashAggregate.sumInt64(slice, 0, 1)
    }
    HashAggregate.mergeSumPartials(partials, 0, 1)

  private def adapt(mapper: Any): PairKernel.IntTripleMap =
    val fn = mapper.asInstanceOf[((Int, (Int, Int))) => Any]
    (key, left, right) => fn((key, (left, right)))

  /** True when a hash join under filters and projects already partitioned this key. */
  def hashedJoin(op: PhysicalOp): Boolean = op match
    case PhysicalOp.HashJoin(_, _) => true
    case PhysicalOp.Project(child, _) => hashedJoin(child)
    case PhysicalOp.Filter(child, _) => hashedJoin(child)
    case _ => false
