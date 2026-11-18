package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*

import ColumnarInterpreter.Columns

/**
 * Collect of a triple [[PhysicalOp.Project]].
 *
 * Every triple project reports `[Int32, Int64]`. The function body is not read. The first fully
 * present row chooses the kernel, and that call is the row's only call. An `(Int, Long)` cell
 * publishes two columns through [[TripleToIntLong.project]]. A later cell that is not
 * `(Int, Long)` is a [[com.ewoodbury.scarlet.columnar.ColumnarFallback]] from that kernel. An
 * `(Int, (Int, Int))` cell stays on [[PairKernel.mapTriple]], which still copies a null component
 * and does not call the function on it. Kind stays triples.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Var",
  ),
)
private[execution] object TripleProject:

  private final case class Probe(batch: ColumnBatch, start: Int, result: Any)

  def columns(parts: Vector[ColumnBatch], mapper: Any): Columns =
    val fn = mapper.asInstanceOf[((Int, (Int, Int))) => Any]
    probe(parts, fn) match
      case Some(hit) if publishesLong(hit.result) => longColumns(parts, mapper, hit)
      case Some(hit) => tripleColumns(parts, fn, Some(hit))
      case None => tripleColumns(parts, fn, None)

  private def longColumns(parts: Vector[ColumnBatch], mapper: Any, hit: Probe): Columns =
    val plain = TripleToIntLong.adapt(mapper)
    Columns.Pairs(ColumnarInterpreter.onEachMorsel(parts) { morsel =>
      val kernel = if (same(morsel, hit)) then onceLong(hit.result, plain) else plain
      TripleToIntLong.project(morsel.batch, morsel.start, morsel.length, kernel)
    })

  private def tripleColumns(
      parts: Vector[ColumnBatch],
      fn: ((Int, (Int, Int))) => Any,
      hit: Option[Probe],
  ): Columns =
    val plain: PairKernel.IntTripleMap = (key, left, right) => fn((key, (left, right)))
    Columns.Triples(ColumnarInterpreter.onEachMorsel(parts) { morsel =>
      val kernel = hit match
        case Some(found) if same(morsel, found) => onceTriple(found.result, plain)
        case _ => plain
      PairKernel.mapTriple(morsel.batch, morsel.start, morsel.length, kernel)
    })

  /** First dense row. An empty range and a null component are left to [[PairKernel.mapTriple]]. */
  private def probe(parts: Vector[ColumnBatch], fn: ((Int, (Int, Int))) => Any): Option[Probe] =
    val batchSize = ColumnarPlanner.batchSize
    var found: Option[Probe] = None
    val batches = parts.iterator
    while (found.isEmpty && batches.hasNext) {
      val morsels = Morsels.of(batches.next(), batchSize).iterator
      while (found.isEmpty && morsels.hasNext) {
        val morsel = morsels.next()
        if (
            morsel.length > 0 &&
            ColumnHash.denseRange(morsel.batch, morsel.start, morsel.length, threeColumns)
          )
        then found = Some(sample(morsel, fn))
      }
    }
    found

  private def sample(morsel: Morsel, fn: ((Int, (Int, Int))) => Any): Probe =
    val row = morsel.start
    val key = ColumnHash.int32At(morsel.batch, 0).values(row)
    val left = ColumnHash.int32At(morsel.batch, 1).values(row)
    val right = ColumnHash.int32At(morsel.batch, 2).values(row)
    Probe(morsel.batch, morsel.start, fn((key, (left, right))))

  /** Ordinals 0, 1, and 2. [[ColumnHash.denseRange]] does not read a clear bit. */
  private val threeColumns: Int = 7

  private def same(morsel: Morsel, hit: Probe): Boolean =
    morsel.batch.eq(hit.batch) && morsel.start == hit.start

  private def publishesLong(value: Any): Boolean = value match
    case row: Product if row.productArity == 2 =>
      isInt(row.productElement(0)) && isLong(row.productElement(1))
    case _ => false

  private def isInt(cell: Any): Boolean = cell match
    case _: Int => true
    case _ => false

  private def isLong(cell: Any): Boolean = cell match
    case _: Long => true
    case _ => false

  /** One morsel, one thread. The first call is the probed row and is not run again. */
  private final class OnceLong(result: Any, rest: TripleToIntLong.TripleMap)
      extends TripleToIntLong.TripleMap:
    private var pending = true
    def apply(key: Int, left: Int, right: Int): Any =
      if (pending) then {
        pending = false
        result
      } else rest.apply(key, left, right)

  private def onceLong(result: Any, rest: TripleToIntLong.TripleMap): TripleToIntLong.TripleMap =
    new OnceLong(result, rest)

  /** One morsel, one thread. The first call is the probed row and is not run again. */
  private final class OnceTriple(result: Any, rest: PairKernel.IntTripleMap)
      extends PairKernel.IntTripleMap:
    private var pending = true
    def apply(key: Int, left: Int, right: Int): Any =
      if (pending) then {
        pending = false
        result
      } else rest.apply(key, left, right)

  private def onceTriple(result: Any, rest: PairKernel.IntTripleMap): PairKernel.IntTripleMap =
    new OnceTriple(result, rest)
