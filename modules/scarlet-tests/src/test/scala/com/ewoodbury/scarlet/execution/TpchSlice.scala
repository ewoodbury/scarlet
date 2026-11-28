package com.ewoodbury.scarlet.execution
import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler
import com.ewoodbury.scarlet.support.Check

/**
 * TPC-H queries 6, 1, and 3 on generated rows. This is not a downloaded scale factor.
 *
 * Money is a long. A date is a long. An average is a sum divided by a count. Query 1 groups by the
 * return flag only. Query 3 multiplies the lineitem price by the discount, joins customer and
 * orders, and groups by the order key. Query 5 joins the local supplier and groups by the nation
 * name. [[note]] names every TPC-H query.
 */
object TpchSlice:

  val lineitemScale: String = "rows-4096"
  val ordersScale: String = "orders-256"
  val lineitemRows: Int = 4096
  val orderRows: Int = 256
  val partitions: Int = 2

  /** Rows whose ship, discount, and quantity pass. Price times discount. */
  val revenue: Long = 1035571L
  val revenueRows: Long = 344L

  /** Return flags 0, 1, and 2. Count, sum, min, and max of quantity. Ordered by the flag. */
  val pricingMix: Long = -2219730004622530266L
  val pricingGroups: Long = 3L

  /**
   * Even order keys from 0 to 126. Each count is 1 and each revenue is 30. The customer segment is
   * BUILDING. Revenue descending, then key.
   */
  val shippingMix: Long = -3443305142726056896L
  val shippingGroups: Long = 64L

  /**
   * Nations china and india. Each count is 1 and each revenue is 30. Count descending, then name.
   */
  val localMix: Long = 516785584372771424L
  val localGroups: Long = 2L

  val note: Vector[String] = Vector(
    "1 This file. One group column. The published query also groups by line status.",
    "2 Missing: subquery.",
    "3 This file. Joins customer and orders. One group column. The published query groups three columns.",
    "4 Missing: exists.",
    "5 This file. Joins supplier and nation. The published query also filters a date literal.",
    "6 This file.",
    "7 Missing: extract year.",
    "8 Missing: case.",
    "9 Missing: extract year.",
    "10 Missing: more than two group columns.",
    "11 Missing: subquery.",
    "12 Missing: a conditional count.",
    "13 Missing: outer join.",
    "14 Missing: string LIKE.",
    "15 Missing: subquery.",
    "16 Missing: subquery.",
    "17 Missing: subquery.",
    "18 Missing: subquery.",
    "19 Missing: an in-list.",
    "20 Missing: subquery.",
    "21 Missing: exists.",
    "22 Missing: subquery.",
  )

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(2)
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = 2,
          columnarBatchSize = 8192,
          defaultShufflePartitions = partitions,
          columnarExecution = true,
          spillEnabled = false,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val problems = failures()
      if problems.nonEmpty then
        System.err.println(s"tpch slice failed: ${problems.mkString("; ")}")
        sys.exit(1)
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  /** Columnar lowering, then the same checksum on the row path. Empty means the slice passed. */
  def failures(): List[String] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = true, spillEnabled = false))
      prove() ++ bothPaths()
    finally ScarletConf.set(saved)

  def revenuePlan(): DistCollection[WideRow] =
    lineitem()
      .filterColumn(0, ColumnCompare(CompareOp.Ge, ColumnLiteral.Int64(100L)))
      .filterColumn(0, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int64(200L)))
      .filterColumn(1, ColumnCompare(CompareOp.Ge, ColumnLiteral.Int64(5L)))
      .filterColumn(1, ColumnCompare(CompareOp.Le, ColumnLiteral.Int64(7L)))
      .filterColumn(2, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int64(24L)))
      .compute(ColumnExpr.Mul(3, 1), "revenue")

  def pricingPlan(): DistCollection[WideRow] =
    pricing()
      .filterColumn(0, ColumnCompare(CompareOp.Le, ColumnLiteral.Int64(3000L)))
      .groupColumns(1, 2)

  def shippingPlan(): DistCollection[WideRow] =
    customers()
      .hashJoinColumns(datedOrders(), 0, 1)
      .hashJoinColumns(pricedLines(), 2, 0)
      .groupColumns(2, 7)

  /**
   * Query 5 reduced to Asia. The supplier's nation must match the customer's nation. Revenue is
   * price times discount. The checker sorts by count, then by the nation name.
   */
  def localPlan(): DistCollection[WideRow] =
    val joined = regions()
      .hashJoinColumns(nations(), 0, 1)
      .hashJoinColumns(localCustomers(), 2, 1)
      .hashJoinColumns(localOrders(), 4, 1)
      .hashJoinColumns(localLines(), 5, 0)
      .hashJoinColumns(suppliers(), 7, 0)
    joined
      .compute(ColumnExpr.Compare(CompareOp.Eq, 2, 11), "sameNation")
      .filterColumn(12, ColumnCompare(CompareOp.Eq, ColumnLiteral.Bool(true)))
      .groupColumns(3, 10)

  def pricingChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(byKey).foldLeft(0L)(mixIntMeasure)

  def shippingChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(byRevenue).foldLeft(0L)(mixLongMeasure)

  def localChecksum(rows: Iterable[WideRow]): Long =
    rows.toVector.sortWith(byNation).foldLeft(0L)(mixNation)

  private def prove(): List[String] =
    val revenueShape =
      "WideCompute(WideFilter2(WideFilter1(WideFilter1(WideFilter0(WideFilter0(Scan))))))"
    List(
      shaped("query 6", revenuePlan().plan, revenueShape),
      summed("query 6", revenuePlan().plan, 4, revenue),
      counted("query 6", revenuePlan().plan, revenueRows),
      shaped("query 1", pricingPlan().plan, "WideGroup(WideFilter0(Scan))"),
      counted("query 1", pricingPlan().plan, pricingGroups),
      shaped(
        "query 3",
        shippingPlan().plan,
        "WideGroup(WideHashJoin(WideHashJoin(WideFilter1(Scan),WideFilter2(Scan)),WideCompute(WideFilter1(Scan))))",
      ),
      counted("query 3", shippingPlan().plan, shippingGroups),
      shaped(
        "query 5",
        localPlan().plan,
        "WideGroup(WideFilter12(WideCompute(WideHashJoin(WideHashJoin(WideHashJoin(WideHashJoin(WideHashJoin(WideFilter1(Scan),Scan),Scan),WideFilter2(Scan)),WideCompute(Scan)),Scan))))",
      ),
      counted("query 5", localPlan().plan, localGroups),
    ).flatten

  private def bothPaths(): List[String] =
    List(
      both("query 6", revenue)(revenuePlan().sumLongColumn(4)),
      both("query 1", pricingMix)(pricingChecksum(pricingPlan().collect())),
      both("query 3", shippingMix)(shippingChecksum(shippingPlan().collect())),
      both("query 5", localMix)(localChecksum(localPlan().collect())),
    ).flatten

  private def lineitem(): DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.longs("ship", Array.tabulate(lineitemRows)(index => 50L + (index % 200).toLong)),
        WideInput.longs("discount", Array.tabulate(lineitemRows)(index => (index % 11).toLong)),
        WideInput.longs("quantity", Array.tabulate(lineitemRows)(index => (index % 40).toLong)),
        WideInput.longs(
          "price",
          Array.tabulate(lineitemRows)(index => ((index * 17) % 1000).toLong),
        ),
      ),
      partitions,
    )

  private def pricing(): DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.longs("ship", Array.tabulate(lineitemRows)(index => index.toLong)),
        WideInput.ints("flag", Array.tabulate(lineitemRows)(index => index % 3)),
        WideInput.ints("qty", Array.tabulate(lineitemRows)(index => 1 + (index % 50))),
      ),
      partitions,
    )

  private def customers(): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("custkey", Array.tabulate(32)(index => index)),
          WideInput.strings(
            "segment",
            Array.tabulate(32)(index => if Check.same(index % 2, 0) then "BUILDING" else "AUTO"),
          ),
        ),
        partitions,
      )
      .filterColumn(1, ColumnCompare(CompareOp.Eq, ColumnLiteral.Utf8("BUILDING")))

  private def datedOrders(): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("orderkey", Array.tabulate(orderRows)(index => index)),
          WideInput.ints("custkey", Array.tabulate(orderRows)(index => index % 32)),
          WideInput.longs("orderdate", Array.tabulate(orderRows)(index => index.toLong)),
        ),
        partitions,
      )
      .filterColumn(2, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int64(128L)))

  private def pricedLines(): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("orderkey", Array.tabulate(orderRows)(index => index)),
          WideInput.longs(
            "shipdate",
            Array.tabulate(orderRows)(index => if index < 128 then 40L else 10L),
          ),
          WideInput.longs("price", Array.tabulate(orderRows)(_ => 10L)),
          WideInput.longs("discount", Array.tabulate(orderRows)(_ => 3L)),
        ),
        partitions,
      )
      .filterColumn(1, ColumnCompare(CompareOp.Gt, ColumnLiteral.Int64(32L)))
      .compute(ColumnExpr.Mul(2, 3), "revenue")

  private def regions(): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("regionkey", Array(0, 1)),
          WideInput.strings("name", Array("ASIA", "EUROPE")),
        ),
        partitions,
      )
      .filterColumn(1, ColumnCompare(CompareOp.Eq, ColumnLiteral.Utf8("ASIA")))

  private def nations(): DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.ints("nationkey", Array(0, 1, 2)),
        WideInput.ints("regionkey", Array(0, 0, 1)),
        WideInput.strings("name", Array("china", "india", "france")),
      ),
      partitions,
    )

  private def localCustomers(): DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.ints("custkey", Array(0, 1, 2)),
        WideInput.ints("nationkey", Array(0, 1, 2)),
      ),
      partitions,
    )

  private def localOrders(): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("orderkey", Array(0, 1, 2, 3)),
          WideInput.ints("custkey", Array(0, 1, 2, 0)),
          WideInput.ints("orderdate", Array(10, 10, 10, 50)),
        ),
        partitions,
      )
      .filterColumn(2, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int32(20)))

  private def localLines(): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("orderkey", Array(0, 0, 1, 1)),
          WideInput.ints("suppkey", Array(0, 1, 1, 0)),
          WideInput.ints("price", Array(10, 10, 10, 10)),
          WideInput.ints("discount", Array(3, 3, 3, 3)),
        ),
        partitions,
      )
      .compute(ColumnExpr.Mul(2, 3), "revenue")

  private def suppliers(): DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.ints("suppkey", Array(0, 1, 2)),
        WideInput.ints("nationkey", Array(0, 1, 2)),
      ),
      partitions,
    )

  private def both(name: String, expected: Long)(query: => Long): Option[String] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = query
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = query
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    if columnar != expected then Some(s"$name columnar $columnar expected $expected")
    else if rows != expected then Some(s"$name rows $rows expected $expected")
    else None

  private def counted(name: String, plan: Plan[WideRow], expected: Long): Option[String] =
    ColumnarPlanner.tryCount(plan) match
      case Some(value) if value == expected => None
      case Some(value) => Some(s"$name columnar count $value expected $expected")
      case None => Some(s"$name did not lower")

  private def summed(
      name: String,
      plan: Plan[WideRow],
      ordinal: Int,
      expected: Long,
  ): Option[String] =
    WideSum.longColumn(plan, ordinal) match
      case Some(value) if value == expected => None
      case Some(value) => Some(s"$name columnar sum $value expected $expected")
      case None => Some(s"$name sum did not lower")

  private def shaped(name: String, plan: Plan[WideRow], shape: String): Option[String] =
    ColumnarPlanner.lower(plan).map(_.shape) match
      case Some(found) if Check.same(found, shape) => None
      case Some(found) => Some(s"$name shape $found expected $shape")
      case None => Some(s"$name did not lower")

  private def byKey(left: WideRow, right: WideRow): Boolean = keyOf(left) < keyOf(right)

  private def byNation(left: WideRow, right: WideRow): Boolean =
    val leftCount = countOf(left)
    val rightCount = countOf(right)
    leftCount > rightCount || (leftCount == rightCount && textAt(left, 0).compareTo(
      textAt(right, 0),
    ) < 0)

  private def mixNation(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Utf8(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      finish(foldText(acc, key), count, sum, min.toLong, max.toLong)
    case other => refuse(other.size)

  private def byRevenue(left: WideRow, right: WideRow): Boolean =
    val leftSum = sumOf(left)
    val rightSum = sumOf(right)
    leftSum > rightSum || (leftSum == rightSum && keyOf(left) < keyOf(right))

  private def mixIntMeasure(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int32(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int32(min),
          Cell.Int32(max),
        ) =>
      ((((acc * 31L + key.toLong) * 31L + count) * 31L + sum) * 31L + min.toLong) * 31L +
        max.toLong
    case other =>
      refuse(other.size)

  private def mixLongMeasure(acc: Long, row: WideRow): Long = cells(row) match
    case Vector(
          Cell.Int32(key),
          Cell.Int64(count),
          Cell.Int64(sum),
          Cell.Int64(min),
          Cell.Int64(max),
        ) =>
      ((((acc * 31L + key.toLong) * 31L + count) * 31L + sum) * 31L + min) * 31L + max
    case other =>
      refuse(other.size)

  private def countOf(row: WideRow): Long = cells(row) match
    case Vector(_, Cell.Int64(count), _, _, _) => count
    case other => refuse(other.size)

  private def textAt(row: WideRow, ordinal: Int): String = Check.at(cells(row), ordinal) match
    case Cell.Utf8(value) => value
    case _ => refuse(cells(row).size)

  private def foldText(acc: Long, text: String): Long =
    text.foldLeft(acc)((hash, ch) => hash * 31L + ch.toLong)

  private def finish(acc: Long, count: Long, sum: Long, min: Long, max: Long): Long =
    (((acc * 31L + count) * 31L + sum) * 31L + min) * 31L + max

  private def keyOf(row: WideRow): Long = cells(row) match
    case Vector(Cell.Int32(key), _, _, _, _) => key.toLong
    case Vector(Cell.Int64(key), _, _, _, _) => key
    case other => refuse(other.size)

  private def sumOf(row: WideRow): Long = cells(row) match
    case Vector(_, _, Cell.Int64(sum), _, _) => sum
    case other => refuse(other.size)

  private def cells(row: WideRow): Vector[Cell] = row.cells

  private def refuse(width: Int): Nothing =
    throw new IllegalArgumentException(s"group row has $width cells")
