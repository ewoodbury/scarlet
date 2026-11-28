package com.ewoodbury.scarlet.columnar
import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.support.Check

/**
 * A wide scan exchanged through the existing spill store.
 *
 * Spill on writes each over-budget bucket and reads it back, including a clear validity bit. The
 * default configuration writes nothing. Float, float32, bool, and decimal columns use the same
 * store.
 */
class TestWideExchangeSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    WideExchange.resetForTest()

  "retained bytes" should "match the int and long exchange estimates" in {
    WideExchange.retainedBytes(0, longKey = false) shouldBe 0L
    WideExchange.retainedBytes(-1, longKey = true) shouldBe 0L
    WideExchange.retainedBytes(4, longKey = false) shouldBe HashJoin.retainedBuildBytes(4)
    WideExchange.retainedBytes(4, longKey = true) shouldBe LongHashJoin.retainedBuildBytes(4)
  }

  "the default config" should "exchange a wide scan and write no spill file" in {
    val defaults = ScarletConf()
    defaults.spillEnabled shouldBe false
    defaults.spillBudgetBytes shouldBe 64L * 1024L * 1024L
    val (fields, batches) = scan()
    withConf(defaults.spillBudgetBytes, enabled = false) { dir =>
      val exchanged = WideExchange.exchange(batches, 0, longKey = false, 2, 1, ScarletConf.get)
      rows(exchanged, fields) shouldBe expectedRows
      WideExchange.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a wide scan over a tiny budget" should "spill, read back, and keep a null bitmap" in {
    val (fields, batches) = scan()
    val memory = WideExchange.exchange(batches, 0, longKey = false, 2, 1, spillOff)
    withConf(1L, enabled = true) { dir =>
      val spilled = WideExchange.exchange(batches, 0, longKey = false, 2, 1, ScarletConf.get)
      rows(spilled, fields) shouldBe rows(memory, fields)
      rows(spilled, fields) shouldBe expectedRows
      bits(spilled) shouldBe bits(memory)
      WideExchange.filesWrittenForTest should be > 0L
      Path.of(WideExchange.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a bucket at the budget" should "stay in memory" in {
    val (fields, batches) = presentScan()
    val budget = WideExchange.retainedBytes(4, longKey = false)
    withConf(budget, enabled = true) { dir =>
      val exchanged = WideExchange.exchange(batches, 0, longKey = false, 1, 1, ScarletConf.get)
      rows(exchanged, fields) shouldBe presentRows
      WideExchange.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "one byte under the budget" should "spill the same rows" in {
    val (fields, batches) = presentScan()
    val budget = WideExchange.retainedBytes(4, longKey = false) - 1L
    withConf(budget, enabled = true) { dir =>
      val exchanged = WideExchange.exchange(batches, 0, longKey = false, 1, 1, ScarletConf.get)
      rows(exchanged, fields) shouldBe presentRows
      WideExchange.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a long key" should "use the long exchange estimate" in {
    val table = WideColumns(
      Vector(
        WideInput.longs("k", Array(1L, 2L)),
        WideInput.nullableInts("v", Array(0, 9), Array(true, false)),
      ),
    )
    val batches = Vector(WideBatch.encode(table))
    val budget = WideExchange.retainedBytes(2, longKey = true)
    withConf(budget, enabled = true) { dir =>
      val held = WideExchange.exchange(batches, 0, longKey = true, 1, 1, ScarletConf.get)
      rows(held, table.fields) shouldBe Vector(
        Vector(Cell.Int64(1L), Cell.Int32(0)),
        Vector(Cell.Int64(2L), Cell.Null),
      )
      WideExchange.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
    WideExchange.resetForTest()
    withConf(budget - 1L, enabled = true) { dir =>
      val spilled = WideExchange.exchange(batches, 0, longKey = true, 1, 1, ScarletConf.get)
      rows(spilled, table.fields) shouldBe Vector(
        Vector(Cell.Int64(1L), Cell.Int32(0)),
        Vector(Cell.Int64(2L), Cell.Null),
      )
      WideExchange.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a null key" should "be dropped and write nothing when every key is null" in {
    val table = WideColumns(
      Vector(
        WideInput.nullableInts("k", Array(1, 2), Array(false, false)),
        WideInput.longs("v", Array(10L, 20L)),
      ),
    )
    withConf(1L, enabled = true) { dir =>
      val exchanged =
        WideExchange.exchange(Vector(WideBatch.encode(table)), 0, false, 2, 1, ScarletConf.get)
      rows(exchanged, table.fields) shouldBe Vector.empty[Vector[Cell]]
      WideExchange.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a float payload" should "spill when the exchange is over budget" in {
    val table = WideColumns(
      Vector(
        WideInput.ints("k", Array(1, 2)),
        WideInput.nullableDoubles("v", Array(1.5, 0.0), Array(true, false)),
      ),
    )
    withConf(1L, enabled = true) { dir =>
      val exchanged =
        WideExchange.exchange(Vector(WideBatch.encode(table)), 0, false, 1, 1, ScarletConf.get)
      rows(exchanged, table.fields) shouldBe Vector(
        Vector(Cell.Int32(1), Cell.Float64(1.5)),
        Vector(Cell.Int32(2), Cell.Null),
      )
      WideExchange.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a float payload" should "stay in memory when spill is off" in {
    val table = WideColumns(
      Vector(
        WideInput.ints("k", Array(1, 2)),
        WideInput.doubles("v", Array(1.5, 2.5)),
      ),
    )
    withConf(1L, enabled = false) { dir =>
      val exchanged =
        WideExchange.exchange(Vector(WideBatch.encode(table)), 0, false, 1, 1, ScarletConf.get)
      rows(exchanged, table.fields) shouldBe Vector(
        Vector(Cell.Int32(1), Cell.Float64(1.5)),
        Vector(Cell.Int32(2), Cell.Float64(2.5)),
      )
      WideExchange.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a decimal payload" should "spill when the exchange is over budget" in {
    val table = WideColumns(
      Vector(
        WideInput.ints("k", Array(1, 2)),
        WideInput.nullableDecimals("v", 4, 2, Array(150L, 0L), Array(true, false)),
      ),
    )
    withConf(1L, enabled = true) { dir =>
      val exchanged =
        WideExchange.exchange(Vector(WideBatch.encode(table)), 0, false, 1, 1, ScarletConf.get)
      rows(exchanged, table.fields) shouldBe Vector(
        Vector(Cell.Int32(1), Cell.Decimal(4, 2, 150L)),
        Vector(Cell.Int32(2), Cell.Null),
      )
      WideExchange.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a decimal payload" should "stay in memory when spill is off" in {
    val table = WideColumns(
      Vector(
        WideInput.ints("k", Array(1, 2)),
        WideInput.decimals("v", 4, 2, Array(150L, 250L)),
      ),
    )
    withConf(1L, enabled = false) { dir =>
      val exchanged =
        WideExchange.exchange(Vector(WideBatch.encode(table)), 0, false, 1, 1, ScarletConf.get)
      rows(exchanged, table.fields) shouldBe Vector(
        Vector(Cell.Int32(1), Cell.Decimal(4, 2, 150L)),
        Vector(Cell.Int32(2), Cell.Decimal(4, 2, 250L)),
      )
      WideExchange.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a float32 and a bool payload" should "spill when the exchange is over budget" in {
    val table = WideColumns(
      Vector(
        WideInput.ints("k", Array(1, 2)),
        WideInput.nullableFloats("n", Array(1.25f, 0.0f), Array(true, false)),
        WideInput.bools("b", Array(true, false)),
      ),
    )
    withConf(1L, enabled = true) { dir =>
      val exchanged =
        WideExchange.exchange(Vector(WideBatch.encode(table)), 0, false, 1, 1, ScarletConf.get)
      rows(exchanged, table.fields) shouldBe Vector(
        Vector(Cell.Int32(1), Cell.Float32(1.25f), Cell.Bool(true)),
        Vector(Cell.Int32(2), Cell.Null, Cell.Bool(false)),
      )
      WideExchange.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a wide hash join" should "match the in-memory join when a side spills" in {
    val left = WideColumns(
      Vector(WideInput.ints("k", Array(1, 2)), WideInput.longs("v", Array(10L, 20L))),
    )
    val right = WideColumns(
      Vector(WideInput.ints("k", Array(2, 3)), WideInput.longs("p", Array(7L, 8L))),
    )
    val memory = join(left, right, enabled = false, budget = 1L)
    val spilled = join(left, right, enabled = true, budget = 1L)
    spilled shouldBe memory
    spilled.map(_.cells) shouldBe Vector(Vector(Cell.Int32(2), Cell.Int64(20L), Cell.Int64(7L)))
  }

  private def join(
      left: WideColumns,
      right: WideColumns,
      enabled: Boolean,
      budget: Long,
  ): Vector[WideRow] =
    withConf(budget, enabled) { dir =>
      val (fields, batch) = WideJoin.hash(
        Seq(WideBatch.encode(left)),
        Seq(WideBatch.encode(right)),
        left.fields,
        right.fields,
        0,
        0,
      )
      val written = WideExchange.filesWrittenForTest
      if (enabled) then written should be > 0L else written shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      WideBatch.decode(batch, fields)
    }

  /** Two batches. Key 2 is null. Value 0 on key 3 is null. Value 0 on key 4 is present. */
  private def scan(): (Vector[ColumnField], Vector[ColumnBatch]) =
    val first = WideColumns(
      Vector(
        WideInput.nullableInts("k", Array(1, 2, 3), Array(true, false, true)),
        WideInput.nullableLongs("v", Array(10L, 0L, 0L), Array(true, true, false)),
        WideInput.nullableInts("n", Array(7, 8, 9), Array(true, true, true)),
      ),
    )
    val second = WideColumns(
      Vector(
        WideInput.nullableInts("k", Array(4), Array(true)),
        WideInput.nullableLongs("v", Array(0L), Array(true)),
        WideInput.nullableInts("n", Array(0), Array(false)),
      ),
    )
    (first.fields, Vector(WideBatch.encode(first), WideBatch.encode(second)))

  private def expectedRows: Vector[Vector[Cell]] =
    Vector(
      Vector(Cell.Int32(1), Cell.Int64(10L), Cell.Int32(7)),
      Vector(Cell.Int32(3), Cell.Null, Cell.Int32(9)),
      Vector(Cell.Int32(4), Cell.Int64(0L), Cell.Null),
    )

  private def presentScan(): (Vector[ColumnField], Vector[ColumnBatch]) =
    val table = WideColumns(
      Vector(
        WideInput.ints("k", Array(1, 2, 3, 4)),
        WideInput.longs("v", Array(10L, 20L, 30L, 40L)),
      ),
    )
    (table.fields, Vector(WideBatch.encode(table)))

  private def presentRows: Vector[Vector[Cell]] =
    Vector(
      Vector(Cell.Int32(1), Cell.Int64(10L)),
      Vector(Cell.Int32(2), Cell.Int64(20L)),
      Vector(Cell.Int32(3), Cell.Int64(30L)),
      Vector(Cell.Int32(4), Cell.Int64(40L)),
    )

  private def spillOff: ScarletConf =
    originalConf.copy(spillEnabled = false, spillBudgetBytes = 1L)

  private def rows(
      batches: Vector[ColumnBatch],
      fields: Vector[ColumnField],
  ): Vector[Vector[Cell]] =
    batches.flatMap(batch => WideBatch.decode(batch, fields)).map(_.cells).sortBy(order)

  private def order(cells: Vector[Cell]): Long = Check.first(cells) match
    case Cell.Int32(value) => value.toLong
    case Cell.Int64(value) => value
    case other => throw new IllegalArgumentException(s"key cell $other")

  /** Validity of every column, in batch order. A clear bit is the null bitmap. */
  private def bits(batches: Vector[ColumnBatch]): Seq[Seq[Seq[Boolean]]] =
    batches.map { batch =>
      batch.columns.map { column =>
        (0 until batch.length).map(column.isValid)
      }
    }

  private def withConf[A](budget: Long, enabled: Boolean)(body: Path => A): A =
    val dir = Files.createTempDirectory("scarlet-wide-spill")
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = enabled,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        defaultShufflePartitions = 2,
      ),
    )
    WideExchange.resetForTest()
    try body(dir)
    finally
      ScarletConf.set(originalConf)
      WideExchange.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
