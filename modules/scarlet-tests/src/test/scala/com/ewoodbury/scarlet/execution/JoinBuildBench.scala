package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale
import java.util.concurrent.locks.LockSupport

import scala.util.control.NonFatal

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Attributes `filter-join` and times table-size and output-buffer policies. Not a test, and not
 * part of `make test`. No product code changes.
 *
 * Cross-engine shape: 1e6 pairs, keys `row % 1024`, filters `key % 3 != 0` and `value >= 0`,
 * joined to 10240 rows (10 copies of `0 until 1024`). Spill is off. The build is under the 64 MiB
 * spill budget, so this is the in-memory kernel. Shuffle width stays 4.
 *
 * The engine builds the filtered left and probes the 10240-row right. `probe` builds that right
 * side and probes the filtered rows, writing nothing. `output-exact` replays the match list into
 * exact-capacity columns. `output-grown` starts at the right-hand probe length and doubles.
 * `policy-current` is `ColumnHash.tableSize` (2×, next power of two). `policy-load75` is
 * `ceil(rows / 0.75)` as a power of two. `policy-exact64` is `rows + 64` rounded to 64, probed
 * with `floorMod`. `bp-*` is engine build plus a probe that does not write. `joined-*` is that
 * build plus a probe that writes. Each phase is its own p50, not a difference of other rows.
 * Unique `exact64` at 1e6 rows is not timed: 64 empty slots.
 *
 * p50 is nearest-rank `ceil(50/100*n)` over 40 rounds after 5 warmups. `alloc_mb` is that rank
 * over 5 later rounds, in decimal megabytes. A worker that exits between probe reads is missing.
 *
 * {{{
 * sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.JoinBuildBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
object JoinBuildBench:

  private val schemaName = "join-build-bench-1"
  private val rows = 1000000
  private val groups = 1024
  private val joinRows = 10 * groups
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val shuffleWidth = 4
  private val warmup = 5
  private val rounds = 40
  private val allocRounds = 5
  private val threadCounts = Vector(1, 4)
  private val lcgSeed = 0x12345678
  private val budget = 1L << 62
  private val stepBudget = 8192L
  private val forcedRows = Vector(100000, 1000000)

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  /** Insert visited more than `rows * stepBudget` slots. The phase is not timed. */
  private final class ProbeCapped(val steps: Long, val row: Int)
      extends RuntimeException(s"insert probe exceeded the step budget at row $row after $steps")

  private final case class Side(keys: Array[Int], values: Array[Int])

  private final case class Part(build: Side, probe: Side, matches: Int, probeRows: Int)

  private final case class Replay(keys: Array[Int], left: Array[Int], right: Array[Int])

  private final case class Prepared(
      survivors: Side,
      joinSide: Side,
      parts: Vector[Part],
      replays: Vector[Replay],
      outputRows: Long,
      leftPayload: Long,
      rightPayload: Long,
      joined: DistCollection[(Int, (Int, Int))],
      leftBatch: ColumnBatch,
      rightBatch: ColumnBatch,
  )

  private final case class Tally(
      rows: Int,
      steps: Long,
      payload: Long,
      occupied: Int,
      slots: Int,
  )

  private final case class Written(rows: Int, checksum: Long, batch: ColumnBatch)

  private final case class Reading(
      phase: String,
      threads: Int,
      rows: Long,
      p50Ms: Double,
      allocMb: Double,
      slots: Long,
      load: Double,
      occupancy: Double,
      probeSteps: Double,
      keys: String,
      note: String,
  )

  private enum WriteMode:
    case Count, Grown, Exact

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(shuffleWidth)
    var status = 0
    try
      configure(savedConf, shuffleWidth)
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val _ = MorselScheduler.map(Vector(0, 1), shuffleWidth)((_, item) => item)
      run(savedConf)
    catch
      case refused: HarnessRefusal =>
        System.err.println(refused.getMessage)
        status = 1
      case NonFatal(thrown) =>
        thrown.printStackTrace(System.err)
        status = 1
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def run(saved: ScarletConf): Unit =
    assertTableSize()
    val data = prepare()
    verify(data)
    preamble(data)
    val readings = Vector.newBuilder[Reading]
    def keep(reading: Reading): Unit =
      println(line(reading))
      val _ = readings += reading
    threadCounts.foreach { threads =>
      keep(timeE2E(saved, data, threads))
      keep(timeKernel(saved, data, threads))
    }
    val buildSides = Vector(
      ("join", data.joinSide),
      ("survivors", data.survivors),
    ) ++ forcedRows.map(count => (s"forced-$count", forced(count, unique = false)))
    buildSides.foreach { (label, side) =>
      threadCounts.foreach(threads => keep(timeOpen(saved, label, side, threads)))
    }
    threadCounts.foreach { threads =>
      keep(timeProbe(saved, data, threads, engine = false))
      keep(timeProbe(saved, data, threads, engine = true))
      keep(timeOutput(saved, data, threads, grown = false))
      keep(timeOutput(saved, data, threads, grown = true))
    }
    policySides(data).foreach { (keys, side) =>
      Vector("current", "load75", "exact64").foreach { policy =>
        threadCounts.foreach { threads =>
          timePolicy(saved, keys, side, policy, threads).foreach(keep)
        }
      }
    }
    Vector("current", "load75", "exact64").foreach { policy =>
      threadCounts.foreach { threads =>
        keep(timeJoined(saved, data, threads, policy, WriteMode.Count))
        keep(timeJoined(saved, data, threads, policy, WriteMode.Grown))
      }
    }
    threadCounts.foreach { threads =>
      keep(timeJoined(saved, data, threads, "current", WriteMode.Exact))
    }
    System.err.println(table(readings.result()))

  private def policySides(data: Prepared): Vector[(String, Side)] =
    val dup = Vector(
      "dup" -> data.joinSide,
      "dup" -> data.survivors,
    ) ++ forcedRows.map(count => "dup" -> forced(count, unique = false))
    val uniqueSmall = Vector(joinRows, 100000).map { count =>
      "unique" -> forced(count, unique = true)
    }
    dup ++ uniqueSmall :+ ("unique" -> forced(rows, unique = true))

  private def prepare(): Prepared =
    val leftKeys = Array.tabulate(rows)(row => row % groups)
    val leftValues = lcgValues(rows)
    val survivors = compact(leftKeys, leftValues)
    val joinKeys = Array.tabulate(joinRows)(row => row % groups)
    val joinValues = Array.tabulate(joinRows)(row => row % 100)
    val joinSide = Side(joinKeys, joinValues)
    if (survivors.keys.length <= 0) then refuse("filter kept no rows")
    val outputRows = matchesOf(survivors, joinSide)
    if (outputRows < 3000000L || outputRows > 3600000L) then
      refuse(s"output rows $outputRows are outside 3.0e6 until 3.6e6")
    val parts = hashParts(survivors, joinSide)
    val replays = parts.map(part => replay(part))
    val replayRows = replays.foldLeft(0L)((total, one) => total + one.keys.length.toLong)
    if (replayRows != outputRows) then refuse(s"replay rows $replayRows != $outputRows")
    val (leftPayload, rightPayload) = replayPayload(replays)
    val filtered = DistCollection
      .ofIntPairs(leftKeys, leftValues, sourcePartitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val side = DistCollection.ofIntPairs(joinKeys, joinValues, sourcePartitions)
    Prepared(
      survivors,
      joinSide,
      parts,
      replays,
      outputRows,
      leftPayload,
      rightPayload,
      filtered.join(side),
      pairBatch(survivors),
      pairBatch(joinSide),
    )

  private def verify(data: Prepared): Unit =
    val forward = HashJoin.openInt32(
      Vector(HashJoin.JoinSide(data.rightBatch, 0, 1)),
      budget,
    )
    val forwardRows = forward.probe(HashJoin.JoinSide(data.leftBatch, 0, 1)).length
    if (forwardRows.toLong != data.outputRows) then
      refuse(s"10240-build probe wrote $forwardRows rows, expected ${data.outputRows}")
    val backward = HashJoin.openInt32(
      Vector(HashJoin.JoinSide(data.leftBatch, 0, 1)),
      budget,
    )
    val backwardRows = backward.probe(HashJoin.JoinSide(data.rightBatch, 0, 1)).length
    if (backwardRows.toLong != data.outputRows) then
      refuse(s"survivor-build probe wrote $backwardRows rows, expected ${data.outputRows}")
    checkReplica(data)
    val kernel = ColumnPipeline.innerJoin(
      Vector(data.leftBatch),
      Vector(data.rightBatch),
      shuffleWidth,
      shuffleWidth,
      batchSize,
    )
    if (ColumnReduce.count(kernel) != data.outputRows) then
      refuse(s"kernel wrote ${ColumnReduce.count(kernel)} rows, expected ${data.outputRows}")
    configure(ScarletConf.get, shuffleWidth)
    val counted = data.joined.count()
    if (counted != data.outputRows) then
      refuse(s"filter-join count $counted != ${data.outputRows}")

  private def checkReplica(data: Prepared): Unit =
    val task = counted(data.joinSide, data.survivors)
    if (task.rows.toLong != data.outputRows || task.payload != data.rightPayload) then
      refuse(s"10240-build replica matches ${task.rows} payload ${task.payload}")
    val engine = counted(data.survivors, data.joinSide)
    if (engine.rows.toLong != data.outputRows || engine.payload != data.leftPayload) then
      refuse(s"survivor-build replica matches ${engine.rows} payload ${engine.payload}")

  private def counted(build: Side, probe: Side): Tally =
    val table = OpenTable.build(build, tableSize(build.keys.length), true, stepBudget * rows)
    table.insert()
    table.count(probe.keys, probe.values, 0, probe.keys.length)

  private def timeE2E(saved: ScarletConf, data: Prepared, threads: Int): Reading =
    configure(saved, threads)
    record("e2e", threads, rows.toLong, "", -1L, -1.0, -1.0, -1.0) {
      ScarletRuntime.get.shuffle.clear()
      data.joined.count()
    } { counted =>
      if (counted != data.outputRows) then
        refuse(s"e2e count $counted != ${data.outputRows}")
    }

  private def timeKernel(saved: ScarletConf, data: Prepared, threads: Int): Reading =
    configure(saved, threads)
    record("kernel", threads, rows.toLong, "", -1L, -1.0, -1.0, -1.0) {
      ColumnPipeline.innerJoin(
        Vector(data.leftBatch),
        Vector(data.rightBatch),
        shuffleWidth,
        threads,
        batchSize,
      )
    } { batches =>
      val counted = ColumnReduce.count(batches)
      if (counted != data.outputRows) then refuse(s"kernel rows $counted != ${data.outputRows}")
      if (batches.length != shuffleWidth) then
        refuse(s"kernel returned ${batches.length} batches, expected $shuffleWidth")
    }

  private def timeOpen(saved: ScarletConf, label: String, side: Side, threads: Int): Reading =
    configure(saved, threads)
    val batches = chunks(side, threads).map(chunk => pairBatch(chunk))
    record(s"build-$label", threads, side.keys.length.toLong, "dup", -1L, -1.0, -1.0, -1.0) {
      MorselScheduler.map(batches, threads) { (_, batch) =>
        HashJoin.openInt32(Vector(HashJoin.JoinSide(batch, 0, 1)), budget)
      }
    } { tables =>
      val built = tables.foldLeft(0L)((total, table) => total + table.buildRows.toLong)
      if (built != side.keys.length.toLong) then
        refuse(s"build-$label rows $built != ${side.keys.length}")
    }

  private def timeProbe(
      saved: ScarletConf,
      data: Prepared,
      threads: Int,
      engine: Boolean,
  ): Reading =
    configure(saved, threads)
    val phase = if (engine) then "probe-engine" else "probe"
    val buildSide = if (engine) then data.survivors else data.joinSide
    val probeSide = if (engine) then data.joinSide else data.survivors
    val slots = tableSize(buildSide.keys.length)
    val table = OpenTable.build(buildSide, slots, powerOfTwo = true, stepBudget * rows)
    table.insert()
    val built = table.tally()
    val ranges = rangesOf(probeSide.keys.length, threads)
    val once = ranges.map { range =>
      table.count(probeSide.keys, probeSide.values, range._1, range._2)
    }
    acceptProbe(phase, data, engine, once)
    val walked = once.foldLeft(0L)((total, tally) => total + tally.steps)
    record(
      phase,
      threads,
      probeSide.keys.length.toLong,
      "dup",
      slots.toLong,
      buildSide.keys.length.toDouble / slots.toDouble,
      built.occupied.toDouble / slots.toDouble,
      walked.toDouble / probeSide.keys.length.toDouble,
    ) {
      MorselScheduler.map(ranges, threads) { (_, range) =>
        table.count(probeSide.keys, probeSide.values, range._1, range._2)
      }
    }(tallies => acceptProbe(phase, data, engine, tallies))

  private def timeOutput(
      saved: ScarletConf,
      data: Prepared,
      threads: Int,
      grown: Boolean,
  ): Reading =
    configure(saved, threads)
    val phase = if (grown) then "output-grown" else "output-exact"
    val work = data.replays.zip(data.parts)
    record(phase, threads, data.outputRows, "", -1L, -1.0, -1.0, -1.0) {
      MorselScheduler.map(work, threads) { (_, item) =>
        val (replay, part) = item
        if (grown) then copyGrown(replay, part.probeRows) else copyExact(replay)
      }
    } { written =>
      val counted = written.foldLeft(0L)((total, one) => total + one.rows.toLong)
      if (counted != data.outputRows) then refuse(s"$phase rows $counted != ${data.outputRows}")
    }

  private def timePolicy(
      saved: ScarletConf,
      keys: String,
      side: Side,
      policy: String,
      threads: Int,
  ): Option[Reading] =
    val counted = side.keys.length
    if (keys == "unique" && policy == "exact64" && counted >= rows) then
      val slots = capacity(policy, counted)
      val load = counted.toDouble / slots.toDouble
      val steps = expectedProbe(load)
      Some(
        Reading(
          s"policy-$policy",
          threads,
          counted.toLong,
          -1.0,
          -1.0,
          slots.toLong,
          load,
          load,
          steps,
          keys,
          "not timed: 64 empty slots at 1e6 unique keys",
        ),
      )
    else
      configure(saved, threads)
      val pieces = chunks(side, threads)
      try
        val pilot = pieces.map { piece =>
          val table = OpenTable.build(piece, capacity(policy, piece.keys.length), pow2(policy), cap)
          table.insert()
          table.tally()
        }
        acceptPolicy(policy, side, pilot)
        val slotTotal = pilot.foldLeft(0L)((total, tally) => total + tally.slots.toLong)
        Some(
          record(
            s"policy-$policy",
            threads,
            counted.toLong,
            keys,
            slotTotal,
            counted.toDouble / slotTotal.toDouble,
            occupancyOf(pilot),
            stepsOf(pilot, counted),
          ) {
            MorselScheduler.map(pieces, threads) { (_, piece) =>
              val table =
                OpenTable.build(piece, capacity(policy, piece.keys.length), pow2(policy), cap)
              table.insert()
              table.tally()
            }
          }(tallies => acceptPolicy(policy, side, tallies)),
        )
      catch
        case capped: ProbeCapped =>
          Some(
            Reading(
              s"policy-$policy",
              threads,
              counted.toLong,
              -1.0,
              -1.0,
              -1L,
              -1.0,
              -1.0,
              -1.0,
              keys,
              s"capped at row ${capped.row} after ${capped.steps} steps",
            ),
          )

  private def cap: Long = stepBudget

  private def timeJoined(
      saved: ScarletConf,
      data: Prepared,
      threads: Int,
      policy: String,
      mode: WriteMode,
  ): Reading =
    configure(saved, threads)
    val phase = mode match
      case WriteMode.Count => s"bp-$policy"
      case WriteMode.Grown => s"joined-$policy"
      case WriteMode.Exact => "joined-presized"
    record(phase, threads, rows.toLong, "dup", -1L, -1.0, -1.0, -1.0) {
      MorselScheduler.map(data.parts, threads) { (_, part) =>
        val slots = capacity(policy, part.build.keys.length)
        val table = OpenTable.build(part.build, slots, pow2(policy), stepBudget * rows)
        table.insert()
        mode match
          case WriteMode.Count =>
            Left(table.count(part.probe.keys, part.probe.values, 0, part.probe.keys.length))
          case WriteMode.Grown =>
            Right(table.write(part.probe.keys, part.probe.values, 0))
          case WriteMode.Exact =>
            Right(table.write(part.probe.keys, part.probe.values, part.matches))
      }
    } { results =>
      val matched = results.foldLeft(0L) { (total, result) =>
        result match
          case Left(tally) => total + tally.rows.toLong
          case Right(written) => total + written.rows.toLong
      }
      if (matched != data.outputRows) then refuse(s"$phase matches $matched != ${data.outputRows}")
    }

  private def acceptProbe(
      phase: String,
      data: Prepared,
      engine: Boolean,
      tallies: Vector[Tally],
  ): Unit =
    val matched = tallies.foldLeft(0L)((total, tally) => total + tally.rows.toLong)
    val payload = tallies.foldLeft(0L)((total, tally) => total + tally.payload)
    if (matched != data.outputRows) then refuse(s"$phase matches $matched != ${data.outputRows}")
    val expected = if (engine) then data.leftPayload else data.rightPayload
    if (payload != expected) then refuse(s"$phase payload $payload != $expected")

  private def acceptPolicy(policy: String, side: Side, tallies: Vector[Tally]): Unit =
    val built = tallies.foldLeft(0L)((total, tally) => total + tally.rows.toLong)
    if (built != side.keys.length.toLong) then
      refuse(s"policy-$policy rows $built != ${side.keys.length}")
    if (tallies.exists(_.occupied <= 0)) then refuse(s"policy-$policy occupied no slot")

  private final class OpenTable(
      val keys: Array[Int],
      val values: Array[Int],
      val slots: Int,
      powerOfTwo: Boolean,
      budgetSteps: Long,
  ):
    private val mask = slots - 1
    private val slotKey = new Array[Int](slots)
    private val head = new Array[Int](slots)
    private val next = new Array[Int](keys.length)
    private var steps = 0L
    private var occupied = 0

    java.util.Arrays.fill(head, -1)
    java.util.Arrays.fill(next, -1)

    def insert(): Unit =
      val limit = keys.length.toLong * budgetSteps
      var row = 0
      while (row < keys.length) {
        val key = keys(row)
        var slot = firstSlot(key)
        var placed = false
        while (!placed) {
          steps += 1
          if (steps > limit) then throw new ProbeCapped(steps, row)
          if (head(slot) == -1) then
            head(slot) = row
            slotKey(slot) = key
            occupied += 1
            placed = true
          else if (slotKey(slot) == key) then
            next(row) = head(slot)
            head(slot) = row
            placed = true
          else slot = advance(slot)
        }
        row += 1
      }

    def tally(): Tally = Tally(keys.length, steps, 0L, occupied, slots)

    def count(probeKeys: Array[Int], probeValues: Array[Int], from: Int, until: Int): Tally =
      var matched = 0
      var walked = 0L
      var payload = 0L
      var row = from
      while (row < until) {
        val key = probeKeys(row)
        var slot = firstSlot(key)
        var resolved = false
        while (!resolved) {
          walked += 1
          if (head(slot) == -1) then resolved = true
          else if (slotKey(slot) == key) then
            var buildRow = head(slot)
            while (buildRow != -1) {
              payload += values(buildRow).toLong
              matched += 1
              buildRow = next(buildRow)
            }
            resolved = true
          else slot = advance(slot)
        }
        row += 1
      }
      if (probeValues.length < until) then refuse("probe values are shorter than the probe range")
      Tally(matched, walked, payload, occupied, slots)

    def write(probeKeys: Array[Int], probeValues: Array[Int], exact: Int): Written =
      val known = exact > 0
      var cap = if (known) then exact else math.max(16, probeKeys.length)
      var outKeys = new Array[Int](cap)
      var outBuild = new Array[Int](cap)
      var outProbe = new Array[Int](cap)
      var keyBits = bitmap(cap)
      var buildBits = bitmap(cap)
      var probeBits = bitmap(cap)
      var filled = 0
      var checksum = 0L
      var row = 0
      while (row < probeKeys.length) {
        val key = probeKeys(row)
        var slot = firstSlot(key)
        var resolved = false
        while (!resolved) {
          if (head(slot) == -1) then resolved = true
          else if (slotKey(slot) == key) then
            var buildRow = head(slot)
            while (buildRow != -1) {
              if (filled == cap) then
                if (known) then refuse(s"presized output $exact overflowed")
                else
                  cap = doubled(cap)
                  outKeys = java.util.Arrays.copyOf(outKeys, cap)
                  outBuild = java.util.Arrays.copyOf(outBuild, cap)
                  outProbe = java.util.Arrays.copyOf(outProbe, cap)
                  keyBits = copyBits(keyBits, cap)
                  buildBits = copyBits(buildBits, cap)
                  probeBits = copyBits(probeBits, cap)
              outKeys(filled) = keys(buildRow)
              outBuild(filled) = values(buildRow)
              outProbe(filled) = probeValues(row)
              setBit(keyBits, filled)
              setBit(buildBits, filled)
              setBit(probeBits, filled)
              checksum += key.toLong
              filled += 1
              buildRow = next(buildRow)
            }
            resolved = true
          else slot = advance(slot)
        }
        row += 1
      }
      checksum ^= foldBits(keyBits) ^ foldBits(buildBits) ^ foldBits(probeBits)
      val batch = triple(outKeys, outBuild, outProbe, filled)
      Written(filled, checksum + batch.length.toLong, batch)

    private def firstSlot(key: Int): Int =
      val mixed = mix(key)
      if (powerOfTwo) then mixed & mask else Math.floorMod(mixed, slots)

    private def advance(slot: Int): Int =
      if (powerOfTwo) then (slot + 1) & mask else if (slot + 1 == slots) then 0 else slot + 1

  private object OpenTable:
    def build(side: Side, slots: Int, powerOfTwo: Boolean, budgetSteps: Long): OpenTable =
      new OpenTable(
        java.util.Arrays.copyOf(side.keys, side.keys.length),
        java.util.Arrays.copyOf(side.values, side.values.length),
        slots,
        powerOfTwo,
        budgetSteps,
      )

  private def copyExact(replay: Replay): Written =
    val count = replay.keys.length
    val outKeys = new Array[Int](count)
    val outLeft = new Array[Int](count)
    val outRight = new Array[Int](count)
    val keyBits = bitmap(count)
    val leftBits = bitmap(count)
    val rightBits = bitmap(count)
    var index = 0
    var checksum = 0L
    while (index < count) {
      outKeys(index) = replay.keys(index)
      outLeft(index) = replay.left(index)
      outRight(index) = replay.right(index)
      setBit(keyBits, index)
      setBit(leftBits, index)
      setBit(rightBits, index)
      checksum += replay.keys(index).toLong
      index += 1
    }
    checksum ^= foldBits(keyBits) ^ foldBits(leftBits) ^ foldBits(rightBits)
    val batch = triple(outKeys, outLeft, outRight, count)
    Written(count, checksum + batch.length.toLong, batch)

  private def copyGrown(replay: Replay, probeRows: Int): Written =
    val count = replay.keys.length
    var cap = math.max(16, probeRows)
    var outKeys = new Array[Int](cap)
    var outLeft = new Array[Int](cap)
    var outRight = new Array[Int](cap)
    var keyBits = bitmap(cap)
    var leftBits = bitmap(cap)
    var rightBits = bitmap(cap)
    var filled = 0
    var checksum = 0L
    while (filled < count) {
      if (filled == cap) then
        cap = doubled(cap)
        outKeys = java.util.Arrays.copyOf(outKeys, cap)
        outLeft = java.util.Arrays.copyOf(outLeft, cap)
        outRight = java.util.Arrays.copyOf(outRight, cap)
        keyBits = copyBits(keyBits, cap)
        leftBits = copyBits(leftBits, cap)
        rightBits = copyBits(rightBits, cap)
      outKeys(filled) = replay.keys(filled)
      outLeft(filled) = replay.left(filled)
      outRight(filled) = replay.right(filled)
      setBit(keyBits, filled)
      setBit(leftBits, filled)
      setBit(rightBits, filled)
      checksum += replay.keys(filled).toLong
      filled += 1
    }
    checksum ^= foldBits(keyBits) ^ foldBits(leftBits) ^ foldBits(rightBits)
    val batch = triple(outKeys, outLeft, outRight, filled)
    Written(filled, checksum + batch.length.toLong, batch)

  private def record[A](
      phase: String,
      threads: Int,
      rows: Long,
      keys: String,
      slots: Long,
      load: Double,
      occupancy: Double,
      probeSteps: Double,
  )(action: => A)(check: A => Unit): Reading =
    System.err.println(s"sample $phase threads=$threads rows=$rows keys=$keys")
    val p50 = sample(action)(check)
    val alloc = sampleAlloc(action)(check)
    Reading(phase, threads, rows, p50, alloc, slots, load, occupancy, probeSteps, keys, "")

  private def sample[A](action: => A)(check: A => Unit): Double =
    var round = 0
    while (round < warmup) {
      check(action)
      round += 1
    }
    val samples = new Array[Double](rounds)
    round = 0
    while (round < rounds) {
      val started = System.nanoTime()
      val produced = action
      val elapsed = (System.nanoTime() - started) / 1_000_000.0
      check(produced)
      samples(round) = elapsed
      round += 1
    }
    nearest(samples)

  private def sampleAlloc[A](action: => A)(check: A => Unit): Double =
    startProbe() match
      case None =>
        refuse("thread allocated-memory probe is unavailable")
      case Some(probe) =>
        try
          val samples = new Array[Double](allocRounds)
          var round = 0
          while (round < allocRounds) {
            val (produced, bytes) = probe.account(action)
            check(produced)
            samples(round) = bytes.toDouble / 1000000.0
            round += 1
          }
          nearest(samples)
        finally probe.stop()

  /** 1-based rank `ceil(50/100*n)`, no interpolation. Forty rounds use index 20. */
  private def nearest(samples: Array[Double]): Double =
    java.util.Arrays.sort(samples)
    val count = samples.length
    val rank = (50 * count + 99) / 100
    samples(math.max(1, math.min(count, rank)) - 1)

  private def assertTableSize(): Unit =
    Vector(16, joinRows, 100000, 333333, rows).foreach { count =>
      val expected = tableSize(count)
      val bytes = HashJoin.retainedBuildBytes(count)
      val slots = (bytes - count.toLong * 13L - 256L) / 8L
      if (slots != expected.toLong) then
        refuse(s"tableSize($count)=$expected but retained bytes imply $slots slots")
    }

  private def tableSize(count: Int): Int =
    val doubled =
      if (count > (1 << 29)) then 1 << 30
      else math.max(16, count << 1)
    if (doubled >= (1 << 30)) then 1 << 30 else nextPow2(doubled)

  private def capacity(policy: String, count: Int): Int =
    policy match
      case "current" => tableSize(count)
      case "load75" => nextPow2(ceilDiv(count.toLong * 4L, 3L))
      case "exact64" => round64(count.toLong + 64L)
      case other => refuse(s"unknown policy $other")

  private def pow2(policy: String): Boolean =
    policy != "exact64"

  private def nextPow2(need: Long): Int =
    if (need <= 1L) then 1
    else
      val bits = 64 - java.lang.Long.numberOfLeadingZeros(need - 1L)
      if (bits >= 31) then refuse(s"capacity $need does not fit a positive int power of two")
      1 << bits

  private def ceilDiv(numerator: Long, denominator: Long): Long =
    (numerator + denominator - 1L) / denominator

  private def round64(need: Long): Int =
    val rounded = ((need + 63L) / 64L) * 64L
    if (rounded > Int.MaxValue.toLong) then refuse(s"capacity $rounded overflows")
    rounded.toInt

  private def expectedProbe(load: Double): Double =
    if (load >= 1.0) then Double.PositiveInfinity
    else 0.5 * (1.0 + 1.0 / (1.0 - load))

  private def occupancyOf(tallies: Vector[Tally]): Double =
    val slots = tallies.foldLeft(0L)((total, tally) => total + tally.slots.toLong)
    val occupied = tallies.foldLeft(0L)((total, tally) => total + tally.occupied.toLong)
    if (slots == 0L) then -1.0 else occupied.toDouble / slots.toDouble

  private def stepsOf(tallies: Vector[Tally], count: Int): Double =
    val steps = tallies.foldLeft(0L)((total, tally) => total + tally.steps)
    if (count == 0) then -1.0 else steps.toDouble / count.toDouble

  private def mix(key: Int): Int =
    val first = (key ^ (key >>> 16)) * 0x7feb352d
    val second = (first ^ (first >>> 15)) * 0x846ca68b
    second ^ (second >>> 16)

  private def doubled(cap: Int): Int =
    if (cap >= (1 << 30)) then refuse(s"output capacity $cap cannot double")
    cap << 1

  private def bitmap(cap: Int): Array[Long] =
    new Array[Long]((cap + 63) >>> 6)

  private def copyBits(words: Array[Long], cap: Int): Array[Long] =
    val needed = (cap + 63) >>> 6
    if (words.length == needed) then words else java.util.Arrays.copyOf(words, needed)

  private def setBit(words: Array[Long], row: Int): Unit =
    val index = row >>> 6
    words(index) = words(index) | (1L << (row & 63))

  private def foldBits(words: Array[Long]): Long =
    var index = 0
    var mixed = 0L
    while (index < words.length) {
      mixed ^= words(index)
      index += 1
    }
    mixed

  private def pairBatch(side: Side): ColumnBatch =
    val count = side.keys.length
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(side.keys, Validity.allValid(count), count),
        Int32Column.of(side.values, Validity.allValid(count), count),
      ),
      count,
    )

  private def triple(
      keys: Array[Int],
      left: Array[Int],
      right: Array[Int],
      count: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.allValid(keys.length), count),
        Int32Column.of(left, Validity.allValid(left.length), count),
        Int32Column.of(right, Validity.allValid(right.length), count),
      ),
      count,
    )

  private def chunks(side: Side, pieces: Int): Vector[Side] =
    val count = side.keys.length
    Vector.tabulate(pieces) { piece =>
      val from = ((count.toLong * piece) / pieces).toInt
      val until = ((count.toLong * (piece + 1)) / pieces).toInt
      if (until <= from) then refuse(s"empty chunk $piece of $count rows")
      Side(
        java.util.Arrays.copyOfRange(side.keys, from, until),
        java.util.Arrays.copyOfRange(side.values, from, until),
      )
    }

  private def rangesOf(count: Int, pieces: Int): Vector[(Int, Int)] =
    Vector.tabulate(pieces) { piece =>
      val from = ((count.toLong * piece) / pieces).toInt
      val until = ((count.toLong * (piece + 1)) / pieces).toInt
      if (until <= from) then refuse(s"empty range $piece of $count")
      (from, until)
    }

  private def hashParts(build: Side, probe: Side): Vector[Part] =
    val builds = hashSplit(build)
    val probes = hashSplit(probe)
    Vector.tabulate(shuffleWidth) { part =>
      val matched = matchCount(builds(part), probes(part))
      if (builds(part).keys.length <= 0 || probes(part).keys.length <= 0 || matched <= 0) then
        refuse(s"partition $part is empty")
      Part(builds(part), probes(part), matched, probes(part).keys.length)
    }

  private def hashSplit(side: Side): Vector[Side] =
    val counts = new Array[Int](shuffleWidth)
    var index = 0
    while (index < side.keys.length) {
      counts(Math.floorMod(side.keys(index), shuffleWidth)) += 1
      index += 1
    }
    val keys = Array.tabulate(shuffleWidth)(part => new Array[Int](counts(part)))
    val values = Array.tabulate(shuffleWidth)(part => new Array[Int](counts(part)))
    val cursor = new Array[Int](shuffleWidth)
    index = 0
    while (index < side.keys.length) {
      val part = Math.floorMod(side.keys(index), shuffleWidth)
      val at = cursor(part)
      keys(part)(at) = side.keys(index)
      values(part)(at) = side.values(index)
      cursor(part) = at + 1
      index += 1
    }
    Vector.tabulate(shuffleWidth)(part => Side(keys(part), values(part)))

  private def matchCount(build: Side, probe: Side): Int =
    val table = OpenTable.build(build, tableSize(build.keys.length), true, stepBudget * rows)
    table.insert()
    table.count(probe.keys, probe.values, 0, probe.keys.length).rows

  private def replay(part: Part): Replay =
    val count = part.matches
    val keys = new Array[Int](count)
    val left = new Array[Int](count)
    val right = new Array[Int](count)
    val table = OpenTable.build(part.build, tableSize(part.build.keys.length), true, stepBudget)
    table.insert()
    val written = table.write(part.probe.keys, part.probe.values, count)
    if (written.rows != count) then refuse(s"replay wrote ${written.rows}, expected $count")
    val batch = written.batch
    copyColumn(batch, 0, keys)
    copyColumn(batch, 1, left)
    copyColumn(batch, 2, right)
    Replay(keys, left, right)

  private def copyColumn(batch: ColumnBatch, ordinal: Int, into: Array[Int]): Unit =
    batch.columns(ordinal) match
      case column: Int32Column =>
        if (column.length != into.length) then
          refuse(s"column $ordinal length ${column.length} != ${into.length}")
        System.arraycopy(column.values, 0, into, 0, into.length)
      case _ => refuse(s"column $ordinal is not int32")

  private def replayPayload(replays: Vector[Replay]): (Long, Long) =
    replays.foldLeft((0L, 0L)) { (total, one) =>
      var index = 0
      var left = total._1
      var right = total._2
      while (index < one.keys.length) {
        left += one.left(index).toLong
        right += one.right(index).toLong
        index += 1
      }
      (left, right)
    }

  private def matchesOf(left: Side, right: Side): Long =
    val copies = new Array[Int](groups)
    var index = 0
    while (index < right.keys.length) {
      val key = right.keys(index)
      if (key < 0 || key >= groups) then refuse(s"join key $key is outside 0 until $groups")
      copies(key) += 1
      index += 1
    }
    index = 0
    var matched = 0L
    while (index < left.keys.length) {
      val key = left.keys(index)
      if (key < 0 || key >= groups) then refuse(s"survivor key $key is outside 0 until $groups")
      val count = copies(key)
      if (count != 10) then refuse(s"key $key has $count join rows, expected 10")
      matched += count.toLong
      index += 1
    }
    matched

  private def compact(keys: Array[Int], values: Array[Int]): Side =
    var index = 0
    var kept = 0
    while (index < keys.length) {
      if (keys(index) % 3 != 0 && values(index) >= 0) then kept += 1
      index += 1
    }
    val outKeys = new Array[Int](kept)
    val outValues = new Array[Int](kept)
    index = 0
    var cursor = 0
    while (index < keys.length) {
      if (keys(index) % 3 != 0 && values(index) >= 0) then
        outKeys(cursor) = keys(index)
        outValues(cursor) = values(index)
        cursor += 1
      index += 1
    }
    if (cursor != kept) then refuse(s"compacted $cursor rows, counted $kept")
    Side(outKeys, outValues)

  private def forced(count: Int, unique: Boolean): Side =
    val keys = new Array[Int](count)
    val values = new Array[Int](count)
    var index = 0
    while (index < count) {
      keys(index) = if (unique) then index else index % groups
      values(index) = index % 100
      index += 1
    }
    Side(keys, values)

  private def lcgValues(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = lcgSeed
    while (index < count) {
      state = state * 1664525 + 1013904223
      generated(index) = ((state & 0x7fffffff) % 2000001) - 1000000
      index += 1
    }
    generated

  private def configure(saved: ScarletConf, threads: Int): Unit =
    ScarletConf.set(
      saved.copy(
        threadPoolSize = threads,
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shuffleWidth,
        spillEnabled = false,
      ),
    )

  private def preamble(data: Prepared): Unit =
    val runtime = ManagementFactory.getRuntimeMXBean
    System.err.println(
      s"join-build-bench rows=$rows survivors=${data.survivors.keys.length} " +
        s"join_rows=$joinRows output_rows=${data.outputRows} batch=$batchSize " +
        s"partitions=$sourcePartitions shuffle=$shuffleWidth warmup=$warmup rounds=$rounds " +
        s"alloc_rounds=$allocRounds threads=${threadCounts.mkString(",")} spill=false " +
        s"p50=nearest-rank java=${runtime.getVmName} ${runtime.getVmVersion}",
    )
    System.err.println(
      "engine builds the filtered left and probes the 10240-row right. " +
        "probe builds the 10240-row side and probes the filtered rows with no output write. " +
        "output-grown starts at that right-hand probe length and doubles.",
    )

  private def table(readings: Vector[Reading]): String =
    val header = Vector(
      "",
      "| phase | threads | rows | keys | p50 ms | alloc mb | slots | load | occ | probe |",
      "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    )
    val body = readings.map { reading =>
      s"| ${reading.phase} | ${reading.threads} | ${reading.rows} | ${reading.keys} | " +
        s"${fmt(reading.p50Ms)} | ${fmt(reading.allocMb)} | ${reading.slots} | " +
        s"${fmt4(reading.load)} | ${fmt4(reading.occupancy)} | ${fmt4(reading.probeSteps)} |"
    }
    (header ++ body).mkString("\n")

  private def line(reading: Reading): String =
    val note =
      if (reading.note.isEmpty) then ""
      else s""","note":"${reading.note}""""
    val keys =
      if (reading.keys.isEmpty) then ""
      else s""","keys":"${reading.keys}""""
    "{" +
      s""""schema":"$schemaName",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""rows":${reading.rows}""" +
      keys +
      s""","p50_ms":${fmt(reading.p50Ms)},""" +
      s""""alloc_mb":${fmt(reading.allocMb)},""" +
      s""""slots":${reading.slots},""" +
      s""""load":${fmt4(reading.load)},""" +
      s""""occupancy":${fmt4(reading.occupancy)},""" +
      s""""probe_steps":${fmt4(reading.probeSteps)},""" +
      s""""warmup":$warmup,""" +
      s""""rounds":$rounds""" +
      note +
      "}"

  private def fmt(value: Double): String =
    if (value < 0.0) then "-1" else String.format(Locale.ENGLISH, "%.3f", value)

  private def fmt4(value: Double): String =
    if (value < 0.0) then "-1" else String.format(Locale.ENGLISH, "%.4f", value)

  private def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)

  private def startProbe(): Option[AllocProbe] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
    }

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  /**
   * Sums bytes on every live thread except the probe. Workers have often exited when the action
   * returns, so a caller-only delta misses them. The watch reads while the action runs.
   */
  private final class AllocProbe(bean: ThreadMXBean):
    private val capacity = 1024
    private val ids = new Array[Long](capacity)
    private val baseline = new Array[Long](capacity)
    private val high = new Array[Long](capacity)
    private var count = 0
    @volatile private var running = true
    @volatile private var probeId = 0L
    private val thread = new Thread(() => watch(), "scarlet-alloc-probe")

    thread.setDaemon(true)
    thread.start()
    while (probeId == 0L) do LockSupport.parkNanos(10000L)

    def stop(): Unit =
      running = false
      LockSupport.unpark(thread)
      thread.join(1000L)
      ()

    def account[A](body: => A): (A, Long) =
      open()
      try
        val value = body
        (value, close())
      catch
        case thrown: Throwable =>
          closeQuietly()
          throw thrown

    private def watch(): Unit =
      probeId = Thread.currentThread().threadId()
      while (running) {
        observe(setBaseline = false)
        LockSupport.parkNanos(20000L)
      }

    private def open(): Unit = this.synchronized {
      count = 0
      observe(setBaseline = true)
    }

    private def close(): Long = this.synchronized {
      observe(setBaseline = false)
      var index = 0
      var total = 0L
      while (index < count) {
        if (ids(index) != probeId) then
          val base = if (baseline(index) < 0L) then 0L else baseline(index)
          val delta = high(index) - base
          if (delta > 0L) then total += delta
        index += 1
      }
      total
    }

    private def closeQuietly(): Unit =
      try
        val _ = close()
      catch case _: IllegalStateException => ()

    private def observe(setBaseline: Boolean): Unit = this.synchronized {
      val live = bean.getAllThreadIds
      var index = 0
      while (index < live.length) {
        val id = live(index)
        val bytes = bean.getThreadAllocatedBytes(id)
        if (bytes >= 0L) then note(id, bytes, setBaseline)
        index += 1
      }
    }

    private def note(id: Long, bytes: Long, setBaseline: Boolean): Unit =
      val at = slot(id)
      if (bytes > high(at)) then high(at) = bytes
      if (setBaseline) then baseline(at) = bytes

    private def slot(id: Long): Int =
      val existing = indexOf(id)
      if (existing >= 0) then existing
      else if (count >= capacity) then
        throw new IllegalStateException(s"alloc probe filled $capacity thread slots")
      else
        val placed = count
        ids(placed) = id
        baseline(placed) = -1L
        high(placed) = 0L
        count = placed + 1
        placed

    private def indexOf(id: Long): Int =
      var index = 0
      var found = -1
      while (index < count && found < 0) {
        if (ids(index) == id) then found = index
        index += 1
      }
      found
