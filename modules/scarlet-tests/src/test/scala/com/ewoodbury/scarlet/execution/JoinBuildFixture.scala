package com.ewoodbury.scarlet.execution
import java.lang.management.ManagementFactory

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.support.Check

/**
 * Types, open-addressed table, allocation probe, and data helpers for [[JoinBuildBench]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
private[execution] object JoinBuildFixture:

  private[execution] val schemaName = "join-build-bench-1"
  private[execution] val rows = 1000000
  private[execution] val groups = 1024
  private[execution] val joinRows = 10 * groups
  private[execution] val batchSize = 8192
  private[execution] val sourcePartitions = 2
  private[execution] val shuffleWidth = 4
  private[execution] val warmup = 5
  private[execution] val rounds = 40
  private[execution] val allocRounds = 5
  private[execution] val threadCounts = Vector(1, 4)
  private[execution] val lcgSeed = 0x12345678
  private[execution] val budget = 1L << 62
  private[execution] val stepBudget = 8192L
  private[execution] val forcedRows = Vector(100000, 1000000)

  private[execution] final class HarnessRefusal(message: String) extends RuntimeException(message)

  /** Insert visited more than `rows * stepBudget` slots. The phase is not timed. */
  private[execution] final class ProbeCapped(val steps: Long, val row: Int)
      extends RuntimeException(s"insert probe exceeded the step budget at row $row after $steps")

  private[execution] final case class Side(keys: Array[Int], values: Array[Int])

  private[execution] final case class Part(build: Side, probe: Side, matches: Int, probeRows: Int)

  private[execution] final case class Replay(keys: Array[Int], left: Array[Int], right: Array[Int])

  private[execution] final case class Prepared(
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

  private[execution] final case class Tally(
      rows: Int,
      steps: Long,
      payload: Long,
      occupied: Int,
      slots: Int,
  )

  private[execution] final case class Written(rows: Int, checksum: Long, batch: ColumnBatch)

  private[execution] final case class Reading(
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

  private[execution] enum WriteMode:
    case Count, Grown, Exact

  private[execution] def prepare(): Prepared =
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

  private[execution] def verify(data: Prepared): Unit =
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

  private[execution] def checkReplica(data: Prepared): Unit =
    val task = counted(data.joinSide, data.survivors)
    if (task.rows.toLong != data.outputRows || task.payload != data.rightPayload) then
      refuse(s"10240-build replica matches ${task.rows} payload ${task.payload}")
    val engine = counted(data.survivors, data.joinSide)
    if (engine.rows.toLong != data.outputRows || engine.payload != data.leftPayload) then
      refuse(s"survivor-build replica matches ${engine.rows} payload ${engine.payload}")

  private[execution] def counted(build: Side, probe: Side): Tally =
    val table = OpenTable.build(build, tableSize(build.keys.length), true, stepBudget * rows)
    table.insert()
    table.count(probe.keys, probe.values, 0, probe.keys.length)

  private[execution] final class OpenTable(
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

  private[execution] object OpenTable:
    def build(side: Side, slots: Int, powerOfTwo: Boolean, budgetSteps: Long): OpenTable =
      new OpenTable(
        java.util.Arrays.copyOf(side.keys, side.keys.length),
        java.util.Arrays.copyOf(side.values, side.values.length),
        slots,
        powerOfTwo,
        budgetSteps,
      )

  private[execution] def copyExact(replay: Replay): Written =
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

  private[execution] def copyGrown(replay: Replay, probeRows: Int): Written =
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

  private[execution] def record[A](
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

  private[execution] def sample[A](action: => A)(check: A => Unit): Double =
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

  private[execution] def sampleAlloc[A](action: => A)(check: A => Unit): Double =
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
  private[execution] def nearest(samples: Array[Double]): Double =
    java.util.Arrays.sort(samples)
    val count = samples.length
    val rank = (50 * count + 99) / 100
    samples(math.max(1, math.min(count, rank)) - 1)

  private[execution] def assertTableSize(): Unit =
    Vector(16, joinRows, 100000, 333333, rows).foreach { count =>
      val expected = tableSize(count)
      val bytes = HashJoin.retainedBuildBytes(count)
      val slots = (bytes - count.toLong * 13L - 256L) / 8L
      if (slots != expected.toLong) then
        refuse(s"tableSize($count)=$expected but retained bytes imply $slots slots")
    }

  private[execution] def tableSize(count: Int): Int =
    ColumnHash.tableSize(count)

  private[execution] def capacity(policy: String, count: Int): Int =
    policy match
      case "current" => tableSize(count)
      case "load75" => nextPow2(ceilDiv(count.toLong * 4L, 3L))
      case "exact64" => round64(count.toLong + 64L)
      case other => refuse(s"unknown policy $other")

  private[execution] def pow2(policy: String): Boolean =
    !Check.same(policy, "exact64")

  private[execution] def nextPow2(need: Long): Int =
    if (need <= 1L) then 1
    else
      val bits = 64 - java.lang.Long.numberOfLeadingZeros(need - 1L)
      if (bits >= 31) then refuse(s"capacity $need does not fit a positive int power of two")
      1 << bits

  private[execution] def ceilDiv(numerator: Long, denominator: Long): Long =
    (numerator + denominator - 1L) / denominator

  private[execution] def round64(need: Long): Int =
    val rounded = ((need + 63L) / 64L) * 64L
    if (rounded > Int.MaxValue.toLong) then refuse(s"capacity $rounded overflows")
    rounded.toInt

  private[execution] def expectedProbe(load: Double): Double =
    if (load >= 1.0) then Double.PositiveInfinity
    else 0.5 * (1.0 + 1.0 / (1.0 - load))

  private[execution] def occupancyOf(tallies: Vector[Tally]): Double =
    val slots = tallies.foldLeft(0L)((total, tally) => total + tally.slots.toLong)
    val occupied = tallies.foldLeft(0L)((total, tally) => total + tally.occupied.toLong)
    if (slots == 0L) then -1.0 else occupied.toDouble / slots.toDouble

  private[execution] def stepsOf(tallies: Vector[Tally], count: Int): Double =
    val steps = tallies.foldLeft(0L)((total, tally) => total + tally.steps)
    if (count == 0) then -1.0 else steps.toDouble / count.toDouble

  private[execution] def mix(key: Int): Int =
    ColumnHash.mix(key)

  private[execution] def doubled(cap: Int): Int =
    if (cap >= (1 << 30)) then refuse(s"output capacity $cap cannot double")
    cap << 1

  private[execution] def bitmap(cap: Int): Array[Long] =
    new Array[Long]((cap + 63) >>> 6)

  private[execution] def copyBits(words: Array[Long], cap: Int): Array[Long] =
    val needed = (cap + 63) >>> 6
    if (words.length == needed) then words else java.util.Arrays.copyOf(words, needed)

  private[execution] def setBit(words: Array[Long], row: Int): Unit =
    val index = row >>> 6
    words(index) = words(index) | (1L << (row & 63))

  private[execution] def foldBits(words: Array[Long]): Long =
    var index = 0
    var mixed = 0L
    while (index < words.length) {
      mixed ^= words(index)
      index += 1
    }
    mixed

  private[execution] def pairBatch(side: Side): ColumnBatch =
    val count = side.keys.length
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(side.keys, Validity.allValid(count), count),
        Int32Column.of(side.values, Validity.allValid(count), count),
      ),
      count,
    )

  private[execution] def triple(
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

  private[execution] def chunks(side: Side, pieces: Int): Vector[Side] =
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

  private[execution] def rangesOf(count: Int, pieces: Int): Vector[(Int, Int)] =
    Vector.tabulate(pieces) { piece =>
      val from = ((count.toLong * piece) / pieces).toInt
      val until = ((count.toLong * (piece + 1)) / pieces).toInt
      if (until <= from) then refuse(s"empty range $piece of $count")
      (from, until)
    }

  private[execution] def hashParts(build: Side, probe: Side): Vector[Part] =
    val builds = hashSplit(build)
    val probes = hashSplit(probe)
    Vector.tabulate(shuffleWidth) { part =>
      val matched = matchCount(Check.at(builds, part), Check.at(probes, part))
      if (
          Check.at(builds, part).keys.length <= 0 || Check
            .at(probes, part)
            .keys
            .length <= 0 || matched <= 0
        )
      then refuse(s"partition $part is empty")
      Part(
        Check.at(builds, part),
        Check.at(probes, part),
        matched,
        Check.at(probes, part).keys.length,
      )
    }

  private[execution] def hashSplit(side: Side): Vector[Side] =
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

  private[execution] def matchCount(build: Side, probe: Side): Int =
    val table = OpenTable.build(build, tableSize(build.keys.length), true, stepBudget * rows)
    table.insert()
    table.count(probe.keys, probe.values, 0, probe.keys.length).rows

  private[execution] def replay(part: Part): Replay =
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

  private[execution] def copyColumn(batch: ColumnBatch, ordinal: Int, into: Array[Int]): Unit =
    Check.at(batch.columns, ordinal) match
      case column: Int32Column =>
        if (column.length != into.length) then
          refuse(s"column $ordinal length ${column.length} != ${into.length}")
        System.arraycopy(column.values, 0, into, 0, into.length)
      case _ => refuse(s"column $ordinal is not int32")

  private[execution] def replayPayload(replays: Vector[Replay]): (Long, Long) =
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

  private[execution] def matchesOf(left: Side, right: Side): Long =
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

  private[execution] def compact(keys: Array[Int], values: Array[Int]): Side =
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

  private[execution] def forced(count: Int, unique: Boolean): Side =
    val keys = new Array[Int](count)
    val values = new Array[Int](count)
    var index = 0
    while (index < count) {
      keys(index) = if (unique) then index else index % groups
      values(index) = index % 100
      index += 1
    }
    Side(keys, values)

  private[execution] def lcgValues(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = lcgSeed
    while (index < count) {
      state = state * 1664525 + 1013904223
      generated(index) = ((state & 0x7fffffff) % 2000001) - 1000000
      index += 1
    }
    generated

  private[execution] def configure(saved: ScarletConf, threads: Int): Unit =
    ScarletConf.set(
      saved.copy(
        threadPoolSize = threads,
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shuffleWidth,
        spillEnabled = false,
      ),
    )

  private[execution] def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)

  private[execution] def startProbe(): Option[AllocProbe] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean, 20000L)) else None
    }

  private[execution] def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None
