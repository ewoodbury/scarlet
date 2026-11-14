package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.{Arrays, Locale}

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Phase split for long `filter-reducebykey`. Not a test, and not part of `make test`.
 *
 * The input matches [[CrossEngineBench]]: 1e6 long pairs, keys `row % 1024`, the same LCG values,
 * two source partitions, batch 8192, shuffle width 4, two filters (`key % 3 != 0`, `value >= 0`).
 * Spill stays on, as that bench does. No bucket exceeds the budget, so the reduce stays in memory.
 *
 * Each line is one phase. Arrays and owned partition slices already exist. The timer wraps that
 * phase only. Phases are not subtracted from each other or from the action. p50 is nearest-rank
 * `ceil(p / 100 * n)` over 40 rounds after 5 warmups. A [[CountingSink]] is installed around the
 * timed call the way [[CrossEngineBench]] installs it.
 *
 *   - `encode` is [[BatchCodec.copiedLongPairs]] on each owned partition, the scan copy.
 *   - `selection` is [[MorselScheduler.map]] of [[Selection.applySelected]] over source morsels.
 *   - `assemble` is the caller-side id copy in `FilterChain.assemble`.
 *   - `partial` is [[LongHashReduce.int64Selected]] on survivor spans. It does not merge.
 *   - `concat` is [[ColumnBatches.concat]] of those partials, one group per source batch.
 *   - `merge` is [[LongHashReduce.int64]] on each concatenated group.
 *   - `exchange` is [[HashExchange.partitionAllInt64]] of the merged groups.
 *   - `bucket` is [[LongHashReduce.int64]] on each exchanged bucket.
 *   - `decode` is [[BatchCodec.decodeLongPairs]] on the reduced buckets, the collect write.
 *   - `checksum` is the bench fold `key * 31 + value` over that decoded seq.
 *   - `kernel` is [[LongHashReduce.byKeySelected]], partials through buckets.
 *   - `action` is the real `reduceByKey.collect` checksum, including encode and decode.
 *   - `tree` is a pairwise merge of the same partials. It is not the product path.
 *
 * A survivor total, a merged row count, or a checksum that misses the oracle fails the process.
 *
 * {{{
 * sbt -J-Xmx2g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.ReducePhaseBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ReducePhaseBench:

  private val rows = 1000000
  private val groups = 1024
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val width = 4
  private val warmup = 5
  private val rounds = 40
  private val lcgSeed = 0x12345678

  private val keepKey: ColumnKernel.Int64Predicate = (key: Long) => key % 3L != 0L
  private val keepValue: ColumnKernel.Int64Predicate = (value: Long) => value >= 0L
  private val steps: List[Selection.Step] = List(
    Selection.Step.Int64(0, keepKey),
    Selection.Step.Int64(1, keepValue),
  )
  private val plus: LongHashReduce.LongBinOp = (left: Long, right: Long) => left + right

  private final class HarnessRefusal(message: String) extends RuntimeException(message)

  private final case class Owned(keys: Array[Long], values: Array[Long])

  private final case class Span(selected: Selected, from: Int, until: Int)

  private final case class Oracle(survivors: Long, distinct: Int, checksum: Long)

  private final case class Prepared(
      owned: Vector[Owned],
      batches: Vector[ColumnBatch],
      morselHits: Vector[Vector[Selected]],
      assembled: Vector[Selected],
      spans: Vector[Span],
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
      concatenated: Vector[ColumnBatch],
      merged: Vector[ColumnBatch],
      exchanged: Vector[ColumnBatch],
      reduced: Vector[ColumnBatch],
      decoded: Seq[(Long, Long)],
      action: () => Long,
      oracle: Oracle,
  )

  private final case class Reading(phase: String, threads: Int, p50Ms: Double)

  def main(args: Array[String]): Unit =
    val saved = ScarletConf.get
    var status = 0
    try
      configure(saved, threads = width)
      run(saved)
    catch case _: HarnessRefusal => status = 1
    finally ScarletConf.set(saved)
    if (status != 0) then sys.exit(status)

  private def run(saved: ScarletConf): Unit =
    val values = lcgValues(rows)
    val keys = Array.tabulate(rows)(row => (row % groups).toLong)
    val oracle = oracleOf(values)
    val owned = ownedPartitions(keys, values)
    val batches = encode(owned)
    val morsels = batches.flatMap(batch => Morsels.of(batch, batchSize))
    val morselHits = groupHits(batches, selectMorsels(morsels, width))
    val assembled = morselHits.zip(batches).map { (hits, batch) =>
      assemble(batch, hits)
    }
    acceptSelected(assembled, oracle.survivors)
    val spans = spansOf(assembled)
    val sizes = assembled.map(one => ColumnBatches.cuts(one.count, batchSize).length)
    val partials = partialOf(spans, width)
    val concatenated = concatGroups(sizes, partials)
    val merged = hashMerge(concatenated, width)
    acceptMerged(merged, oracle.distinct)
    val exchanged = exchange(merged, width)
    val reduced = buckets(exchanged, width)
    acceptReduced(reduced, oracle)
    val decoded = decode(reduced)
    if (checksumOf(decoded) != oracle.checksum) then refuse("setup checksum drifted")
    val prepared = Prepared(
      owned,
      batches,
      morselHits,
      assembled,
      spans,
      sizes,
      partials,
      concatenated,
      merged,
      exchanged,
      reduced,
      decoded,
      actionOf(keys, values),
      oracle,
    )
    preamble(oracle, morsels.length, spans.length, partials.length)
    val four = timeAll(saved, prepared, threads = width)
    val one = timeAll(saved, prepared, threads = 1)
    System.err.println(table(one ++ four))

  private def timeAll(saved: ScarletConf, prepared: Prepared, threads: Int): Vector[Reading] =
    Vector(
      time(saved, "encode", threads)(encode(prepared.owned)) { batches =>
        acceptBatches(batches, rows)
      },
      time(saved, "selection", threads) {
        selectMorsels(prepared.batches.flatMap(batch => Morsels.of(batch, batchSize)), threads)
      } { selected =>
        if (selected.length <= sourcePartitions) then refuse("selection returned too few morsels")
      },
      time(saved, "assemble", threads) {
        prepared.morselHits.zip(prepared.batches).map { (hits, batch) =>
          assemble(batch, hits)
        }
      } { selected =>
        acceptSelected(selected, prepared.oracle.survivors)
      },
      time(saved, "partial", threads)(partialOf(prepared.spans, threads)) { partials =>
        if (partials.length != prepared.partials.length) then
          refuse(s"partials ${partials.length} != ${prepared.partials.length}")
      },
      time(saved, "concat", threads)(concatGroups(prepared.sizes, prepared.partials)) { batches =>
        acceptRows(batches, rowCount(prepared.concatenated))
      },
      time(saved, "merge", threads)(hashMerge(prepared.concatenated, threads)) { batches =>
        acceptRows(batches, prepared.oracle.distinct.toLong * sourcePartitions.toLong)
      },
      time(saved, "exchange", threads)(exchange(prepared.merged, threads)) { batches =>
        if (batches.length != width) then refuse(s"exchange produced ${batches.length} buckets")
      },
      time(saved, "bucket", threads)(buckets(prepared.exchanged, threads)) { batches =>
        acceptRows(batches, prepared.oracle.distinct.toLong)
      },
      time(saved, "decode", threads)(decode(prepared.reduced)) { decoded =>
        if (decoded.length != prepared.oracle.distinct) then
          refuse(s"decoded ${decoded.length} keys, expected ${prepared.oracle.distinct}")
      },
      time(saved, "checksum", threads)(checksumOf(prepared.decoded)) { got =>
        if (got != prepared.oracle.checksum) then
          refuse(s"checksum $got != ${prepared.oracle.checksum}")
      },
      time(saved, "tree", threads)(treeMerge(prepared.sizes, prepared.partials, threads)) {
        batches =>
          acceptMerged(batches, prepared.oracle.distinct)
      },
      time(saved, "kernel", threads)(kernel(prepared.assembled, threads)) { batches =>
        val total = batches.foldLeft(0)((sum, batch) => sum + batch.length)
        if (total != prepared.oracle.distinct) then
          refuse(s"kernel reduced $total keys, expected ${prepared.oracle.distinct}")
      },
      time(saved, "action", threads)(prepared.action()) { got =>
        if (got != prepared.oracle.checksum) then
          refuse(s"action $got != ${prepared.oracle.checksum}")
      },
    )

  private def time[A](saved: ScarletConf, phase: String, threads: Int)(body: => A)(
      check: A => Unit,
  ): Reading =
    configure(saved, threads)
    System.err.println(s"sample $phase threads=$threads")
    val p50 = sample(body)(check)
    val reading = Reading(phase, threads, p50)
    println(line(reading))
    reading

  private def sample[A](body: => A)(check: A => Unit): Double =
    var round = 0
    while (round < warmup) do
      check(body)
      round += 1
    val samples = new Array[Double](rounds)
    round = 0
    while (round < rounds) do
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        val started = System.nanoTime()
        val produced = body
        val elapsed = (System.nanoTime() - started) / 1_000_000.0
        check(produced)
        samples(round) = elapsed
      finally ColumnMetrics.restore(previous)
      round += 1
    nearestP50(samples)

  private def encode(owned: Vector[Owned]): Vector[ColumnBatch] =
    owned.map(part => BatchCodec.copiedLongPairs(part.keys, part.values))

  private def selectMorsels(morsels: Vector[Morsel], threads: Int): Vector[Selected] =
    MorselScheduler.map(morsels, threads) { (_, morsel) =>
      Selection.applySelected(morsel.batch, morsel.start, morsel.length, steps)
    }

  private def groupHits(
      batches: Vector[ColumnBatch],
      flat: Vector[Selected],
  ): Vector[Vector[Selected]] =
    val counts = batches.map(batch => Morsels.of(batch, batchSize).length)
    val seed: (Vector[Selected], Vector[Vector[Selected]]) =
      (flat, Vector.empty[Vector[Selected]])
    val (left, groups) = counts.foldLeft(seed) { (state, count) =>
      val (rest, acc) = state
      val (group, tail) = rest.splitAt(count)
      (tail, acc :+ group)
    }
    if (left.nonEmpty) then refuse(s"${left.length} selection morsels were not grouped")
    groups

  private def assemble(batch: ColumnBatch, morsels: Vector[Selected]): Selected =
    val copied = new Array[Int](batch.length)
    val total = morsels.foldLeft(0) { (written, morsel) =>
      if (written + morsel.count > copied.length) then
        refuse(s"selection morsels exceed capacity ${copied.length}")
      System.arraycopy(morsel.rows, 0, copied, written, morsel.count)
      written + morsel.count
    }
    Selected(batch, copied, total)

  private def spansOf(selected: Vector[Selected]): Vector[Span] =
    selected.flatMap { one =>
      ColumnBatches.cuts(one.count, batchSize).map { case (start, length) =>
        Span(one, start, start + length)
      }
    }

  private def partialOf(spans: Vector[Span], threads: Int): Vector[ColumnBatch] =
    MorselScheduler.map(spans, threads) { (_, span) =>
      LongHashReduce.int64Selected(span.selected, span.from, span.until, 0, 1, plus)
    }

  private def concatGroups(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
  ): Vector[ColumnBatch] =
    groupsOf(sizes, partials).map { group =>
      if (group.length == 1) then group.headOption.getOrElse(refuse("partial group is empty"))
      else ColumnBatches.concat(group)
    }

  private def hashMerge(batches: Vector[ColumnBatch], threads: Int): Vector[ColumnBatch] =
    MorselScheduler.map(batches, threads) { (_, batch) =>
      LongHashReduce.int64(batch, keyOrdinal = 0, valueOrdinal = 1, plus)
    }

  private def exchange(batches: Vector[ColumnBatch], threads: Int): Vector[ColumnBatch] =
    HashExchange.partitionAllInt64(
      batches,
      keyOrdinal = 0,
      numPartitions = width,
      parallelism = threads,
    )

  private def buckets(batches: Vector[ColumnBatch], threads: Int): Vector[ColumnBatch] =
    MorselScheduler.map(batches, threads) { (_, batch) =>
      LongHashReduce.int64(batch, keyOrdinal = 0, valueOrdinal = 1, plus)
    }

  private def kernel(selected: Vector[Selected], threads: Int): Vector[ColumnBatch] =
    LongHashReduce.byKeySelected(selected, width, threads, batchSize, plus)

  private def decode(batches: Vector[ColumnBatch]): Seq[(Long, Long)] =
    batches.flatMap(batch => BatchCodec.decodeLongPairs(batch))

  private def checksumOf(rowsOut: Seq[(Long, Long)]): Long =
    rowsOut.foldLeft(0L)((total, row) => total + row._1 * 31L + row._2)

  private def treeMerge(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
      threads: Int,
  ): Vector[ColumnBatch] =
    var level = groupsOf(sizes, partials)
    while (level.exists(_.length > 1)) do level = treeLevel(level, threads)
    level.map(group => group.headOption.getOrElse(refuse("tree group is empty")))

  private def treeLevel(
      level: Vector[Vector[ColumnBatch]],
      threads: Int,
  ): Vector[Vector[ColumnBatch]] =
    val pairs = level.zipWithIndex.flatMap { (group, index) =>
      pair(group).map(piece => (index, piece))
    }
    val merged = MorselScheduler.map(pairs, threads) { (_, piece) =>
      val (index, group) = piece
      val batch = group match
        case Vector(only) => only
        case many =>
          LongHashReduce.int64(ColumnBatches.concat(many), keyOrdinal = 0, valueOrdinal = 1, plus)
      (index, batch)
    }
    val slots = Array.fill(level.length)(Vector.empty[ColumnBatch])
    merged.foreach { (index, batch) =>
      slots(index) = slots(index) :+ batch
    }
    slots.toVector

  private def pair(group: Vector[ColumnBatch]): Vector[Vector[ColumnBatch]] =
    val seed: (Vector[Vector[ColumnBatch]], Vector[ColumnBatch]) =
      (Vector.empty[Vector[ColumnBatch]], Vector.empty[ColumnBatch])
    val (paired, rest) = group.foldLeft(seed) { (state, batch) =>
      val (acc, held) = state
      if (held.isEmpty) then (acc, Vector(batch))
      else (acc :+ (held :+ batch), Vector.empty[ColumnBatch])
    }
    if (rest.isEmpty) then paired else paired :+ rest

  private def actionOf(keys: Array[Long], values: Array[Long]): () => Long =
    val data = DistCollection
      .ofLongPairs(keys, values, sourcePartitions)
      .filterKeys((key: Long) => key % 3L != 0L)
      .filterValues((value: Long) => value >= 0L)
    val reduced = data.reduceByKey[Long, Long](_ + _)
    () => {
      ScarletRuntime.get.shuffle.clear()
      reduced.collect().map { case (key, value) => key * 31L + value }.sum
    }

  private def groupsOf(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
  ): Vector[Vector[ColumnBatch]] =
    val seed: (Vector[ColumnBatch], Vector[Vector[ColumnBatch]]) =
      (partials, Vector.empty[Vector[ColumnBatch]])
    val (left, groups) = sizes.foldLeft(seed) { (state, size) =>
      val (rest, acc) = state
      val (group, tail) = rest.splitAt(size)
      if (group.length != size) then refuse(s"group has ${group.length} partials, expected $size")
      (tail, acc :+ group)
    }
    if (left.nonEmpty) then refuse(s"${left.length} partials were not grouped")
    groups

  private def ownedPartitions(keys: Array[Long], values: Array[Long]): Vector[Owned] =
    val base = rows / sourcePartitions
    val extra = rows % sourcePartitions
    Vector.tabulate(sourcePartitions) { index =>
      val start = index * base + math.min(index, extra)
      val end = start + base + (if (index < extra) then 1 else 0)
      Owned(Arrays.copyOfRange(keys, start, end), Arrays.copyOfRange(values, start, end))
    }

  private def oracleOf(values: Array[Long]): Oracle =
    val sums = new Array[Long](groups)
    val seen = new Array[Boolean](groups)
    var row = 0
    var survivors = 0L
    while (row < rows) do
      val key = row % groups
      val value = values(row)
      if (key % 3 != 0 && value >= 0L) then
        sums(key) = sums(key) + value
        seen(key) = true
        survivors += 1L
      row += 1
    var key = 0
    var distinct = 0
    var checksum = 0L
    while (key < groups) do
      if (seen(key)) then
        distinct += 1
        checksum += key.toLong * 31L + sums(key)
      key += 1
    if (survivors <= 0L || distinct <= 0) then refuse("oracle kept nothing")
    Oracle(survivors, distinct, checksum)

  private def lcgValues(count: Int): Array[Long] =
    val generated = new Array[Long](count)
    var index = 0
    var state = lcgSeed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = ((mixed % 2000001) - 1000000).toLong
      index += 1
    generated

  private def acceptRows(batches: Vector[ColumnBatch], expected: Long): Unit =
    val total = rowCount(batches)
    if (total != expected) then refuse(s"phase produced $total rows, expected $expected")

  private def rowCount(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((sum, batch) => sum + batch.length.toLong)

  private def acceptBatches(batches: Vector[ColumnBatch], expected: Int): Unit =
    val total = batches.foldLeft(0L)((sum, batch) => sum + batch.length.toLong)
    if (total != expected.toLong) then refuse(s"encoded $total rows, expected $expected")

  private def acceptSelected(selected: Vector[Selected], survivors: Long): Unit =
    val total = selected.foldLeft(0L)((sum, one) => sum + one.count.toLong)
    if (total != survivors) then refuse(s"selected $total rows, expected $survivors")

  private def acceptMerged(batches: Vector[ColumnBatch], distinct: Int): Unit =
    if (batches.length != sourcePartitions) then
      refuse(s"merge produced ${batches.length} batches, expected $sourcePartitions")
    batches.foreach { batch =>
      if (batch.length != distinct) then
        refuse(s"merged batch has ${batch.length} keys, expected $distinct")
    }

  private def acceptReduced(batches: Vector[ColumnBatch], oracle: Oracle): Unit =
    if (batches.length != width) then refuse(s"reduce produced ${batches.length} buckets")
    val total = batches.foldLeft(0)((sum, batch) => sum + batch.length)
    if (total != oracle.distinct) then refuse(s"reduced $total keys, expected ${oracle.distinct}")
    if (checksumOf(decode(batches)) != oracle.checksum) then refuse("reduced checksum drifted")

  /** 1-based rank `ceil(50 / 100 * n)`, no interpolation. Forty rounds use rank 20. */
  private def nearestP50(samples: Array[Double]): Double =
    Arrays.sort(samples)
    val count = samples.length
    val rank = (50 * count + 99) / 100
    val index = math.max(1, math.min(count, rank)) - 1
    samples(index)

  private def configure(saved: ScarletConf, threads: Int): Unit =
    ScarletConf.set(
      saved.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = width,
        spillEnabled = true,
        threadPoolSize = threads,
      ),
    )

  private def preamble(oracle: Oracle, morsels: Int, spans: Int, partials: Int): Unit =
    val runtime = ManagementFactory.getRuntimeMXBean
    System.err.println(
      s"reduce-phase rows=$rows survivors=${oracle.survivors} distinct=${oracle.distinct} " +
        s"morsels=$morsels spans=$spans partials=$partials batch=$batchSize width=$width " +
        s"warmup=$warmup rounds=$rounds spill=true p50=nearest-rank " +
        s"java=${runtime.getVmName} ${runtime.getVmVersion}",
    )

  private def table(readings: Vector[Reading]): String =
    val header = Vector(
      "",
      "| phase | threads | p50 ms |",
      "| --- | ---: | ---: |",
    )
    val body = readings.map { reading =>
      s"| ${reading.phase} | ${reading.threads} | ${millis(reading.p50Ms)} |"
    }
    (header ++ body).mkString("\n")

  private def line(reading: Reading): String =
    "{" +
      """"schema":"reduce-phase-1",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""rows":$rows,""" +
      s""""p50_ms":${millis(reading.p50Ms)}}"""

  private def millis(value: Double): String =
    String.format(Locale.ENGLISH, "%.3f", value)

  private def refuse(message: String): Nothing =
    System.err.println(message)
    throw new HarnessRefusal(message)
