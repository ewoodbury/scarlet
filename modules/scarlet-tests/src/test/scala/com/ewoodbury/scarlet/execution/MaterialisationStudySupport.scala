package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale
import java.util.concurrent.locks.LockSupport

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}

/**
 * Timing, percentile, allocation estimate, and raw floors for [[MaterialisationStudy]].
 *
 * The sampler stays parked during timed samples. Allocation is a later pass. Nearest-rank
 * percentiles use `ceil(p / 100 * n)` with no interpolation. `measure` is large enough that p95
 * is not the maximum or the second-largest sample.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object MaterialisationStudySupport:

  val warmup: Int = 5
  val measure: Int = 41
  val allocSamples: Int = 5
  val batchSize: Int = 8192
  val sourcePartitions: Int = 2
  val shuffleWidth: Int = 4
  val elementRows: Int = 1000000
  val groupedRows: Int = 10000000
  val groups: Int = 1024

  /** Fixed seed for the shuffled grouped keys. The multiset matches `row % groups`. */
  val permutedSeed: Long = 58L
  val threadCounts: Vector[Int] = Vector(1, 4)
  val estimatedKind: String = "process-wide-estimate"
  val callerKind: String = "caller-thread"
  val unavailableKind: String = "unavailable"

  /** 1-based rank `ceil(percent / 100 * count)`, returned as a 0-based index. */
  def rankIndex(count: Int, percent: Int): Int =
    val rank = (percent * count + 99) / 100
    val clamped = math.max(1, math.min(count, rank))
    clamped - 1

  def checkPercentileConvention(): Unit =
    val p50 = rankIndex(measure, 50)
    val p95 = rankIndex(measure, 95)
    val last = measure - 1
    require(measure >= 21, s"measure=$measure is below 21")
    require(
      p95 < last - 1,
      s"p95 index $p95 collapses onto the top two samples of $measure",
    )
    require(p50 != p95 && p50 < p95, s"p50 index $p50 and p95 index $p95 are unordered")

  final case class Timed(
      elapsedNs: Long,
      batches: Long,
      encodes: Long,
      visits: Long,
      gcMs: Long,
  )

  final case class Account[A](value: A, callerBytes: Long, estimateBytes: Long)

  final case class Row(
      kind: String,
      name: String,
      threads: Int,
      shape: String,
      rowCount: Int,
      p50Ns: Long,
      p95Ns: Long,
      maxNs: Long,
      spreadNs: Long,
      allocBytes: Long,
      callerBytes: Long,
      allocKind: String,
      batches: Long,
      encodes: Long,
      visits: Long,
      gcMs: Long,
      checksum: Long,
  )

  def timeOnce[A](body: => A): (A, Timed) =
    // Allocate and install before the GC read. Creating the sink inside that window biases
    // every sample.
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    val gcBefore = gcMillis
    try
      val start = System.nanoTime()
      val value = body
      val elapsed = System.nanoTime() - start
      val timed = Timed(
        elapsed,
        sink.batchCount,
        sink.primitiveEncodeCount,
        sink.visitCount,
        gcDelta(gcBefore, gcMillis),
      )
      (value, timed)
    finally ColumnMetrics.restore(previous)

  def summarise(samples: Array[Long]): (Long, Long, Long) =
    val sorted = samples.clone()
    java.util.Arrays.sort(sorted)
    val count = sorted.length
    (sorted(rankIndex(count, 50)), sorted(rankIndex(count, 95)), sorted(count - 1))

  /**
   * Absolute gap between the median of rounds where this width ran first and the median of rounds
   * where it ran second. Subset medians use the same nearest-rank rule.
   */
  def orderSpread(samples: Array[Long], ledWhenEven: Boolean): Long =
    val led = subset(samples, ledWhenEven, wantLed = true)
    val followed = subset(samples, ledWhenEven, wantLed = false)
    math.abs(summarise(led)._1 - summarise(followed)._1)

  def mean(values: Array[Long]): Long =
    var index = 0
    var total = 0L
    var negative = false
    while (index < values.length) {
      val value = values(index)
      if (value < 0L) then negative = true
      else total += value
      index += 1
    }
    if (negative || values.isEmpty) then -1L else total / values.length.toLong

  def sumOf(values: Array[Long]): Long =
    var index = 0
    var total = 0L
    var negative = false
    while (index < values.length) {
      val value = values(index)
      if (value < 0L) then negative = true
      else total += value
      index += 1
    }
    if (negative) then -1L else total

  def same(values: Array[Long], label: String): Long =
    val first = values(0)
    var index = 1
    while (index < values.length) {
      if (values(index) != first) then
        throw new IllegalStateException(s"$label varied: ${values.mkString(",")}")
      index += 1
    }
    first

  def rowOf(
      kind: String,
      name: String,
      threads: Int,
      shape: String,
      rowCount: Int,
      checksum: Long,
      allocKind: String,
      elapsed: Array[Long],
      batches: Array[Long],
      encodes: Array[Long],
      visits: Array[Long],
      gc: Array[Long],
      allocBytes: Long,
      callerBytes: Long,
      spreadNs: Long,
  ): Row =
    val (p50, p95, maxNs) = summarise(elapsed)
    Row(
      kind,
      name,
      threads,
      shape,
      rowCount,
      p50,
      p95,
      maxNs,
      spreadNs,
      allocBytes,
      callerBytes,
      allocKind,
      same(batches, s"$kind $name batches"),
      same(encodes, s"$kind $name encodes"),
      same(visits, s"$kind $name visits"),
      sumOf(gc),
      checksum,
    )

  def startSampler(): Option[AllocSampler] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocSampler(bean)) else None
    }

  def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  def callerAccount[A](bean: Option[ThreadMXBean])(body: => A): Account[A] =
    bean match
      case None => Account(body, -1L, -1L)
      case Some(active) =>
        val id = Thread.currentThread().getId
        val before = active.getThreadAllocatedBytes(id)
        val value = body
        val after = active.getThreadAllocatedBytes(id)
        val bytes = delta(before, after)
        Account(value, bytes, bytes)

  def gcDelta(before: Long, after: Long): Long =
    if (before < 0L || after < 0L) then -1L else after - before

  def gcMillis: Long =
    val beans = ManagementFactory.getGarbageCollectorMXBeans
    var index = 0
    var total = 0L
    var known = false
    while (index < beans.size) {
      val time = beans.get(index).getCollectionTime
      if (time >= 0L) then
        known = true
        total += time
      index += 1
    }
    if (known) total else -1L

  /** Four dependent `+1` adds. The JIT can fold this chain into `+4`. */
  def rawFolded(values: Array[Int], start: Int, end: Int): Long =
    var total = 0L
    var row = start
    while (row < end) {
      val first = values(row) + 1
      val second = first + 1
      val third = second + 1
      val fourth = third + 1
      total += fourth.toLong
      row += 1
    }
    total

  /**
   * Four updates whose addend depends on the current value. This is not a constant `+4`. The JIT
   * may still unroll the four stores.
   */
  def rawDependent(values: Array[Int], start: Int, end: Int): Long =
    var total = 0L
    var row = start
    while (row < end) {
      var current = values(row)
      current = current + (current & 1) + 1
      current = current + (current & 1) + 1
      current = current + (current & 1) + 1
      current = current + (current & 1) + 1
      total += current.toLong
      row += 1
    }
    total

  def generate(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = 0x12345678
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  def expectPlus(values: Array[Int], steps: Int): Long =
    var index = 0
    var sum = 0L
    val extra = steps.toLong
    while (index < values.length) {
      sum += values(index).toLong + extra
      index += 1
    }
    sum

  def ms(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000.0)

  def signedMs(nanos: Long): String =
    String.format(Locale.ENGLISH, "%+.3f", nanos.toDouble / 1000000.0)

  def bytes(value: Long): String =
    if (value < 0L) then "n/a" else signedBytes(value)

  def signedBytes(value: Long): String =
    String.format(Locale.ENGLISH, "%.2f MB", value.toDouble / 1000000.0)

  def multiple(numerator: Long, denominator: Long): String =
    if (denominator == 0L) then "n/a"
    else String.format(Locale.ENGLISH, "%.2f", numerator.toDouble / denominator.toDouble)

  def percent(numerator: Long, denominator: Long): String =
    if (denominator == 0L) then "n/a"
    else
      String.format(Locale.ENGLISH, "%.1f%%", (100.0 * numerator.toDouble) / denominator.toDouble)

  def nsPerRow(p50Ns: Long, rows: Int): String =
    if (rows <= 0) then "n/a"
    else String.format(Locale.ENGLISH, "%.2f", p50Ns.toDouble / rows.toDouble)

  def spreadText(spreadNs: Long): String =
    if (spreadNs < 0L) then "n/a" else s"${ms(spreadNs)} ms"

  private def subset(samples: Array[Long], ledWhenEven: Boolean, wantLed: Boolean): Array[Long] =
    val count = samples.length
    var seen = 0
    var index = 0
    while (index < count) {
      if (isLed(index, ledWhenEven) == wantLed) then seen += 1
      index += 1
    }
    val out = new Array[Long](seen)
    var write = 0
    index = 0
    while (index < count) {
      if (isLed(index, ledWhenEven) == wantLed) then
        out(write) = samples(index)
        write += 1
      index += 1
    }
    out

  private def isLed(index: Int, ledWhenEven: Boolean): Boolean =
    (index % 2 == 0) == ledWhenEven

  private def delta(before: Long, after: Long): Long =
    if (before < 0L || after < 0L) then -1L else after - before

  /**
   * Counts bytes on the caller, on `scarlet-morsel-*`, and on raw workers that publish their own
   * counter before returning. The thread is parked unless an allocation pass sets it scanning.
   * Morsel counters are the largest value observed before the worker left the thread list. A worker
   * that is never seen, or that allocates after the last read, is missing. Caller bytes are an
   * exact before/after pair and include planner objects and TLAB reservations.
   */
  final class AllocSampler(val bean: ThreadMXBean):
    private val scratch = new Array[Thread](1024)
    private val capacity = 256
    private val ids = new Array[Long](capacity)
    private val high = new Array[Long](capacity)
    private var count = 0
    private var committed = 0L
    @volatile private var running = true
    @volatile private var scanning = false
    @volatile private var entered = false
    private val lock = new Object
    private val roots = rootGroup()
    private val thread = new Thread(() => loop(), "scarlet-alloc-probe")

    thread.setDaemon(true)
    thread.start()

    def stop(): Unit =
      running = false
      scanning = false
      LockSupport.unpark(thread)
      thread.join()
      ()

    def account[A](body: => A)(rawBytes: => Long): Account[A] =
      val callerId = Thread.currentThread().getId
      scanning = false
      awaitIdle()
      lock.synchronized { reset() }
      scanning = true
      val outcome =
        try
          awaitEntered()
          val before = bean.getThreadAllocatedBytes(callerId)
          val value = body
          val after = bean.getThreadAllocatedBytes(callerId)
          val raw = rawBytes
          (value, delta(before, after), raw)
        finally
          scanning = false
          awaitIdle()
      val (value, caller, raw) = outcome
      val morsel = lock.synchronized { morselTotal() }
      Account(value, caller, combine(caller, morsel, raw))

    private def loop(): Unit =
      while (running) {
        if (scanning) then
          entered = true
          observe()
        else
          entered = false
          if (scanning) then ()
          else if (running) then LockSupport.park()
      }

    private def awaitEntered(): Unit =
      val deadline = System.nanoTime() + 1000000000L
      while (!entered) {
        LockSupport.unpark(thread)
        if (System.nanoTime() > deadline) then
          throw new IllegalStateException("alloc sampler did not start")
        LockSupport.parkNanos(10000L)
      }

    private def awaitIdle(): Unit =
      val deadline = System.nanoTime() + 1000000000L
      while (entered) {
        if (System.nanoTime() > deadline) then
          throw new IllegalStateException("alloc sampler did not pause")
        LockSupport.parkNanos(10000L)
      }

    private def observe(): Unit =
      val found = roots.enumerate(scratch)
      if (found >= scratch.length) then
        throw new IllegalStateException(
          s"alloc sampler saw at least ${scratch.length} live threads",
        )
      lock.synchronized {
        var index = 0
        while (index < found) {
          val live = scratch(index)
          val name = live.getName
          if (name.startsWith("scarlet-morsel-")) then
            val id = live.getId
            val bytes = bean.getThreadAllocatedBytes(id)
            if (bytes >= 0L) then note(id, bytes)
          index += 1
        }
      }

    private def note(id: Long, bytes: Long): Unit =
      val at = indexOf(id)
      if (at >= 0) then
        if (bytes < high(at)) then
          committed += high(at)
          high(at) = bytes
        else if (bytes > high(at)) then high(at) = bytes
      else if (count >= capacity) then
        throw new IllegalStateException(s"alloc sampler filled $capacity thread slots")
      else
        ids(count) = id
        high(count) = bytes
        count += 1

    private def morselTotal(): Long =
      var total = committed
      var index = 0
      while (index < count) {
        total += high(index)
        index += 1
      }
      total

    private def reset(): Unit =
      count = 0
      committed = 0L

    private def indexOf(id: Long): Int =
      var index = 0
      var found = -1
      while (index < count && found < 0) {
        if (ids(index) == id) then found = index
        index += 1
      }
      found

    private def combine(caller: Long, morsel: Long, raw: Long): Long =
      if (caller < 0L || morsel < 0L || raw < 0L) then -1L else caller + morsel + raw

    private def rootGroup(): ThreadGroup =
      var group = Thread.currentThread().getThreadGroup
      var parent = Option(group.getParent)
      while (parent.nonEmpty) {
        group = parent.get
        parent = Option(group.getParent)
      }
      group

  /**
   * Create and join raw workers inside [[sum]], matching `MorselScheduler`'s per-action lifecycle.
   * The threads are not started for engine samples. `publish` is set only on the allocation pass,
   * and each worker stores its counter before returning.
   */
  final class RawFloor(values: Array[Int], workers: Int):
    private val width = workers
    private val starts = new Array[Int](math.max(width, 1))
    private val ends = new Array[Int](math.max(width, 1))
    private val partials = new Array[Long](math.max(width, 1))
    private val workerAlloc = new Array[Long](math.max(width, 1))

    require(width >= 1, s"raw floor needs a positive thread count, got $width")
    java.util.Arrays.fill(workerAlloc, -1L)
    if (width > 1) then fillRanges(values.length, starts, ends)

    def publishedTotal: Long =
      if (width <= 1) then 0L
      else
        var index = 0
        var total = 0L
        var missing = false
        while (index < width) {
          val bytes = workerAlloc(index)
          if (bytes < 0L) then missing = true else total += bytes
          index += 1
        }
        if (missing) -1L else total

    def sum(dependent: Boolean, publish: Option[ThreadMXBean]): Long =
      if (width <= 1) then scan(dependent, 0, values.length)
      else
        java.util.Arrays.fill(workerAlloc, -1L)
        val threads = Array.tabulate(width) { id =>
          val workerThread = new Thread(() => worker(id, dependent, publish), s"scarlet-raw-$id")
          workerThread.setDaemon(true)
          workerThread
        }
        threads.foreach(_.start())
        threads.foreach(_.join())
        var total = 0L
        var index = 0
        while (index < width) {
          total += partials(index)
          index += 1
        }
        total

    private def worker(id: Int, dependent: Boolean, publish: Option[ThreadMXBean]): Unit =
      partials(id) = scan(dependent, starts(id), ends(id))
      publish.foreach { active =>
        val bytes = active.getThreadAllocatedBytes(Thread.currentThread().getId)
        workerAlloc(id) = if (bytes >= 0L) then bytes else -1L
      }

    private def scan(dependent: Boolean, start: Int, end: Int): Long =
      if (dependent) then rawDependent(values, start, end) else rawFolded(values, start, end)

    private def fillRanges(length: Int, intoStart: Array[Int], intoEnd: Array[Int]): Unit =
      val count = width
      val base = length / count
      val extra = length % count
      var index = 0
      var cursor = 0
      while (index < count) {
        val size = base + (if (index < extra) then 1 else 0)
        intoStart(index) = cursor
        cursor += size
        intoEnd(index) = cursor
        index += 1
      }
      require(cursor == length, s"split $cursor != $length")
