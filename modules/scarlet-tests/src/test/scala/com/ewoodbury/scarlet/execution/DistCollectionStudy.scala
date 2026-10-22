package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.concurrent.locks.LockSupport

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Times `sum` and `groupedSum` on primitive sources. Not a test, and not part of `make test`.
 *
 * The timed region is the action. The checksum is outside it. `alloc_bytes` is the mean of bytes
 * allocated during the action. A probe reads `ThreadMXBean` while the action runs, because morsel
 * workers have exited by the time the action returns. The probe's own bytes are left out. Columnar
 * and row run the same plans. Each thread count sets `ScarletConf.threadPoolSize` and installs a
 * `LocalTaskScheduler` of that width, because the process-wide scheduler is built once.
 *
 * Narrow is `ofInts.filter(_ > 0).map(_ * 2).sum` at 1e6 rows. The chain is six `map(_ + 1)`
 * calls and then `sum`, on that same array. Grouped is `ofIntPairs.groupedSum` at 1e7 rows and
 * 1,024 groups. Plans are built before the timer.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.DistCollectionStudy"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object DistCollectionStudy:

  private val warmup = 5
  private val measure = 11
  private val narrowRows = 1000000
  private val groupedRows = 10000000
  private val groups = 1024
  private val partitions = 4
  private val narrowChecksum = 500354792612L

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val probe = startProbe()
    try
      val values = generate(narrowRows)
      val ints = DistCollection.ofInts(values, partitions)
      val narrow = ints.filter(_ > 0).map(_ * 2)
      val chain = ints
        .map(_ + 1)
        .map(_ + 1)
        .map(_ + 1)
        .map(_ + 1)
        .map(_ + 1)
        .map(_ + 1)
      val grouped = locally {
        val keys = Array.tabulate(groupedRows)(row => row % groups)
        val groupValues = Array.tabulate(groupedRows)(row => row)
        DistCollection.ofIntPairs(keys, groupValues, partitions)
      }
      val narrowSum = expectNarrow(values)
      val chainSum = expectPlus(values, steps = 6)
      val groupedSum = groupedRows.toLong * (groupedRows.toLong - 1L) / 2L
      require(narrowSum == narrowChecksum, s"narrow generator drifted: $narrowSum")
      println(header(probe.isDefined, narrowSum, chainSum, groupedSum))
      runBoth(probe, "narrow", narrowRows, narrowSum)(narrow.sum)
      runBoth(probe, "chain6", narrowRows, chainSum)(chain.sum)
      runBoth(probe, "grouped1024", groupedRows, groupedSum)(grouped.groupedSum)
    finally
      probe.foreach(_.stop())
      ScarletConf.set(savedConf)
      ScarletRuntime.set(savedRuntime)

  private def runBoth(
      probe: Option[AllocProbe],
      name: String,
      rows: Int,
      expected: Long,
  )(action: => Long): Unit =
    Seq(1, 4).foreach { threads =>
      timeLayouts(probe, s"$name-t$threads", threads, rows, expected)(action)
    }

  private def timeLayouts(
      probe: Option[AllocProbe],
      name: String,
      threads: Int,
      rows: Int,
      expected: Long,
  )(action: => Long): Unit =
    installThreads(threads)
    Seq(true, false).foreach { columnar =>
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = columnar))
      val label = if (columnar) "columnar" else "row"
      val title = s"$name-$label"
      System.err.println(s"run $title")
      (0 until warmup).foreach { _ =>
        ScarletRuntime.get.shuffle.clear()
        val got = action
        require(got == expected, s"$title warmup $got != $expected")
      }
      val samples = new Array[Long](measure)
      val allocated = new Array[Long](measure)
      val gcBefore = gcMillis
      (0 until measure).foreach { sample =>
        ScarletRuntime.get.shuffle.clear()
        val (got, elapsed, bytes) = measure(probe)(action)
        require(got == expected, s"$title $got != $expected")
        samples(sample) = elapsed
        allocated(sample) = bytes
      }
      val gcAfter = gcMillis
      val gc = if (gcBefore < 0L || gcAfter < 0L) -1L else gcAfter - gcBefore
      report(title, rows, expected, samples, allocated, gc)
    }

  /** The row scheduler is constructed once. Swap it so the thread count is the one just set. */
  private def installThreads(threads: Int): Unit =
    val conf = ScarletConf.get
    ScarletConf.set(conf.copy(threadPoolSize = threads))
    val runtime = ScarletRuntime.get
    ScarletRuntime.set(runtime.copy(scheduler = new LocalTaskScheduler(threads)))

  private def report(
      title: String,
      rows: Int,
      expected: Long,
      samples: Array[Long],
      allocated: Array[Long],
      gcMs: Long,
  ): Unit =
    val p50 = percentile(samples, 50)
    val p95 = percentile(samples, 95)
    val alloc = mean(allocated)
    val rate = if (p50 <= 0L) 0L else (rows.toLong * 1000000000L) / p50
    println(
      s"$title\trows=$rows\tp50_ns=$p50\tp95_ns=$p95\trows_per_s=$rate\t" +
        s"alloc_bytes=$alloc\tgc_ms=$gcMs\tchecksum=$expected",
    )

  private def measure[A](probe: Option[AllocProbe])(body: => A): (A, Long, Long) =
    probe match
      case Some(active) => active.sample(body)
      case None =>
        val start = System.nanoTime()
        val value = body
        (value, System.nanoTime() - start, -1L)

  private def percentile(samples: Array[Long], percent: Int): Long =
    val sorted = samples.clone()
    java.util.Arrays.sort(sorted)
    val index =
      if (percent <= 0) then 0
      else if (percent >= 100) then sorted.length - 1
      else (percent * (sorted.length - 1)) / 100
    sorted(index)

  private def mean(values: Array[Long]): Long =
    var index = 0
    var total = 0L
    var negative = false
    while (index < values.length) {
      val value = values(index)
      if (value < 0L) then negative = true
      else total += value
      index += 1
    }
    if (negative) -1L else total / values.length.toLong

  private def expectNarrow(values: Array[Int]): Long =
    var index = 0
    var sum = 0L
    while (index < values.length) {
      val value = values(index)
      if (value > 0) then sum += value.toLong * 2L
      index += 1
    }
    sum

  private def expectPlus(values: Array[Int], steps: Int): Long =
    var index = 0
    var sum = 0L
    val extra = steps.toLong
    while (index < values.length) {
      sum += values(index).toLong + extra
      index += 1
    }
    sum

  private def generate(rows: Int): Array[Int] =
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

  private def header(
      allocOn: Boolean,
      narrowSum: Long,
      chainSum: Long,
      groupedSum: Long,
  ): String =
    val conf = ScarletConf.get
    val runtime = ManagementFactory.getRuntimeMXBean
    val alloc = if (allocOn) "probe" else "unavailable"
    s"distcollection study warmup=$warmup measure=$measure batch=${conf.columnarBatchSize} " +
      s"partitions=$partitions shuffle=${conf.defaultShufflePartitions} alloc_bytes=$alloc\n" +
      s"narrow rows=$narrowRows checksum=$narrowSum chain6 rows=$narrowRows checksum=$chainSum " +
      s"grouped1024 rows=$groupedRows groups=$groups checksum=$groupedSum\n" +
      s"java=${runtime.getVmName} ${runtime.getVmVersion} args=${runtime.getInputArguments}"

  private def startProbe(): Option[AllocProbe] =
    threadBean.flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
    }

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  /**
   * Samples live threads while an action runs. Morsel workers are gone when the action returns, so
   * a before/after pair on the caller misses their bytes. The probe's own counter is excluded.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
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
    while (probeId == 0L) LockSupport.parkNanos(10000L)

    def stop(): Unit =
      running = false
      LockSupport.unpark(thread)
      thread.join(1000L)
      ()

    def sample[A](body: => A): (A, Long, Long) =
      open()
      val start = System.nanoTime()
      val value = body
      val elapsed = System.nanoTime() - start
      (value, elapsed, close())

    private def watch(): Unit =
      probeId = Thread.currentThread().getId
      while (running) {
        observe(setBaseline = false)
        LockSupport.parkNanos(200000L)
      }

    private def open(): Unit =
      this.synchronized {
        count = 0
        observe(setBaseline = true)
      }

    private def close(): Long =
      this.synchronized {
        observe(setBaseline = false)
        var index = 0
        var total = 0L
        while (index < count) {
          if (ids(index) != probeId) then
            val base = if (baseline(index) < 0L) 0L else baseline(index)
            val delta = high(index) - base
            if (delta > 0L) then total += delta
          index += 1
        }
        total
      }

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
      if (at >= 0) then
        if (bytes > high(at)) then high(at) = bytes
        if (setBaseline) then baseline(at) = bytes

    private def slot(id: Long): Int =
      val existing = indexOf(id)
      if (existing >= 0) then existing
      else if (count >= capacity) then -1
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

  private def gcMillis: Long =
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
