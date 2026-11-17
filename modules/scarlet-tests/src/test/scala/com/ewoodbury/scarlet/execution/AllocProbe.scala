package com.ewoodbury.scarlet.execution

import java.util.concurrent.locks.LockSupport

import com.sun.management.ThreadMXBean

/**
 * Sums bytes on every live thread except the probe. Workers have often exited when the action
 * returns, so a caller-only delta misses them. The watch reads while the action runs.
 *
 * `parkNanos` is the watch interval. Join-build uses 20 µs; the query and cross-engine benches use
 * 50 µs. DistCollectionStudy keeps its own probe: `sample` returns elapsed nanos, a full table
 * returns -1 instead of throwing, and the watch parks 200 µs.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] final class AllocProbe(bean: ThreadMXBean, parkNanos: Long = 50000L):
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
    probeId = Thread.currentThread().getId
    while (running) {
      observe(setBaseline = false)
      LockSupport.parkNanos(parkNanos)
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
        val base = if (baseline(index) < 0L) 0L else baseline(index)
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
