package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.locks.LockSupport
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, CyclicBarrier, TimeUnit}

import scala.jdk.CollectionConverters.*

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

class TestMorselScheduler extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ()

  "scheduler" should "return results in morsel order on one thread" in {
    val morsels = (0 until 5).toVector.map(row => BatchCodec.ints(Seq(row)))

    val lengths = MorselScheduler.map(morsels, parallelism = 1)((_, batch) => batch.length)

    lengths shouldBe morsels.map(_.length)
  }

  it should "count one visit for every morsel when several workers run" in {
    val morsels = (0 until 8).toVector.map(row => BatchCodec.ints(Seq(row)))
    ColumnMetrics.withSink(new CountingSink) {
      val lengths = MorselScheduler.map(morsels, parallelism = 4)((_, batch) => batch.length)
      lengths shouldBe morsels.map(_.length)
      ColumnMetrics.visitCount shouldBe morsels.length.toLong
    }
  }

  it should "count every visit when workers note together" in {
    // Workers share one sink and note together after the barrier. The total is exact: a lost
    // update must fail this, which eight visits on four workers does not. The pool seats
    // `threadPoolSize` workers, so a larger barrier would wait for a thread that cannot start.
    val workers = 8
    val parties = math.min(workers, ScarletConf.get.threadPoolSize)
    parties should be > 1
    val rounds = 4
    val perRound = 2048
    val batch = BatchCodec.ints(Seq(1))
    val morsels = Vector.fill(perRound)(batch)
    val expected = rounds.toLong * perRound.toLong
    ColumnMetrics.withSink(new CountingSink) {
      (1 to rounds).foreach { _ =>
        val gate = new CyclicBarrier(parties)
        val seen = ConcurrentHashMap.newKeySet[Thread]()
        val lengths = MorselScheduler.map(morsels, parallelism = workers) { (_, morsel) =>
          if (seen.add(Thread.currentThread)) then
            val _ = gate.await(5, TimeUnit.SECONDS)
          morsel.length
        }
        lengths.sum shouldBe perRound
      }
      ColumnMetrics.visitCount shouldBe expected
    }
  }

  it should "steal the tail of a worker blocked on its own head" in {
    // Two workers, eight morsels. Worker 0's deque is 0, 2, 4, 6 and it takes 0 first.
    // 0 blocks until 2, 4, and 6 finish. Those sit behind 0, so they run only if the other
    // worker steals the tail. A failed steal times out instead of passing.
    val morsels = (0 until 8).toVector.map(row => BatchCodec.ints(Seq(row)))
    val holdingHead = new CountDownLatch(1)
    val matesDone = new CountDownLatch(3)
    val servedBy = Array.fill(morsels.length)("")

    val results = MorselScheduler.map(morsels, parallelism = 2) { (index, batch) =>
      servedBy(index) = Thread.currentThread.getName
      if (index == 0) then
        holdingHead.countDown()
        val opened = matesDone.await(5, TimeUnit.SECONDS)
        (opened, batch.length)
      else
        val held = holdingHead.await(5, TimeUnit.SECONDS)
        if (index == 2 || index == 4 || index == 6) then matesDone.countDown()
        (held, batch.length)
    }

    results.map(_._2) shouldBe morsels.map(_.length)
    results.foreach { (opened, _) => opened shouldBe true }
    val owner = servedBy(0)
    owner should startWith("scarlet-morsel-")
    Seq(2, 4, 6).foreach { index =>
      servedBy(index) should startWith("scarlet-morsel-")
      servedBy(index) shouldNot equal(owner)
    }
  }

  it should "fail the call when a morsel throws" in {
    val morsels = Vector(BatchCodec.ints(Seq(1)), BatchCodec.ints(Seq(2)))

    val thrown = intercept[IllegalStateException] {
      MorselScheduler.map(morsels, parallelism = 2) { (index, _) =>
        if (index == 0) then throw new IllegalStateException("morsel failed")
        else index
      }
    }

    thrown.getMessage shouldBe "morsel failed"
  }

  it should "surface an Error from a worker instead of a missing result" in {
    val morsels = Vector(
      BatchCodec.ints(Seq(1)),
      BatchCodec.ints(Seq(2)),
      BatchCodec.ints(Seq(3)),
    )

    val thrown = intercept[OutOfMemoryError] {
      MorselScheduler.map(morsels, parallelism = 3) { (index, _) =>
        if (index == 1) then throw new OutOfMemoryError("morsel")
        else index
      }
    }

    thrown.getMessage shouldBe "morsel"
  }

  it should "run one morsel, and parallelism of 1, on the caller" in {
    val caller = Thread.currentThread
    val single = MorselScheduler.mapByThread(Vector(7), parallelism = 4) { (id, value) =>
      (id, value, Thread.currentThread)
    }
    single shouldBe Vector((0, 7, caller))

    val serial = MorselScheduler.map((0 until 4).toVector, parallelism = 1) { (index, value) =>
      Thread.currentThread should be theSameInstanceAs caller
      index + value
    }
    serial shouldBe (0 until 4).toVector.map(value => value + value)
  }

  it should "return nested results from the calling pool worker" in {
    // A pool worker that dispatches again must not wait for a pool thread. The nested call runs
    // on that worker, in order, and every mapByThread id is 0.
    val outer = (0 until 6).toVector
    val inner = (0 until 5).toVector
    val results = MorselScheduler.mapByThread(outer, parallelism = 3) { (outerId, outerValue) =>
      val name = Thread.currentThread.getName
      val nested = MorselScheduler.mapByThread(inner, parallelism = 4) { (innerId, value) =>
        (innerId, value, Thread.currentThread.getName)
      }
      (outerId, outerValue, name, nested)
    }

    results.map(_._2) shouldBe outer
    results.map(_._1).foreach { id =>
      id should be >= 0
      id should be < 3
    }
    results.groupBy(_._3).foreachEntry { (name, rows) =>
      name should startWith("scarlet-morsel-")
      rows.map(_._1).distinct should have size 1
      rows.foreach { case (_, _, _, nested) =>
        nested.map(_._2) shouldBe inner
        nested.foreach { case (innerId, _, innerName) =>
          innerId shouldBe 0
          innerName shouldBe name
        }
      }
    }
  }

  it should "keep results in morsel order when one morsel is slow" in {
    val morsels = (0 until 12).toVector
    val results = MorselScheduler.map(morsels, parallelism = 4) { (index, value) =>
      if (index == 0) then LockSupport.parkNanos(40_000_000L)
      value
    }
    results shouldBe morsels
  }

  it should "fail with the same throwable and stop taking morsels" in {
    val workers = 4
    val morsels = (0 until 64).toVector
    val boom = new IllegalStateException("morsel failed")
    val late = failFast(morsels, workers, boom)
    late should be <= (workers * 2)
  }

  it should "fail with the same Error and stop taking morsels" in {
    val workers = 4
    val morsels = (0 until 64).toVector
    val boom = new OutOfMemoryError("morsel")
    val late = failFast(morsels, workers, boom)
    late should be <= (workers * 2)
  }

  it should "give each worker one id, and a stolen morsel the thief's id" in {
    val morsels = (0 until 8).toVector
    val holdingHead = new CountDownLatch(1)
    val matesDone = new CountDownLatch(3)
    val servedId = Array.fill(morsels.length)(-1)
    val idsByThread = new ConcurrentHashMap[String, Integer]()

    val results = MorselScheduler.mapByThread(morsels, parallelism = 2) { (id, index) =>
      val name = Thread.currentThread.getName
      val prior = Option(idsByThread.put(name, Integer.valueOf(id)))
      prior.foreach(old => old.intValue shouldBe id)
      servedId(index) = id
      if (index == 0) then
        holdingHead.countDown()
        val opened = matesDone.await(5, TimeUnit.SECONDS)
        (opened, id)
      else
        val held = holdingHead.await(5, TimeUnit.SECONDS)
        if (index == 2 || index == 4 || index == 6) then matesDone.countDown()
        (held, id)
    }

    results.foreach { (opened, _) => opened shouldBe true }
    servedId(0) shouldBe 0
    Seq(2, 4, 6).foreach(index => servedId(index) shouldBe 1)
    servedId.foreach { id =>
      id should be >= 0
      id should be < 2
    }
    idsByThread.size shouldBe 2
  }

  it should "keep one id per worker across a dispatch" in {
    val width = 4
    val morsels = (0 until 32).toVector
    val idsByThread = new ConcurrentHashMap[String, Integer]()
    val ids = MorselScheduler.mapByThread(morsels, parallelism = width) { (id, value) =>
      val name = Thread.currentThread.getName
      name should startWith("scarlet-morsel-")
      val prior = Option(idsByThread.put(name, Integer.valueOf(id)))
      prior.foreach(old => old.intValue shouldBe id)
      id should be >= 0
      id should be < width
      value
    }
    ids shouldBe morsels
    idsByThread.size should be <= width
    idsByThread.size should be > 0
  }

  it should "reuse pool threads across dispatches" in {
    val morsels = (0 until 16).toVector
    val _ = MorselScheduler.map(morsels, parallelism = 4)((index, value) => value + index)
    val namedBefore = morselThreads
    val liveBefore = Thread.getAllStackTraces.size
    (1 to 50).foreach { _ =>
      val got = MorselScheduler.map(morsels, parallelism = 4)((index, value) => value + index)
      got shouldBe morsels.map(value => value + value)
    }
    morselThreads shouldBe namedBefore
    Thread.getAllStackTraces.size should be <= (liveBefore + 4)
    namedBefore should be > 0
  }

  it should "keep two concurrent dispatches from mixing results" in {
    // Each caller blocks in its first morsel until the other caller is there too. A lock around
    // dispatch never opens that barrier. The two results stay in their own morsel order.
    val meet = new CyclicBarrier(2)
    val left = (0 until 32).toVector
    val right = (1000 until 1032).toVector
    val failure = new AtomicReference[Option[Throwable]](None)
    val leftOut = new AtomicReference[Option[Vector[Int]]](None)
    val rightOut = new AtomicReference[Option[Vector[Int]]](None)
    val leftThread = new Thread(() => concurrent(left, meet, leftOut, failure), "dispatch-left")
    val rightThread = new Thread(() => concurrent(right, meet, rightOut, failure), "dispatch-right")
    leftThread.start()
    rightThread.start()
    leftThread.join(10_000L)
    rightThread.join(10_000L)
    leftThread.isAlive shouldBe false
    rightThread.isAlive shouldBe false
    failure.get() shouldBe Option.empty[Throwable]
    leftOut.get() shouldBe Some(left)
    rightOut.get() shouldBe Some(right)
  }

  it should "finish a second dispatch while the first is still in flight" in {
    val started = new CountDownLatch(2)
    val release = new CountDownLatch(1)
    val slowOut = new AtomicReference[Option[Vector[Int]]](None)
    val slowError = new AtomicReference[Option[Throwable]](None)
    val slow = new Thread(
      () =>
        try
          val got = MorselScheduler.map(Vector(1, 2), parallelism = 2) { (_, value) =>
            started.countDown()
            val opened = release.await(5, TimeUnit.SECONDS)
            if (!opened) then throw new IllegalStateException("slow dispatch was not released")
            value
          }
          slowOut.set(Some(got))
        catch
          case thrown: Throwable =>
            val _ = slowError.compareAndSet(None, Some(thrown)),
      "dispatch-slow",
    )
    slow.start()
    val running = started.await(5, TimeUnit.SECONDS)
    running shouldBe true
    val began = System.nanoTime()
    val fast = MorselScheduler.map(Vector(10, 20, 30, 40), parallelism = 2) { (index, value) =>
      value + index
    }
    val elapsedMs = (System.nanoTime() - began) / 1_000_000L
    release.countDown()
    slow.join(5_000L)
    slow.isAlive shouldBe false
    slowError.get() shouldBe Option.empty[Throwable]
    slowOut.get() shouldBe Some(Vector(1, 2))
    fast shouldBe Vector(10, 21, 32, 43)
    elapsedMs should be < 2_000L
  }

  /**
   * One caller. The first morsel waits until the other dispatch has entered, then the rest run.
   * Ids stay inside this dispatch's width, and one thread keeps one id here even if it also serves
   * the other dispatch.
   */
  private def concurrent(
      morsels: Vector[Int],
      meet: CyclicBarrier,
      sink: AtomicReference[Option[Vector[Int]]],
      failure: AtomicReference[Option[Throwable]],
  ): Unit =
    val announced = new AtomicBoolean(false)
    val idsByThread = new ConcurrentHashMap[String, Integer]()
    try
      val got = MorselScheduler.mapByThread(morsels, parallelism = 2) { (id, value) =>
        val name = Thread.currentThread.getName
        val prior = Option(idsByThread.put(name, Integer.valueOf(id)))
        prior.foreach(old => old.intValue shouldBe id)
        if (announced.compareAndSet(false, true)) then
          val _ = meet.await(5, TimeUnit.SECONDS)
        id should be >= 0
        id should be < 2
        value
      }
      sink.set(Some(got))
    catch case thrown: Throwable =>
      val _ = failure.compareAndSet(None, Some(thrown))

  private def morselThreads: Int =
    Thread.getAllStackTraces.keySet.asScala.count(_.getName.startsWith("scarlet-morsel-"))

  /**
   * The other workers block inside their first morsel until index 0 is about to throw `boom`.
   * Morsels taken after that count as late. A scheduler that ignores the failure runs the rest of
   * the queue, which is much larger than one extra morsel per worker.
   */
  private def failFast(morsels: Vector[Int], workers: Int, boom: Throwable): Int =
    val seen = ConcurrentHashMap.newKeySet[Thread]()
    val arrived = new CountDownLatch(workers - 1)
    val release = new CountDownLatch(1)
    val late = new AtomicInteger(0)
    val thrown = intercept[Throwable] {
      MorselScheduler.map(morsels, parallelism = workers) { (index, _) =>
        if (index == 0) then
          val others = arrived.await(5, TimeUnit.SECONDS)
          release.countDown()
          if (!others) then throw new IllegalStateException("workers did not arrive")
          throw boom
        else if (seen.add(Thread.currentThread)) then
          arrived.countDown()
          val _ = release.await(5, TimeUnit.SECONDS)
          index
        else
          // Park briefly so a worker cannot sprint through the queue in the window between the
          // throw and the scheduler recording it. Without this the count depends on how fast the
          // pool can churn, and the bound below is unmeetable on a fast machine.
          val _ = late.incrementAndGet()
          LockSupport.parkNanos(2_000_000L)
          index
      }
    }
    thrown should be theSameInstanceAs (boom)
    late.get()
