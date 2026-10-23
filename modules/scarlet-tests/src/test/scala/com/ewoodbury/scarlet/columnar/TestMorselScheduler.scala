package com.ewoodbury.scarlet.columnar

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, CyclicBarrier, TimeUnit}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
    // update must fail this, which eight visits on four workers does not.
    val workers = 8
    val rounds = 4
    val perRound = 2048
    val batch = BatchCodec.ints(Seq(1))
    val morsels = Vector.fill(perRound)(batch)
    val expected = rounds.toLong * perRound.toLong
    ColumnMetrics.withSink(new CountingSink) {
      (1 to rounds).foreach { _ =>
        val gate = new CyclicBarrier(workers)
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
