package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.columnar.{ColumnMetrics, NoOpSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Proves the join harness oracle and the bookkeeping around it. The full sizes stay in
 * [[JoinBench]] and are not executed here.
 */
class TestJoinBench extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "the digest" should "ignore row order and keep a null key out of key 0" in {
    val forward = Seq(
      JoinCases.joinRow(None, 1, 2),
      JoinCases.joinRow(Some(0), 3, 4),
      JoinCases.joinRow(Some(1), 5, 6),
      JoinCases.joinRow(Some(1), 7, 8),
    )
    val backward = forward.reverse
    val first = JoinDigest.from(forward)
    val second = JoinDigest.from(backward)

    JoinDigest.agrees(first, second) shouldBe true
    first.total shouldBe 4L
    first.counts.map(_.key) shouldBe Vector(None, Some(0), Some(1))
    first.counts.map(_.count) shouldBe Vector(1L, 1L, 2L)
  }

  it should "treat a value change as a different multiset when the per-key counts match" in {
    val left = JoinDigest.from(Seq(JoinCases.joinRow(Some(1), 2, 3)))
    val right = JoinDigest.from(Seq(JoinCases.joinRow(Some(1), 3, 2)))

    left.counts shouldBe right.counts
    left.total shouldBe right.total
    JoinDigest.agrees(left, right) shouldBe false
    JoinDigest.difference(left, right) shouldBe s"checksum ${left.checksum} != ${right.checksum}"
  }

  it should "name the key whose pair count changed" in {
    val left = JoinDigest.from(
      Seq(JoinCases.joinRow(Some(1), 1, 1), JoinCases.joinRow(Some(1), 1, 2)),
    )
    val right = JoinDigest.from(Seq(JoinCases.joinRow(Some(2), 1, 1)))

    JoinDigest.difference(left, right) shouldBe "pairs 2 != 1"
    val moved = JoinDigest.from(
      Seq(JoinCases.joinRow(Some(1), 1, 1), JoinCases.joinRow(Some(2), 1, 1)),
    )
    JoinDigest.difference(left, moved) shouldBe "key 1 pairs 2 != 1"
  }

  "nearest rank" should "put p50 at rank 5 and p99 on the last of ten samples" in {
    JoinBench.checkHarness()
    JoinBench.rankIndex(10, 50) shouldBe 4
    JoinBench.rankIndex(10, 99) shouldBe 9
    JoinBench.mean(Array(1L, 100L)) shouldBe JoinBench.mean(Array(100L, 1L))
    JoinBench.mean(Array(1L, 2L, 3L, 4L)) shouldBe 2.5
  }

  "warmup" should "leave the sample unset" in {
    val samples = Array.fill(JoinBench.measure)(-1L)
    JoinBench.assign(samples, round = 0, nanos = 5L, record = false)
    samples(0) shouldBe -1L
    JoinBench.assign(samples, round = 0, nanos = 5L, record = true)
    samples(0) shouldBe 5L
    samples(1) shouldBe -1L
  }

  "join settings" should "restore the threshold and the sort-merge flag when the body throws" in {
    val beforeThreshold = ScarletConf.get.broadcastJoinThreshold
    val beforeSort = ScarletConf.get.enableSortMergeJoin
    intercept[IllegalStateException] {
      JoinBench.withJoinSettings(threshold = 1L, sortMerge = false, threads = 1, columnar = true) {
        ScarletConf.get.broadcastJoinThreshold shouldBe 1L
        ScarletConf.get.enableSortMergeJoin shouldBe false
        ScarletConf.set(ScarletConf.get.copy(broadcastJoinThreshold = 42L, enableSortMergeJoin = true))
        throw new IllegalStateException("stop")
      }
    }
    ScarletConf.get.broadcastJoinThreshold shouldBe beforeThreshold
    ScarletConf.get.enableSortMergeJoin shouldBe beforeSort
  }

  "broadcast-shaped" should "keep the broadcast hint and refuse the columnar plan" in {
    val spec = JoinCases.broadcast(probeRows = 32, buildRows = 4)
    JoinCases.hint(spec) shouldBe Some(Plan.JoinStrategy.Broadcast)
    JoinCases.rowStrategy(spec) shouldBe "broadcast"
    JoinBench.columnarShape(spec) shouldBe None

    val row = JoinBench.digestOf(spec, threads = 1, columnar = false)
    row.batches shouldBe 0L
    row.digest.total shouldBe spec.expectedPairs
    row.digest.counts.map(_.count).sum shouldBe spec.expectedPairs
  }

  "hash cases" should "agree across layouts for symmetric, duplicate, and hot keys" in {
    val specs = Seq(
      JoinCases.symmetric(8),
      JoinCases.duplicates(rows = 12, distinct = 3),
      JoinCases.hotKey(8),
    )
    specs.foreach { spec =>
      withClue(s"${spec.name}: ") {
        JoinCases.hint(spec) shouldBe None
        JoinCases.rowStrategy(spec) shouldBe "shuffle-hash"
        JoinBench.columnarShape(spec).isDefined shouldBe true
        val row = JoinBench.digestOf(spec, threads = 1, columnar = false)
        val columnar = JoinBench.digestOf(spec, threads = 1, columnar = true)
        row.batches shouldBe 0L
        columnar.batches should be > 0L
        row.digest.total shouldBe spec.expectedPairs
        JoinDigest.agrees(row.digest, columnar.digest) shouldBe true
      }
    }
  }

  "duplicates" should "define more pairs than either input" in {
    val spec = JoinCases.duplicates(rows = 12, distinct = 3)
    spec.expectedPairs should be > spec.leftRows.toLong
    spec.expectedPairs should be > spec.rightRows.toLong
  }
