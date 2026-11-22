package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Multiplicity product for an int inner-join count. A probe key that misses is not written into
 * the build table.
 */
class TestJoinKeyCount extends AnyFlatSpec with Matchers:

  "a join count table" should "multiply duplicate keys and ignore a foreign probe key" in {
    val build = JoinKeyCount.fresh()
    build.addOne(1)
    build.addOne(1)
    build.addOne(2)
    val probe = JoinKeyCount.fresh()
    probe.addOne(1)
    probe.addOne(9)
    probe.addOne(1)

    build.product(probe) shouldBe 4L
    build.stored(9) shouldBe 0L
    probe.stored(9) shouldBe 1L
    build.stored(1) shouldBe 2L
    build.stored(2) shouldBe 1L
  }

  it should "stay empty when every probe key misses" in {
    val build = JoinKeyCount.fresh()
    build.addOne(4)
    val probe = JoinKeyCount.fresh()
    probe.addOne(8)
    probe.addOne(9)

    JoinKeyCount.multiply(build, probe) shouldBe 0L
    build.stored(8) shouldBe 0L
    build.stored(9) shouldBe 0L
  }

  it should "merge partial counts of the same key" in {
    val left = JoinKeyCount.fresh()
    val right = JoinKeyCount.fresh()
    left.addOne(3)
    right.add(3, 4L)
    val merged = JoinKeyCount.merge(Vector(left, right))

    merged.stored(3) shouldBe 5L
    merged.product(right) shouldBe 20L
  }

  it should "count a negative key and a key outside the direct window" in {
    val table = JoinKeyCount.fresh()
    table.addOne(-1)
    table.addOne(-1)
    table.addOne(4096)
    table.add(4096, 3L)
    table.stored(-1) shouldBe 2L
    table.stored(4096) shouldBe 4L
    table.stored(0) shouldBe 0L

    val probe = JoinKeyCount.fresh()
    probe.addOne(-1)
    probe.addOne(4096)
    table.product(probe) shouldBe 6L
  }

  it should "match a filtered scan to one add per kept key" in {
    val keys = Array(1, -4, 4096, 1, 2, 5000, 0)
    val values = Array(1, 1, -1, 3, -2, 4, 8)
    val keepKey: Int => Boolean = key => key % 3 != 0
    val keepValue: Int => Boolean = value => value >= 0
    val batched = JoinKeyCount.fresh()
    batched.addFiltered(keys, values, 0, keys.length, keepKey, keepValue)

    batched.stored(1) shouldBe 2L
    batched.stored(-4) shouldBe 1L
    batched.stored(5000) shouldBe 1L
    batched.stored(4096) shouldBe 0L
    batched.stored(2) shouldBe 0L
    batched.stored(0) shouldBe 0L

    val other = JoinKeyCount.fresh()
    other.add(1, 2L)
    other.addOne(-4)
    other.addOne(5000)
    batched.product(other) shouldBe 6L
  }

  it should "publish each key product and skip a miss" in {
    val build = JoinKeyCount.fresh()
    build.addOne(1)
    build.addOne(1)
    build.addOne(-4)
    build.addOne(9)
    val probe = JoinKeyCount.fresh()
    probe.addOne(1)
    probe.add(1, 1L)
    probe.addOne(-4)
    probe.addOne(3)

    val (keys, counts) = JoinKeyCount.productPairs(build, probe)
    keys.zip(counts).toMap shouldBe Map(1 -> 4, -4 -> 1)
    keys.length shouldBe 2
  }

  it should "wrap a per-key product to 32 bits" in {
    val build = JoinKeyCount.fresh()
    build.add(7, Int.MaxValue.toLong)
    val probe = JoinKeyCount.fresh()
    probe.add(7, 2L)

    val (keys, counts) = JoinKeyCount.productPairs(build, probe)
    keys.zip(counts).toMap shouldBe Map(7 -> -2)
  }

  it should "publish nothing when the sides do not overlap" in {
    val build = JoinKeyCount.fresh()
    build.addOne(4)
    val probe = JoinKeyCount.fresh()
    probe.addOne(8)

    val (keys, counts) = JoinKeyCount.productPairs(build, probe)
    keys.length shouldBe 0
    counts.length shouldBe 0
  }
