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
