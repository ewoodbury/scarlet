package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The 32-row lock for [[FoundationParents]].
 *
 * A checksum mismatch fails. A simulated ratio above 2 is `GAP` and does not by itself fail the
 * 32-row run. Times are not asserted here.
 */
class TestFoundationParents extends AnyFlatSpec with Matchers:

  "the parent split" should "match checksums at 32 rows" in {
    FoundationParents.check(32)
  }

  it should "call a simulated ratio above 2 a GAP" in {
    FoundationParents.judge(2.0) shouldBe "OK"
    FoundationParents.judge(2.01) shouldBe "GAP"
    FoundationParents.exitCode(Seq("OK", "GAP")) shouldBe 0
    FoundationParents.exitCode(Seq("FAIL")) shouldBe 1
  }
