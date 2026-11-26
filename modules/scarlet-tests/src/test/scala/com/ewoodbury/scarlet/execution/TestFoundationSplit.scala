package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The 32-row lock for [[FoundationSplit]].
 *
 * A checksum mismatch fails. A simulated ratio above 2 is `GAP` and does not by itself fail the
 * 32-row run. Times are not asserted here.
 */
class TestFoundationSplit extends AnyFlatSpec with Matchers:

  "the boundary split" should "match checksums at 32 rows" in {
    FoundationSplit.check(32)
  }

  it should "call a simulated ratio above 2 a GAP" in {
    FoundationSplit.judge(2.0) shouldBe "OK"
    FoundationSplit.judge(2.01) shouldBe "GAP"
    FoundationSplit.exitCode(Seq("OK", "GAP")) shouldBe 0
    FoundationSplit.exitCode(Seq("FAIL")) shouldBe 1
  }
