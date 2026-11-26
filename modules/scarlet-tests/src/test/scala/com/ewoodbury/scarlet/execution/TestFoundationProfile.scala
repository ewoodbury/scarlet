package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The 32-row lock for [[FoundationProfile]].
 *
 * A checksum mismatch or a broken batch rule fails. A slow `to-string` or `flat-map` is `GAP` and
 * does not fail the process. Times are not asserted here.
 */
class TestFoundationProfile extends AnyFlatSpec with Matchers:

  "the foundation profile" should "match checksums and batch rules at 32 rows" in {
    FoundationProfile.check(32)
  }

  it should "keep a slow boundary as GAP and exit 0" in {
    FoundationProfile.judge("to-string", matched = true, ratio = 3.0) shouldBe "GAP"
    FoundationProfile.judge("flat-map", matched = true, ratio = 2.01) shouldBe "GAP"
    FoundationProfile.judge("widen", matched = true, ratio = 2.0) shouldBe "OK"
    FoundationProfile.judge("element-widen", matched = true, ratio = 5.0) shouldBe "OK"
    FoundationProfile.exitCode(Seq("OK", "GAP")) shouldBe 0
  }

  it should "fail when the checksum or the batch rule does not hold" in {
    FoundationProfile.judge("widen", matched = false, ratio = 1.0) shouldBe "FAIL"
    FoundationProfile.exitCode(Seq("FAIL")) shouldBe 1
    FoundationProfile.exitCode(Seq("OK", "FAIL", "GAP")) shouldBe 1
  }
