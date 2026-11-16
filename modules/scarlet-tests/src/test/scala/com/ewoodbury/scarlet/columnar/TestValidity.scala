package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestValidity extends AnyFlatSpec with Matchers:

  "countSet" should "count a partial tail word and ignore bits at live and beyond" in {
    val words = Validity.allocate(128)
    Validity.setBit(words, 0)
    Validity.setBit(words, 63)
    Validity.setBit(words, 64)
    Validity.setBit(words, 69)
    Validity.setBit(words, 70)
    Validity.setBit(words, 127)
    Validity.countSet(words, 0) shouldBe 0
    Validity.countSet(words, 64) shouldBe 2
    Validity.countSet(words, 70) shouldBe 4
    Validity.countSet(words, 71) shouldBe 5
  }

  it should "count a full word of set bits" in {
    val validity = Validity.allValid(64)
    Validity.countSet(validity.words, 64) shouldBe 64
    Validity.countSet(validity.words, 63) shouldBe 63
  }
