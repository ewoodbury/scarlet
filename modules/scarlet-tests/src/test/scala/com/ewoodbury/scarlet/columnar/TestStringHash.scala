package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.runtime.local.HashPartitioner

/** `String.hashCode` contract shared with the row `HashPartitioner`. */
class TestStringHash extends AnyFlatSpec with Matchers:

  private val partitioner = new HashPartitioner
  private val accent = "é"
  private val emoji = "😀"

  "StringHash" should "match HashPartitioner for null, empty, accented, and emoji keys" in {
    agrees(nullKey, 1)
    agrees(nullKey, 8)
    agrees("", 3)
    agrees("", 8)
    agrees(accent, 4)
    agrees(accent, 8)
    agrees(emoji, 5)
    agrees(emoji, 8)
    StringHash.hash(nullKey) shouldBe StringHash.Null
    StringHash.hash("") shouldBe 0
    StringHash.Null shouldBe 0
    StringHash.hash(accent) shouldBe accent.hashCode()
    StringHash.hash(emoji) shouldBe emoji.hashCode()
  }

  it should "hash each dictionary string once and never the code" in {
    val dictionary = Array("", accent, emoji, "red")
    val hashed = StringHash.hashDictionary(dictionary)

    hashed.length shouldBe dictionary.length
    hashed shouldBe dictionary.map(text => StringHash.hash(text))
    hashed.zipWithIndex.exists { (codeHash, code) => codeHash != code } shouldBe true
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey: String = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def agrees(key: String, partitions: Int): Unit =
    val code = StringHash.hash(key)
    Math.floorMod(code, partitions) shouldBe partitioner.partition(key, partitions)
