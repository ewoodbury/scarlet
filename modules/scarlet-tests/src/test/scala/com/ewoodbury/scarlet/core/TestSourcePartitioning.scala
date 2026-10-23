package com.ewoodbury.scarlet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection

class TestSourcePartitioning extends AnyFlatSpec with Matchers {

  private def partitionSizes[A](dc: DistCollection[A]): Seq[Int] = dc.plan match {
    case Plan.Source(partitions, _) => partitions.map(partition => partition.data.size).toSeq
    case other => fail(s"Expected Plan.Source, got $other")
  }

  "DistCollection.apply" should "produce exactly the requested number of balanced partitions" in {
    val dc = DistCollection(Seq(1, 2, 3, 4, 5), 3)

    partitionSizes(dc) shouldEqual Seq(2, 2, 1)
    sourceDescriptor(dc) shouldEqual Plan.SourceDescriptor.Unknown
  }

  it should "produce exactly N empty partitions for empty input" in {
    val dc = DistCollection(Seq.empty[Int], 3)

    partitionSizes(dc) shouldEqual Seq(0, 0, 0)
  }

  it should "produce N partitions when records are fewer than partitions" in {
    val dc = DistCollection(Seq(1, 2), 4)

    partitionSizes(dc) shouldEqual Seq(1, 1, 0, 0)
  }

  it should "distribute evenly divisible input into equal partitions" in {
    val dc = DistCollection(Seq(1, 2, 3, 4), 2)

    partitionSizes(dc) shouldEqual Seq(2, 2)
  }

  it should "preserve input order within partitions and across partitions" in {
    val data = (1 to 100)
    val dc = DistCollection(data, 7)

    dc.collect() shouldEqual data
  }

  it should "place earlier records in earlier partitions" in {
    val dc = DistCollection(Seq(1, 2, 3, 4, 5), 2)

    dc.plan match {
      case Plan.Source(partitions, _) =>
        partitions.head.data.toSeq shouldEqual Seq(1, 2, 3)
        partitions.drop(1).head.data.toSeq shouldEqual Seq(4, 5)
      case other => fail(s"Expected Plan.Source, got $other")
    }
  }

  it should "reject non-positive partition counts" in {
    an[IllegalArgumentException] should be thrownBy DistCollection(Seq(1, 2), 0)
    an[IllegalArgumentException] should be thrownBy DistCollection(Seq(1, 2), -2)
  }

  it should "support execution over empty partitions end to end" in {
    val dc = DistCollection(Seq.empty[(String, Int)], 3)

    dc.collect() shouldEqual Seq.empty[(String, Int)]
    dc.count() shouldEqual 0L
    dc.map(pair => pair._2).collect() shouldEqual Seq.empty[Int]
    dc.filter(pair => pair._2 > 0).collect() shouldEqual Seq.empty[(String, Int)]
    dc.groupByKey.collect() shouldEqual Seq.empty[(String, Iterable[Int])]
  }

  it should "support a single element in many partitions" in {
    val dc = DistCollection(Seq(42), 4)

    partitionSizes(dc) shouldEqual Seq(1, 0, 0, 0)
    dc.collect() shouldEqual Seq(42)
  }

  "DistCollection.ofInts" should "copy contiguous ranges into primitive partitions" in {
    val input = Array(1, 2, 3, 4, 5)
    val dc = DistCollection.ofInts(input, 3)
    input(0) = 99

    partitionSizes(dc) shouldEqual Seq(2, 2, 1)
    sourceDescriptor(dc) shouldEqual Plan.SourceDescriptor.Ints
    dc.collect() shouldEqual Seq(1, 2, 3, 4, 5)
    sourceData(dc).foreach {
      case stored: IntArrays =>
        stored.values should not be theSameInstanceAs(input)
      case other => fail(s"expected IntArrays, got $other")
    }
  }

  it should "produce exactly N empty primitive partitions" in {
    val dc = DistCollection.ofInts(Array.empty[Int], 3)

    partitionSizes(dc) shouldEqual Seq(0, 0, 0)
    dc.collect() shouldEqual Seq.empty[Int]
    dc.sum shouldEqual 0L
    sourceData(dc).foreach {
      case stored: IntArrays => stored.knownSize shouldEqual 0
      case other => fail(s"expected IntArrays, got $other")
    }
  }

  it should "reject a non-positive partition count" in {
    val caught = intercept[IllegalArgumentException] {
      DistCollection.ofInts(Array(1), 0)
    }
    caught.getMessage shouldBe partitionCountMessage
    an[IllegalArgumentException] should be thrownBy DistCollection.ofInts(Array(1), -2)
  }

  "DistCollection.ofDoubles" should "copy contiguous ranges into primitive partitions" in {
    val input = Array(1.5, -2.0, 3.25)
    val dc = DistCollection.ofDoubles(input, 2)
    input(0) = 9.0

    partitionSizes(dc) shouldEqual Seq(2, 1)
    sourceDescriptor(dc) shouldEqual Plan.SourceDescriptor.Doubles
    dc.collect() shouldEqual Seq(1.5, -2.0, 3.25)
    sourceData(dc).foreach {
      case stored: DoubleArrays =>
        stored.values should not be theSameInstanceAs(input)
      case other => fail(s"expected DoubleArrays, got $other")
    }
  }

  it should "produce exactly N empty primitive partitions" in {
    val dc = DistCollection.ofDoubles(Array.empty[Double], 4)

    partitionSizes(dc) shouldEqual Seq(0, 0, 0, 0)
    dc.collect() shouldEqual Seq.empty[Double]
    dc.sum shouldEqual 0.0
    sourceData(dc).foreach {
      case stored: DoubleArrays => stored.knownSize shouldEqual 0
      case other => fail(s"expected DoubleArrays, got $other")
    }
  }

  it should "reject a non-positive partition count" in {
    val caught = intercept[IllegalArgumentException] {
      DistCollection.ofDoubles(Array(1.0), 0)
    }
    caught.getMessage shouldBe partitionCountMessage
    an[IllegalArgumentException] should be thrownBy DistCollection.ofDoubles(Array(1.0), -1)
  }

  "IntArrays" should "iterate copied ints, including an empty array" in {
    val raw = Array(1, 2)
    val owned = IntArrays(raw)
    raw(0) = 9

    owned.toList shouldEqual List(1, 2)
    owned.knownSize shouldEqual 2
    owned.values should not be theSameInstanceAs(raw)
    IntArrays(Array.empty[Int]).toList shouldEqual Nil
    IntArrays(Array.empty[Int]).knownSize shouldEqual 0
  }

  "DoubleArrays" should "iterate copied doubles, including an empty array" in {
    val raw = Array(1.5, -2.0)
    val owned = DoubleArrays(raw)
    raw(0) = 9.0

    owned.toList shouldEqual List(1.5, -2.0)
    owned.knownSize shouldEqual 2
    owned.values should not be theSameInstanceAs(raw)
    DoubleArrays(Array.empty[Double]).toList shouldEqual Nil
    DoubleArrays(Array.empty[Double]).knownSize shouldEqual 0
  }

  "IntPairArrays" should "iterate pairs in order and copy its arrays" in {
    val keys = Array(1, 2)
    val values = Array(10, 20)
    val pairs = IntPairArrays(keys, values)
    keys(0) = 9
    values(1) = 9

    pairs.toList shouldEqual List((1, 10), (2, 20))
    pairs.knownSize shouldEqual 2
    pairs.keys should not be theSameInstanceAs(keys)
    pairs.values should not be theSameInstanceAs(values)
    IntPairArrays(Array.empty[Int], Array.empty[Int]).toList shouldEqual Nil
    val caught = intercept[IllegalArgumentException] {
      IntPairArrays(Array(1), Array.empty[Int])
    }
    caught.getMessage shouldBe lengthMismatchMessage
    val owned = intercept[IllegalArgumentException] {
      IntPairArrays.fromOwned(Array(1), Array(10, 20))
    }
    owned.getMessage shouldBe lengthMismatchMessage
  }

  "DistCollection.ofIntPairs" should "copy key and value ranges and still iterate" in {
    val keys = Array(1, 2, 1, 3, 2)
    val values = Array(10, 20, 5, 7, 1)
    val dc = DistCollection.ofIntPairs(keys, values, 2)
    keys(0) = 0
    values(4) = 0

    partitionSizes(dc) shouldEqual Seq(3, 2)
    sourceDescriptor(dc) shouldEqual Plan.SourceDescriptor.Pairs
    dc.collect() shouldEqual Seq((1, 10), (2, 20), (1, 5), (3, 7), (2, 1))
    sourceData(dc).foreach {
      case stored: IntPairArrays =>
        stored.keys should not be theSameInstanceAs(keys)
        stored.values should not be theSameInstanceAs(values)
      case other => fail(s"expected IntPairArrays, got $other")
    }
  }

  it should "produce exactly N empty pair partitions" in {
    val dc = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 3)

    partitionSizes(dc) shouldEqual Seq(0, 0, 0)
    dc.collect() shouldEqual Seq.empty[(Int, Int)]
    dc.count() shouldEqual 0L
    sourceData(dc).foreach {
      case stored: IntPairArrays => stored.knownSize shouldEqual 0
      case other => fail(s"expected IntPairArrays, got $other")
    }
  }

  it should "reject unequal key and value arrays" in {
    val caught = intercept[IllegalArgumentException] {
      DistCollection.ofIntPairs(Array(1, 2), Array(1), 1)
    }
    caught.getMessage shouldBe lengthMismatchMessage
  }

  it should "reject a longer value array before slicing hides it" in {
    val caught = intercept[IllegalArgumentException] {
      DistCollection.ofIntPairs(Array(1), Array(1, 2), 1)
    }
    caught.getMessage shouldBe lengthMismatchMessage
  }

  it should "reject a non-positive partition count" in {
    val caught = intercept[IllegalArgumentException] {
      DistCollection.ofIntPairs(Array(1), Array(2), 0)
    }
    caught.getMessage shouldBe partitionCountMessage
    an[IllegalArgumentException] should be thrownBy {
      DistCollection.ofIntPairs(Array(1), Array(2), -3)
    }
  }

  private val partitionCountMessage = "requirement failed: Number of partitions must be positive."

  private val lengthMismatchMessage =
    s"requirement failed: ${IntPairArrays.LengthMismatch}"

  private def sourceData[A](dc: DistCollection[A]): Seq[Iterable[A]] = dc.plan match {
    case Plan.Source(partitions, _) => partitions.map(_.data)
    case other => fail(s"Expected Plan.Source, got $other")
  }

  private def sourceDescriptor[A](dc: DistCollection[A]): Plan.SourceDescriptor = dc.plan match {
    case Plan.Source(_, descriptor) => descriptor
    case other => fail(s"Expected Plan.Source, got $other")
  }
}
