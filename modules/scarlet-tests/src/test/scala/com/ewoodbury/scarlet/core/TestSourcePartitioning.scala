package com.ewoodbury.scarlet.core

import scala.collection.immutable.ArraySeq

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection

class TestSourcePartitioning extends AnyFlatSpec with Matchers {

  private def partitionSizes[A](dc: DistCollection[A]): Seq[Int] = dc.plan match {
    case Plan.Source(partitions) => partitions.map(partition => partition.data.size).toSeq
    case other => fail(s"Expected Plan.Source, got $other")
  }

  "DistCollection.apply" should "produce exactly the requested number of balanced partitions" in {
    val dc = DistCollection(Seq(1, 2, 3, 4, 5), 3)

    partitionSizes(dc) shouldEqual Seq(2, 2, 1)
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
      case Plan.Source(partitions) =>
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
    dc.collect() shouldEqual Seq(1, 2, 3, 4, 5)
    primitiveArrays(dc).foreach { stored =>
      stored should not be theSameInstanceAs(input)
    }
  }

  it should "produce exactly N empty primitive partitions" in {
    val dc = DistCollection.ofInts(Array.empty[Int], 3)

    partitionSizes(dc) shouldEqual Seq(0, 0, 0)
    dc.collect() shouldEqual Seq.empty[Int]
    dc.sum shouldEqual 0L
  }

  it should "reject a non-positive partition count" in {
    an[IllegalArgumentException] should be thrownBy DistCollection.ofInts(Array(1), 0)
  }

  "DistCollection.ofDoubles" should "copy contiguous ranges into primitive partitions" in {
    val input = Array(1.5, -2.0, 3.25)
    val dc = DistCollection.ofDoubles(input, 2)
    input(0) = 9.0

    partitionSizes(dc) shouldEqual Seq(2, 1)
    dc.collect() shouldEqual Seq(1.5, -2.0, 3.25)
    doubleArrays(dc).foreach { stored =>
      stored should not be theSameInstanceAs(input)
    }
  }

  it should "reject a non-positive partition count" in {
    an[IllegalArgumentException] should be thrownBy DistCollection.ofDoubles(Array(1.0), -1)
  }

  "IntPairArrays" should "iterate pairs in order" in {
    val pairs = IntPairArrays(Array(1, 2), Array(10, 20))

    pairs.toList shouldEqual List((1, 10), (2, 20))
    pairs.knownSize shouldEqual 2
    an[IllegalArgumentException] should be thrownBy IntPairArrays(Array(1), Array.empty[Int])
  }

  "DistCollection.ofIntPairs" should "copy key and value ranges and still iterate" in {
    val keys = Array(1, 2, 1, 3, 2)
    val values = Array(10, 20, 5, 7, 1)
    val dc = DistCollection.ofIntPairs(keys, values, 2)
    keys(0) = 0
    values(4) = 0

    partitionSizes(dc) shouldEqual Seq(3, 2)
    dc.collect() shouldEqual Seq((1, 10), (2, 20), (1, 5), (3, 7), (2, 1))
    pairArrays(dc).foreach { stored =>
      stored.keys should not be theSameInstanceAs(keys)
      stored.values should not be theSameInstanceAs(values)
    }
  }

  it should "reject a non-positive partition count or unequal arrays" in {
    an[IllegalArgumentException] should be thrownBy {
      DistCollection.ofIntPairs(Array(1), Array(1), 0)
    }
    an[IllegalArgumentException] should be thrownBy {
      DistCollection.ofIntPairs(Array(1, 2), Array(1), 1)
    }
  }

  private def primitiveArrays(dc: DistCollection[Int]): Seq[Array[Int]] = dc.plan match {
    case Plan.Source(partitions) =>
      partitions.map {
        case Partition(primitive: ArraySeq.ofInt) => primitive.unsafeArray
        case Partition(other) => fail(s"expected ArraySeq.ofInt, got $other")
      }
    case other => fail(s"Expected Plan.Source, got $other")
  }

  private def doubleArrays(dc: DistCollection[Double]): Seq[Array[Double]] = dc.plan match {
    case Plan.Source(partitions) =>
      partitions.map {
        case Partition(primitive: ArraySeq.ofDouble) => primitive.unsafeArray
        case Partition(other) => fail(s"expected ArraySeq.ofDouble, got $other")
      }
    case other => fail(s"Expected Plan.Source, got $other")
  }

  private def pairArrays(dc: DistCollection[(Int, Int)]): Seq[IntPairArrays] = dc.plan match {
    case Plan.Source(partitions) =>
      partitions.map {
        case Partition(pairs: IntPairArrays) => pairs
        case Partition(other) => fail(s"expected IntPairArrays, got $other")
      }
    case other => fail(s"Expected Plan.Source, got $other")
  }
}
