package com.ewoodbury.scarlet.execution

import scala.collection.mutable

/**
 * Order-independent summary of an inner-join result.
 *
 * Rows are grouped by key, null included. The checksum is the sum of a per-row mix of
 * `(key, leftValue, rightValue)`, so two results with the same pairs in a different order agree,
 * and a value change at one row does not. Callers compare [[Digest]] values, not raw order.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.MutableDataStructures",
  ),
)
private[execution] object JoinDigest:

  /** One key's pair count. `None` is the null key. */
  final case class KeyCount(key: Option[Int], count: Long)

  final case class Digest(checksum: Long, total: Long, counts: Vector[KeyCount])

  val empty: Digest = Digest(0L, 0L, Vector.empty)

  /** Null keys stay null. A null value, or any other cell, fails the run. */
  def from(rows: Iterable[(Int, (Int, Int))]): Digest =
    val counts = mutable.HashMap.empty[Option[Int], Long]
    var checksum = 0L
    var total = 0L
    rows.foreach { row =>
      val (key, left, right) = read(row)
      total += 1L
      checksum += mix(key, left, right)
      val count = counts.getOrElse(key, 0L) + 1L
      counts.update(key, count)
    }
    Digest(checksum, total, ordered(counts))

  def agrees(left: Digest, right: Digest): Boolean =
    left.checksum == right.checksum && left.total == right.total && sameCounts(left.counts, right.counts)

  /** First disagreement, for a failed run. `equal` when [[agrees]] is true. */
  def difference(left: Digest, right: Digest): String =
    if (left.total != right.total) then s"pairs ${left.total} != ${right.total}"
    else
      firstCountGap(left.counts, right.counts).getOrElse {
        if (left.checksum != right.checksum) then
          s"checksum ${left.checksum} != ${right.checksum}"
        else "equal"
      }

  /**
   * Reads one join row without unboxing a null key. The tuple fields are erased references: a
   * present int is boxed, and a null key stays null. `productElement` is the read that keeps the
   * null visible.
   */
  private def read(row: (Int, (Int, Int))): (Option[Int], Int, Int) =
    val key = readKey(row.productElement(0))
    val nested = row.productElement(1) match
      case pair: Product => pair
      case _ => throw new IllegalStateException("join value was not a pair")
    (key, readPresent("left", nested.productElement(0)), readPresent("right", nested.productElement(1)))

  private def readKey(cell: Any): Option[Int] = cell match
    case null => None
    case number: Int => Some(number)
    case _ => throw new IllegalStateException("join key was not an int")

  private def readPresent(slot: String, cell: Any): Int = cell match
    case number: Int => number
    case null => throw new IllegalStateException(s"null join $slot value")
    case _ => throw new IllegalStateException(s"join $slot value was not an int")

  /** Wrapping sum of this mix is independent of row order. Left and right use different factors. */
  private def mix(key: Option[Int], left: Int, right: Int): Long =
    val mixedKey = key match
      case Some(present) => present.toLong
      case None => nullKeyBits
    var hash = mixedKey * keyFactor
    hash ^= left.toLong * leftFactor
    hash ^= right.toLong * rightFactor
    hash

  private def ordered(counts: mutable.HashMap[Option[Int], Long]): Vector[KeyCount] =
    counts.toVector
      .map { case (key, count) => KeyCount(key, count) }
      .sortWith(before)

  private def before(left: KeyCount, right: KeyCount): Boolean = (left.key, right.key) match
    case (None, Some(_)) => true
    case (Some(_), None) => false
    case (Some(leftKey), Some(rightKey)) => leftKey < rightKey
    case (None, None) => false

  private def firstCountGap(left: Vector[KeyCount], right: Vector[KeyCount]): Option[String] =
    val keys = (left.map(_.key) ++ right.map(_.key)).distinct.sortWith(keyBefore)
    keys.iterator.map { key =>
      val leftCount = countAt(left, key)
      val rightCount = countAt(right, key)
      if (leftCount == rightCount) then None
      else Some(s"key ${showKey(key)} pairs $leftCount != $rightCount")
    }.collectFirst { case Some(text) => text }

  private def keyBefore(left: Option[Int], right: Option[Int]): Boolean = (left, right) match
    case (None, Some(_)) => true
    case (Some(_), None) => false
    case (Some(leftKey), Some(rightKey)) => leftKey < rightKey
    case (None, None) => false

  private def countAt(counts: Vector[KeyCount], key: Option[Int]): Long =
    counts.find(count => sameKey(count.key, key)).fold(0L)(_.count)

  private def sameCounts(left: Vector[KeyCount], right: Vector[KeyCount]): Boolean =
    left.length == right.length && left.zip(right).forall { case (leftCount, rightCount) =>
      sameKey(leftCount.key, rightCount.key) && leftCount.count == rightCount.count
    }

  private def sameKey(left: Option[Int], right: Option[Int]): Boolean = (left, right) match
    case (Some(leftKey), Some(rightKey)) => leftKey == rightKey
    case (None, None) => true
    case _ => false

  private def showKey(key: Option[Int]): String = key match
    case Some(present) => present.toString
    case None => "null"

  private val keyFactor: Long = 0x9e3779b97f4a7c15L
  private val leftFactor: Long = 0xbf58476d1ce4e5b9L
  private val rightFactor: Long = 0x94d049bb133111ebL

  /** Not a sign-extended int, so a null key does not collide with a present key in [[mix]]. */
  private val nullKeyBits: Long = 0x5a5a5a5a5a5a5a5aL
