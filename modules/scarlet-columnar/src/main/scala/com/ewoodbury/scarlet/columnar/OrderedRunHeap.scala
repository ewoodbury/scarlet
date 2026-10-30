package com.ewoodbury.scarlet.columnar

/**
 * Min-heap of run ids. Order is `keys(id)`, then the smaller id.
 *
 * The caller owns `keys` and writes `keys(id)` before each [[push]]. That id keeps the key until
 * [[pop]]. The sift compares those ints. A present key and an effective key (a null sorts as zero)
 * are both just a value in the array, so the merge loop grows no branch and boxes nothing.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] final class OrderedRunHeap(keys: Array[Int]):

  private val nodes = new Array[Int](keys.length)
  private var size: Int = 0

  def isEmpty: Boolean = size == 0

  def pop(): Int =
    val chosen = nodes(0)
    size -= 1
    if (size > 0) then
      nodes(0) = nodes(size)
      siftDown(0)
    chosen

  /** `keys(id)` is already the sort key. Leave it unchanged until [[pop]] returns this id. */
  def push(id: Int): Unit =
    nodes(size) = id
    siftUp(size)
    size += 1

  private def less(leftId: Int, rightId: Int): Boolean =
    val order = Integer.compare(keys(leftId), keys(rightId))
    order < 0 || (order == 0 && leftId < rightId)

  private def siftUp(start: Int): Unit =
    var at = start
    var moved = true
    while (moved && at > 0) {
      val parent = (at - 1) >>> 1
      if (less(nodes(at), nodes(parent))) then
        swap(at, parent)
        at = parent
      else moved = false
    }

  private def siftDown(start: Int): Unit =
    var at = start
    var moved = true
    while (moved) {
      val left = at * 2 + 1
      val right = left + 1
      var best = at
      if (left < size && less(nodes(left), nodes(best))) then best = left
      if (right < size && less(nodes(right), nodes(best))) then best = right
      if (best == at) then moved = false
      else
        swap(at, best)
        at = best
    }

  private def swap(left: Int, right: Int): Unit =
    val held = nodes(left)
    nodes(left) = nodes(right)
    nodes(right) = held
