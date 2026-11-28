package com.ewoodbury.scarlet.support

/** Replacements for the operators WartRemover rejects in tests. */
object Check extends org.scalatest.matchers.should.Matchers:

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  def same[A](left: A, right: A): Boolean =
    left == right

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  def ref(left: AnyRef, right: AnyRef): Boolean =
    left eq right

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  def isNull[A](value: A): Boolean =
    value == null

  /** `None` with the element type taken from the receiver, so Nothing is not inferred. */
  extension [A](value: Option[A])
    def shouldBeEmpty: org.scalatest.compatible.Assertion =
      value shouldBe Option.empty[A]

  /** An erased cell compared with `None`. `productElement` types the cell as `Any`. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Nothing",
    ),
  )
  def shouldBeNone(value: Any): org.scalatest.compatible.Assertion =
    value shouldBe None

  def at[A](values: Seq[A], index: Int): A =
    values.lift(index) match
      case Some(value) => value
      case None => throw new IllegalArgumentException(s"missing index $index")

  def first[A](values: Iterable[A]): A =
    val iterator = values.iterator
    if iterator.hasNext then iterator.next()
    else throw new NoSuchElementException("empty")

  def last[A](values: Iterable[A]): A =
    val iterator = values.iterator
    if !iterator.hasNext then throw new NoSuchElementException("empty")
    val seed = iterator.next()
    iterator.foldLeft(seed)((_, value) => value)

  def maximum[A](values: Iterable[A])(using Ordering[A]): A =
    val iterator = values.iterator
    if !iterator.hasNext then throw new NoSuchElementException("empty maximum")
    val seed = iterator.next()
    iterator.foldLeft(seed) { (best, value) =>
      if Ordering[A].gt(value, best) then value else best
    }

  def maximumBy[A, B](values: Iterable[A])(score: A => B)(using Ordering[B]): A =
    val iterator = values.iterator
    if !iterator.hasNext then throw new NoSuchElementException("empty maximum")
    val seed = iterator.next()
    val seedScore = score(seed)
    iterator
      .foldLeft((seed, seedScore)) { case ((best, bestScore), value) =>
        val valueScore = score(value)
        if Ordering[B].gt(valueScore, bestScore) then (value, valueScore) else (best, bestScore)
      }
      ._1

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.ToString",
    ),
  )
  def text(value: Any): String =
    if value == null then "null" else value.toString
