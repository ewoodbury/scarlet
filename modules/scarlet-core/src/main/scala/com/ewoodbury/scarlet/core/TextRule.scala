package com.ewoodbury.scarlet.core

/**
 * String order and UTF-16 length for the wide row path and the wide columnar path.
 *
 * Order is JVM `compareTo`. Length is `String.length`. `LIKE` stays [[LikeMatch]]. Equality of a
 * dictionary string stays on its codes.
 */
object TextRule:

  def compare(left: String, right: String): Int = left.compareTo(right)

  def length(text: String): Int = text.length
