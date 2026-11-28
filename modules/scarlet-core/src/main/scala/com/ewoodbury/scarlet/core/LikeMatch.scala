package com.ewoodbury.scarlet.core

/**
 * SQL `LIKE` against one JVM string.
 *
 * `%` is any sequence, including empty. `_` is one UTF-16 code unit. There is no escape. Matching
 * is case sensitive. `"é"` and `"e\u0301"` do not match each other.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object LikeMatch:

  def matches(text: String, pattern: String): Boolean =
    val textLength = text.length
    val patternLength = pattern.length
    var textAt = 0
    var patternAt = 0
    var star = -1
    var mark = -1
    var failed = false
    while textAt < textLength && !failed do
      if patternAt < patternLength && one(pattern.charAt(patternAt), text.charAt(textAt)) then
        textAt += 1
        patternAt += 1
      else if patternAt < patternLength && pattern.charAt(patternAt) == '%' then
        star = patternAt
        mark = textAt
        patternAt += 1
      else if star >= 0 then
        patternAt = star + 1
        mark += 1
        textAt = mark
      else failed = true
    if failed then false
    else
      while patternAt < patternLength && pattern.charAt(patternAt) == '%' do patternAt += 1
      patternAt == patternLength

  private def one(pattern: Char, text: Char): Boolean =
    pattern == '_' || pattern == text
