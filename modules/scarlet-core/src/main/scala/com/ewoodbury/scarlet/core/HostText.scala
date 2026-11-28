package com.ewoodbury.scarlet.core

/**
 * The one host replace from ClickBench 28.
 *
 * The pattern is `^https?://(?:www\.)?([^/]+)/.*$` and the replacement is the captured host. A
 * string that does not match stays as it is. Any other pattern stays out.
 */
object HostText:

  val pattern: String = "^https?://(?:www\\.)?([^/]+)/.*$"
  val replacement: String = "\\1"

  def extract(text: String): String =
    val scheme = schemeEnd(text)
    if scheme == 0 then text
    else
      val body = if text.startsWith("www.", scheme) then scheme + 4 else scheme
      val slash = text.indexOf('/', body)
      if slash <= body then text else text.substring(body, slash)

  private def schemeEnd(text: String): Int =
    if text.startsWith("https://") then 8
    else if text.startsWith("http://") then 7
    else 0
