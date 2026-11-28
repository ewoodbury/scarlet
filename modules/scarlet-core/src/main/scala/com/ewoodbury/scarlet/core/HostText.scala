package com.ewoodbury.scarlet.core

/**
 * The one host replace from ClickBench 28.
 *
 * The pattern is `^https?://(?:www\.)?([^/]+)/.*$` and the replacement is the captured host. The
 * optional `www.` is taken only when a host remains after it. A line break after the slash does
 * not match, and that string stays as it is. Any other pattern stays out.
 */
object HostText:

  val pattern: String = "^https?://(?:www\\.)?([^/]+)/.*$"
  val replacement: String = "\\1"

  def extract(text: String): String =
    val scheme = schemeEnd(text)
    if scheme == 0 then text
    else hostAt(text, scheme).getOrElse(text)

  private def hostAt(text: String, scheme: Int): Option[String] =
    val preferred =
      if text.startsWith("www.", scheme) then capture(text, scheme + 4) else None
    preferred.orElse(capture(text, scheme))

  private def capture(text: String, from: Int): Option[String] =
    val slash = text.indexOf('/', from)
    if slash <= from || text.indexOf('\n', slash + 1) >= 0 then None
    else Some(text.substring(from, slash))

  private def schemeEnd(text: String): Int =
    if text.startsWith("https://") then 8
    else if text.startsWith("http://") then 7
    else 0
