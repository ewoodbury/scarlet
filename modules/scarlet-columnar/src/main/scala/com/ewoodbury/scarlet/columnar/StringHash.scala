package com.ewoodbury.scarlet.columnar

/**
 * Hash of a string key, matching the row path.
 *
 * A null key hashes to [[Null]], which is `0`. Every other key uses `String.hashCode`, so the
 * UTF-16 code units are what the row `HashPartitioner` uses. `""` also hashes to `0` and is not
 * null. A dictionary id is not a hash: hash each dictionary string once.
 */
private[scarlet] object StringHash:

  /** Hash of a null key. The same constant the row partitioner uses. */
  val Null: Int = 0

  /** `String.hashCode` for a present string, and [[Null]] for a null key. */
  def hash(text: String): Int =
    if (BatchCodec.isAbsent(text)) Null else text.hashCode()

  /**
   * One hash per dictionary string, in dictionary order. Index `code` is the hash of that string,
   * not the code.
   */
  def hashDictionary(dictionary: Array[String]): Array[Int] =
    Array.tabulate(dictionary.length)(code => hash(dictionary(code)))
