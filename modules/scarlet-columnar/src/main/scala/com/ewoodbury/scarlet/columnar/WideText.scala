package com.ewoodbury.scarlet.columnar

/**
 * UTF-16 length of a string column.
 *
 * A null cell stays null. An empty string has length zero. `"é"` has length 1 and `"e\u0301"` has
 * length 2.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object WideText:

  def length(column: Column, count: Int): Int32Column = column match
    case texts: DictUtf8Column =>
      val values = new Array[Int](count)
      val present = new Array[Boolean](count)
      var row = 0
      while row < count do
        if texts.isValid(row) then
          present(row) = true
          values(row) = texts.dictionary(texts.codes(row)).length
        row += 1
      Int32Column.of(values, Validity.pack(count, at => present(at)), count)
    case other =>
      throw new IllegalArgumentException(s"length of ${other.logicalType} is not supported")
