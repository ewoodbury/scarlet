package com.ewoodbury.scarlet.columnar

/** Which side of an outer join may be absent. Absence is a validity bit, not a sentinel. */
enum OuterKind:
  case Left, Right, Full
