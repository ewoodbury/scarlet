package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.Plan

/**
 * Names every current source descriptor and physical kind. An inline match is checked where it is
 * expanded, so adding an enum case does not fail this file unless a call site passes that case.
 * A new case fails the build through the `values.length` assertions below and through
 * `-Wconf:name=PatternMatchExhaustivity:e` on the matches that receive the value.
 */
class TestColumnarKinds extends AnyFlatSpec with Matchers:

  "descriptors and kinds" should "be named by an exhaustive match" in {
    descriptorOrdinal(Plan.SourceDescriptor.Unknown) shouldBe 0
    descriptorOrdinal(Plan.SourceDescriptor.Ints) shouldBe 1
    descriptorOrdinal(Plan.SourceDescriptor.Longs) shouldBe 2
    descriptorOrdinal(Plan.SourceDescriptor.Doubles) shouldBe 3
    descriptorOrdinal(Plan.SourceDescriptor.Pairs) shouldBe 4
    descriptorOrdinal(Plan.SourceDescriptor.Strings) shouldBe 5
    descriptorOrdinal(Plan.SourceDescriptor.Bools) shouldBe 6
    kindOrdinal(PhysicalOp.Kind.Ints) shouldBe 0
    kindOrdinal(PhysicalOp.Kind.Longs) shouldBe 1
    kindOrdinal(PhysicalOp.Kind.Floats) shouldBe 2
    kindOrdinal(PhysicalOp.Kind.Pairs) shouldBe 3
    kindOrdinal(PhysicalOp.Kind.Triples) shouldBe 4
    kindOrdinal(PhysicalOp.Kind.Strings) shouldBe 5
    kindOrdinal(PhysicalOp.Kind.Bools) shouldBe 6
    Plan.SourceDescriptor.values.length shouldBe 7
    PhysicalOp.Kind.values.length shouldBe 7
  }

inline def descriptorOrdinal(inline descriptor: Plan.SourceDescriptor): Int =
  inline descriptor match
    case Plan.SourceDescriptor.Unknown => 0
    case Plan.SourceDescriptor.Ints => 1
    case Plan.SourceDescriptor.Longs => 2
    case Plan.SourceDescriptor.Doubles => 3
    case Plan.SourceDescriptor.Pairs => 4
    case Plan.SourceDescriptor.Strings => 5
    case Plan.SourceDescriptor.Bools => 6

inline def kindOrdinal(inline kind: PhysicalOp.Kind): Int =
  inline kind match
    case PhysicalOp.Kind.Ints => 0
    case PhysicalOp.Kind.Longs => 1
    case PhysicalOp.Kind.Floats => 2
    case PhysicalOp.Kind.Pairs => 3
    case PhysicalOp.Kind.Triples => 4
    case PhysicalOp.Kind.Strings => 5
    case PhysicalOp.Kind.Bools => 6
