package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * Kind of a [[Plan.Source]] that lowers to [[PhysicalOp.Scan]].
 *
 * A descriptor and the rows have to agree. Empty partitions use the descriptor. An owned array is
 * type-safe and is not scanned again. A leading null can only witness a string. A leading null
 * component does not witness int pairs or long pairs.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
private[execution] object ScanLowering:

  def classify(source: Plan.Source[_]): Option[PhysicalOp.Kind] =
    val inferred = classifyData(source.partitions)
    source.descriptor match
      case Plan.SourceDescriptor.Unknown => inferred
      case descriptor =>
        val describedKind = kindOf(descriptor)
        inferred match
          case Some(kind)
              if describedKind.contains(kind) && dataMatchesDescriptor(source, descriptor) =>
            Some(kind)
          case None if source.partitions.forall(_.data.isEmpty) => describedKind
          case _ => None

  private def kindOf(descriptor: Plan.SourceDescriptor): Option[PhysicalOp.Kind] = descriptor match
    // Boundary guard: classify returns on Unknown before this match.
    case Plan.SourceDescriptor.Unknown => None
    case Plan.SourceDescriptor.Ints => Some(PhysicalOp.Kind.Ints)
    case Plan.SourceDescriptor.Longs => Some(PhysicalOp.Kind.Longs)
    case Plan.SourceDescriptor.Doubles => Some(PhysicalOp.Kind.Floats)
    case Plan.SourceDescriptor.Pairs => Some(PhysicalOp.Kind.Pairs)
    case Plan.SourceDescriptor.Strings => Some(PhysicalOp.Kind.Strings)
    case Plan.SourceDescriptor.Bools => Some(PhysicalOp.Kind.Bools)
    case Plan.SourceDescriptor.LongPairs => Some(PhysicalOp.Kind.LongPairs)

  private def dataMatchesDescriptor(
      source: Plan.Source[_],
      descriptor: Plan.SourceDescriptor,
  ): Boolean =
    if (ownedMatches(source, descriptor)) then true
    else
      val rows = source.partitions.iterator.flatMap(_.data.iterator)
      descriptor match
        case Plan.SourceDescriptor.Ints => rows.forall(isIntValue)
        case Plan.SourceDescriptor.Longs => rows.forall(isLongValue)
        case Plan.SourceDescriptor.Doubles => rows.forall(isDoubleValue)
        case Plan.SourceDescriptor.Pairs => rows.forall(isIntPairValue)
        case Plan.SourceDescriptor.Strings => rows.forall(isStringValue)
        case Plan.SourceDescriptor.Bools => rows.forall(isBoolValue)
        case Plan.SourceDescriptor.LongPairs => rows.forall(isLongPairValue)
        // Boundary guard: classify does not call this with Unknown.
        case Plan.SourceDescriptor.Unknown => false

  /**
   * Engine-built array holders are type-safe by construction. Skipping them avoids a second scan
   * that would box every element before encode.
   */
  private def ownedMatches(source: Plan.Source[_], descriptor: Plan.SourceDescriptor): Boolean =
    import com.ewoodbury.scarlet.core.{
      BoolArrays,
      DoubleArrays,
      IntArrays,
      IntPairArrays,
      LongArrays,
      LongPairArrays,
      StringArrays,
    }

    descriptor match
      case Plan.SourceDescriptor.Ints =>
        source.partitions.forall(partition => partition.data.isInstanceOf[IntArrays])
      case Plan.SourceDescriptor.Longs =>
        source.partitions.forall(partition => partition.data.isInstanceOf[LongArrays])
      case Plan.SourceDescriptor.Doubles =>
        source.partitions.forall(partition => partition.data.isInstanceOf[DoubleArrays])
      case Plan.SourceDescriptor.Pairs =>
        source.partitions.forall(partition => partition.data.isInstanceOf[IntPairArrays])
      case Plan.SourceDescriptor.Bools =>
        source.partitions.forall(partition => partition.data.isInstanceOf[BoolArrays])
      case Plan.SourceDescriptor.Strings =>
        source.partitions.forall(partition => partition.data.isInstanceOf[StringArrays])
      case Plan.SourceDescriptor.LongPairs =>
        source.partitions.forall(partition => partition.data.isInstanceOf[LongPairArrays])
      // Boundary guard: classify does not call this with Unknown.
      case Plan.SourceDescriptor.Unknown => false

  private def isIntValue(value: Any): Boolean = value match
    case _: Int => true
    case _ => false

  private def isLongValue(value: Any): Boolean = value match
    case _: Long => true
    case _ => false

  private def isDoubleValue(value: Any): Boolean = value match
    case _: Double => true
    case _ => false

  private def isBoolValue(value: Any): Boolean = value match
    case _: Boolean => true
    case _ => false

  private def isIntPairValue(value: Any): Boolean = value match
    case (_: Int, _: Int) => true
    case _ => false

  private def isLongPairValue(value: Any): Boolean = value match
    case (_: Long, _: Long) => true
    case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isStringValue(value: Any): Boolean = value match
    case null => true
    case _: String => true
    case _ => false

  private def classifyData(partitions: Seq[Partition[_]]): Option[PhysicalOp.Kind] =
    if (partitions.isEmpty) then None
    else
      val values = partitions.iterator.flatMap(_.data.iterator)
      values.nextOption().flatMap(value => classifyValue(value, values))

  /**
   * The first present value selects the kind. A leading empty cell can only witness a string. A
   * pair witness is two `Int` cells or two `Long` cells. A `(String, Int)` cell witnesses string
   * pairs, including a null int. A leading null key keeps looking until a present string key. A
   * leading null component does not witness [[PhysicalOp.Kind.Pairs]] or
   * [[PhysicalOp.Kind.LongPairs]]. A later null on a witnessed boxed int pair stays absent.
   * Grouped sum also accepts a leading null; see [[ColumnarPlanner.groupedPairSource]].
   */
  private def classifyValue(value: Any, rest: Iterator[Any]): Option[PhysicalOp.Kind] =
    Option(value) match
      case Some(_: Int) => Some(PhysicalOp.Kind.Ints)
      case Some(_: Long) => Some(PhysicalOp.Kind.Longs)
      case Some(_: Double) => Some(PhysicalOp.Kind.Floats)
      case Some((_: Int, _: Int)) => Some(PhysicalOp.Kind.Pairs)
      case Some((_: Long, _: Long)) => Some(PhysicalOp.Kind.LongPairs)
      case Some(value) if StringPairPlan.presentKey(value) => StringPairPlan.confirm(rest)
      case Some(value) if StringPairPlan.nullKey(value) => StringPairPlan.witness(rest)
      case Some(_: Boolean) => Some(PhysicalOp.Kind.Bools)
      case Some(_: String) => Some(PhysicalOp.Kind.Strings)
      case Some(_) => None
      case None => stringWitness(rest)

  @tailrec
  private def stringWitness(values: Iterator[Any]): Option[PhysicalOp.Kind] =
    values.nextOption() match
      case None => None
      case Some(value) =>
        Option(value) match
          case None => stringWitness(values)
          case Some(_: String) => Some(PhysicalOp.Kind.Strings)
          case Some(_) => None
