package com.ewoodbury.scarlet.execution

import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal

/**
 * Declared shape of a lambda whose JVM signature is erased.
 *
 * `(Int, (Int, Int)) => (Int, Long)` and a map that stays `(Int, (Int, Int))` are both
 * `Tuple2 => Tuple2`. [[returnsIntLong]] reads the compiler's return constructor: the value handed
 * to `areturn` is `Tuple2.apply` of a boxed `Int` and a boxed `Long`. It does not run the
 * function. A different return stays on the triple map. `(Long, Long) => Long` is `ladd` of the
 * two parameters, or it is not a sum. [[addsLongs]] accepts only that instruction sequence.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.MutableDataStructures",
  ),
)
private[execution] object DeclaredPair:

  def returnsIntLong(function: Any): Boolean =
    try lambda(function).exists(returnsIntLongPair)
    catch case NonFatal(_) => false

  def addsLongs(function: Any): Boolean =
    try lambda(function).exists(addsParameters)
    catch case NonFatal(_) => false

  private final case class Lambda(loader: ClassLoader, owner: String, name: String, desc: String)

  private def lambda(function: Any): Option[Lambda] =
    val cls = function.asInstanceOf[AnyRef].getClass
    val method = cls.getDeclaredMethods.find { candidate =>
      candidate.getName == "writeReplace" && candidate.getParameterCount == 0
    }
    method.map { writer =>
      writer.setAccessible(true)
      val serialized = writer.invoke(function).asInstanceOf[java.lang.invoke.SerializedLambda]
      val loader = Option(cls.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)
      Lambda(
        loader,
        serialized.getImplClass,
        serialized.getImplMethodName,
        serialized.getImplMethodSignature,
      )
    }

  private def returnsIntLongPair(found: Lambda): Boolean =
    found.desc == "(Lscala/Tuple2;)Lscala/Tuple2;" &&
      codeOf(found).exists(code => Returned.intLong(code.bytes, code.pool))

  private def addsParameters(found: Lambda): Boolean =
    found.desc == "(JJ)J" && codeOf(found).exists(code => Returned.longSum(code.bytes))

  private final class Code(val bytes: Array[Byte], val pool: Pool)

  private def codeOf(found: Lambda): Option[Code] =
    val resource = found.owner + ".class"
    val stream = found.loader.getResourceAsStream(resource)
    if (stream == null) then None
    else
      val bytes =
        try stream.readAllBytes()
        finally stream.close()
      ClassFile.code(bytes, found.name, found.desc)

  private sealed trait Cell
  private case object IntCell extends Cell
  private case object LongCell extends Cell
  private case object IntBox extends Cell
  private case object LongBox extends Cell
  private case object Ref extends Cell
  private final case class Tup(left: Cell, right: Cell) extends Cell

  private enum Step:
    case Next(pc: Int)
    case Done(accepted: Boolean)

  private object Returned:

    def intLong(code: Array[Byte], pool: Pool): Boolean =
      val stack = new scala.collection.mutable.ArrayBuffer[Cell]
      var pc = 0
      var steps = 0
      val limit = math.max(code.length * 4, 8)
      var accepted = false
      var stop = false
      while (pc >= 0 && pc < code.length && steps < limit && !stop) {
        steps += 1
        step(code, pc, stack, pool) match
          case Step.Done(result) =>
            accepted = result
            stop = true
          case Step.Next(next) => pc = next
      }
      accepted

    /** `lload` of slot 0, `lload` of slot 2, `ladd`, `lreturn`, in either load order. */
    def longSum(code: Array[Byte]): Boolean =
      loads(code) match
        case Some((0, 2)) | Some((2, 0)) => true
        case _ => false

    private def loads(code: Array[Byte]): Option[(Int, Int)] =
      var pc = 0
      var first = -1
      var second = -1
      var seenAdd = false
      var seenReturn = false
      var ok = true
      while (ok && pc < code.length && !seenReturn) {
        val op = code(pc) & 0xff
        op match
          case 0x1e =>
            if (first < 0) then first = 0 else if (second < 0) then second = 0 else ok = false
            pc += 1
          case 0x20 =>
            if (first < 0) then first = 2 else if (second < 0) then second = 2 else ok = false
            pc += 1
          case 0x16 =>
            val slot = code(pc + 1) & 0xff
            if (first < 0) then first = slot
            else if (second < 0) then second = slot
            else ok = false
            pc += 2
          case 0x61 =>
            seenAdd = first >= 0 && second >= 0
            ok = seenAdd
            pc += 1
          case 0xad =>
            seenReturn = seenAdd
            ok = seenReturn
            pc += 1
          case _ => ok = false
      }
      if (ok && seenReturn && pc == code.length) then Some((first, second)) else None

    private def step(
        code: Array[Byte],
        pc: Int,
        stack: scala.collection.mutable.ArrayBuffer[Cell],
        pool: Pool,
    ): Step =
      val op = code(pc) & 0xff
      op match
        case 0x00 => Step.Next(pc + 1)
        case 0x01 =>
          push(stack, Ref)
          Step.Next(pc + 1)
        case n if n >= 0x02 && n <= 0x08 =>
          push(stack, IntCell)
          Step.Next(pc + 1)
        case 0x10 =>
          push(stack, IntCell)
          Step.Next(pc + 2)
        case 0x11 =>
          push(stack, IntCell)
          Step.Next(pc + 3)
        case 0x15 =>
          push(stack, IntCell)
          Step.Next(pc + 2)
        case 0x19 =>
          push(stack, Ref)
          Step.Next(pc + 2)
        case n if n >= 0x1a && n <= 0x1d =>
          push(stack, IntCell)
          Step.Next(pc + 1)
        case n if n >= 0x2a && n <= 0x2d =>
          push(stack, Ref)
          Step.Next(pc + 1)
        case 0x36 =>
          if (pop(stack).contains(IntCell)) then Step.Next(pc + 2) else Step.Done(false)
        case 0x3a =>
          if (pop(stack).isDefined) then Step.Next(pc + 2) else Step.Done(false)
        case n if n >= 0x3b && n <= 0x3e =>
          if (pop(stack).contains(IntCell)) then Step.Next(pc + 1) else Step.Done(false)
        case n if n >= 0x4b && n <= 0x4e =>
          if (pop(stack).isDefined) then Step.Next(pc + 1) else Step.Done(false)
        case 0x57 =>
          if (pop(stack).isDefined) then Step.Next(pc + 1) else Step.Done(false)
        case 0x60 | 0x64 | 0x68 | 0x6c | 0x70 =>
          if (pop(stack).contains(IntCell) && pop(stack).contains(IntCell)) then
            push(stack, IntCell)
            Step.Next(pc + 1)
          else Step.Done(false)
        case 0x74 =>
          if (pop(stack).contains(IntCell)) then
            push(stack, IntCell)
            Step.Next(pc + 1)
          else Step.Done(false)
        case 0x84 => Step.Next(pc + 3)
        case 0x85 =>
          if (pop(stack).contains(IntCell)) then
            push(stack, LongCell)
            Step.Next(pc + 1)
          else Step.Done(false)
        case 0x99 | 0x9a | 0x9b | 0x9c | 0x9d | 0x9e =>
          if (pop(stack).contains(IntCell)) then Step.Next(pc + 3) else Step.Done(false)
        case n if n >= 0x9f && n <= 0xa4 =>
          if (pop(stack).isDefined && pop(stack).isDefined) then Step.Next(pc + 3)
          else Step.Done(false)
        case 0xa5 | 0xa6 =>
          if (pop(stack).isDefined && pop(stack).isDefined) then Step.Next(pc + 3)
          else Step.Done(false)
        case 0xa7 => Step.Next(pc + jump(code, pc))
        case 0xb0 =>
          pop(stack) match
            case Some(Tup(IntBox, LongBox)) => Step.Done(true)
            case _ => Step.Done(false)
        case 0xb1 => Step.Done(false)
        case 0xbf => Step.Done(false)
        case 0xb2 =>
          pool.field(u2(code, pc + 1)) match
            case Some(desc) =>
              push(stack, cellOf(desc))
              Step.Next(pc + 3)
            case None => Step.Done(false)
        case 0xb4 =>
          if (pop(stack).isEmpty) then Step.Done(false)
          else
            pool.field(u2(code, pc + 1)) match
              case Some(desc) =>
                push(stack, cellOf(desc))
                Step.Next(pc + 3)
              case None => Step.Done(false)
        case 0xb6 | 0xb7 | 0xb8 =>
          invoke(op, u2(code, pc + 1), stack, pool) match
            case Some(_) => Step.Next(pc + 3)
            case None => Step.Done(false)
        case 0xc0 => Step.Next(pc + 3)
        case 0xc6 | 0xc7 =>
          if (pop(stack).isDefined) then Step.Next(pc + 3) else Step.Done(false)
        case _ => Step.Done(false)

    private def invoke(
        op: Int,
        index: Int,
        stack: scala.collection.mutable.ArrayBuffer[Cell],
        pool: Pool,
    ): Option[Unit] =
      pool.method(index).map { member =>
        val (owner, name, desc) = member
        if (name == "boxToInteger") then
          val _ = pop(stack)
          push(stack, IntBox)
        else if (name == "boxToLong") then
          val _ = pop(stack)
          push(stack, LongBox)
        else if (name == "unboxToInt" || name == "unboxToLong") then
          val _ = pop(stack)
          push(stack, if (name == "unboxToInt") then IntCell else LongCell)
        else if (owner == "scala/Tuple2$" && name == "apply") then
          val right = pop(stack).getOrElse(Ref)
          val left = pop(stack).getOrElse(Ref)
          if (op != 0xb8) then
            val _ = pop(stack)
          push(stack, Tup(left, right))
        else genericInvoke(op == 0xb8, desc, stack)
      }

    private def genericInvoke(
        isStatic: Boolean,
        desc: String,
        stack: scala.collection.mutable.ArrayBuffer[Cell],
    ): Unit =
      val (args, ret) = split(desc)
      args.reverse.foreach { _ =>
        val _ = pop(stack)
      }
      if (!isStatic) then
        val _ = pop(stack)
      ret.foreach(cell => push(stack, cell))

    private def split(desc: String): (List[Cell], Option[Cell]) =
      var index = 1
      val args = scala.collection.mutable.ListBuffer.empty[Cell]
      while (index < desc.length && desc.charAt(index) != ')') {
        val (cell, next) = readType(desc, index)
        args += cell
        index = next
      }
      val returned =
        if (index + 1 >= desc.length || desc.charAt(index + 1) == 'V') then None
        else Some(readType(desc, index + 1)._1)
      (args.toList, returned)

    private def readType(desc: String, at: Int): (Cell, Int) =
      desc.charAt(at) match
        case 'B' | 'C' | 'I' | 'S' | 'Z' | 'F' => (IntCell, at + 1)
        case 'J' | 'D' => (LongCell, at + 1)
        case 'L' =>
          val end = desc.indexOf(';', at)
          (Ref, end + 1)
        case '[' =>
          val (_, next) = readType(desc, at + 1)
          (Ref, next)
        case _ => (Ref, at + 1)

    private def cellOf(desc: String): Cell =
      if (desc.isEmpty) then Ref
      else readType(desc, 0)._1

    private def push(stack: scala.collection.mutable.ArrayBuffer[Cell], cell: Cell): Unit =
      val _ = stack.addOne(cell)

    private def pop(stack: scala.collection.mutable.ArrayBuffer[Cell]): Option[Cell] =
      if (stack.isEmpty) then None
      else Some(stack.remove(stack.length - 1))

    private def jump(code: Array[Byte], pc: Int): Int =
      val offset = ((code(pc + 1) & 0xff) << 8) | (code(pc + 2) & 0xff)
      (offset.toShort: Int)

    private def u2(code: Array[Byte], at: Int): Int =
      ((code(at) & 0xff) << 8) | (code(at + 1) & 0xff)
  end Returned

  private sealed trait Entry
  private final case class Utf8(text: String) extends Entry
  private final case class ClassRef(name: Int) extends Entry
  private final case class Nat(name: Int, desc: Int) extends Entry
  private final case class Member(classIndex: Int, nameAndType: Int) extends Entry
  private case object Other extends Entry
  private case object Gap extends Entry

  private final class Pool(entries: Array[Entry]):
    def utfAt(index: Int): Option[String] =
      if (index < 0 || index >= entries.length) then None
      else
        entries(index) match
          case Utf8(text) => Some(text)
          case _ => None

    def method(index: Int): Option[(String, String, String)] = member(index)
    def field(index: Int): Option[String] =
      member(index).map { case (_, _, desc) => desc }

    private def member(index: Int): Option[(String, String, String)] =
      if (index < 0 || index >= entries.length) then None
      else
        entries(index) match
          case Member(classIndex, nat) =>
            (className(classIndex), natName(nat)) match
              case (Some(owner), Some((name, desc))) => Some((owner, name, desc))
              case _ => None
          case _ => None

    private def className(index: Int): Option[String] = entries(index) match
      case ClassRef(name) => utf(name)
      case _ => None

    private def natName(index: Int): Option[(String, String)] = entries(index) match
      case Nat(name, desc) =>
        (utf(name), utf(desc)) match
          case (Some(method), Some(descriptor)) => Some((method, descriptor))
          case _ => None
      case _ => None

    private def utf(index: Int): Option[String] = utfAt(index)

  private object ClassFile:
    def code(bytes: Array[Byte], name: String, desc: String): Option[Code] =
      val in = new DataInputStream(new ByteArrayInputStream(bytes))
      val _ = in.readInt()
      val _ = in.readUnsignedShort()
      val _ = in.readUnsignedShort()
      val pool = readPool(in)
      val _ = in.readUnsignedShort()
      val _ = in.readUnsignedShort()
      val _ = in.readUnsignedShort()
      skipShorts(in, in.readUnsignedShort())
      skipMembers(in)
      methodCode(in, pool, name, desc)

    private def readPool(in: DataInputStream): Pool =
      val count = in.readUnsignedShort()
      val entries = Array.fill[Entry](count)(Gap)
      var index = 1
      while (index < count) {
        val tag = in.readUnsignedByte()
        tag match
          case 1 =>
            val length = in.readUnsignedShort()
            val raw = new Array[Byte](length)
            in.readFully(raw)
            entries(index) = Utf8(new String(raw, StandardCharsets.UTF_8))
            index += 1
          case 7 =>
            entries(index) = ClassRef(in.readUnsignedShort())
            index += 1
          case 3 | 4 =>
            val _ = in.readInt()
            entries(index) = Other
            index += 1
          case 5 | 6 =>
            val _ = in.readLong()
            entries(index) = Other
            index += 2
          case 8 | 16 | 19 | 20 =>
            val _ = in.readUnsignedShort()
            entries(index) = Other
            index += 1
          case 9 | 10 | 11 =>
            entries(index) = Member(in.readUnsignedShort(), in.readUnsignedShort())
            index += 1
          case 12 =>
            entries(index) = Nat(in.readUnsignedShort(), in.readUnsignedShort())
            index += 1
          case 15 =>
            val _ = in.readUnsignedByte()
            val _ = in.readUnsignedShort()
            entries(index) = Other
            index += 1
          case 17 | 18 =>
            val _ = in.readUnsignedShort()
            val _ = in.readUnsignedShort()
            entries(index) = Other
            index += 1
          case _ =>
            throw new IllegalArgumentException(s"unknown constant pool tag $tag")
      }
      new Pool(entries)

    private def skipMembers(in: DataInputStream): Unit =
      val count = in.readUnsignedShort()
      var index = 0
      while (index < count) {
        val _ = in.readUnsignedShort()
        val _ = in.readUnsignedShort()
        val _ = in.readUnsignedShort()
        skipAttributes(in)
        index += 1
      }

    private def methodCode(
        in: DataInputStream,
        pool: Pool,
        name: String,
        desc: String,
    ): Option[Code] =
      val count = in.readUnsignedShort()
      var index = 0
      var found: Option[Code] = None
      while (index < count) {
        val _ = in.readUnsignedShort()
        val methodName = in.readUnsignedShort()
        val methodDesc = in.readUnsignedShort()
        val attributes = in.readUnsignedShort()
        var attr = 0
        while (attr < attributes) {
          val attrName = in.readUnsignedShort()
          val length = in.readInt()
          val body = in.readNBytes(length)
          val matched =
            found.isEmpty &&
              pool.utfAt(methodName).contains(name) &&
              pool.utfAt(methodDesc).contains(desc) &&
              pool.utfAt(attrName).contains("Code") &&
              body.length >= 8
          if (matched) then found = Some(readCode(body, pool))
          attr += 1
        }
        index += 1
      }
      found

    private def readCode(body: Array[Byte], pool: Pool): Code =
      val in = new DataInputStream(new ByteArrayInputStream(body))
      val _ = in.readUnsignedShort()
      val _ = in.readUnsignedShort()
      val length = in.readInt()
      val bytes = in.readNBytes(length)
      new Code(bytes, pool)

    private def skipAttributes(in: DataInputStream): Unit =
      val count = in.readUnsignedShort()
      var index = 0
      while (index < count) {
        val _ = in.readUnsignedShort()
        val length = in.readInt()
        val _ = in.readNBytes(length)
        index += 1
      }

    private def skipShorts(in: DataInputStream, count: Int): Unit =
      var index = 0
      while (index < count) {
        val _ = in.readUnsignedShort()
        index += 1
      }
  end ClassFile
