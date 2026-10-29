package com.ewoodbury.scarlet.execution

/**
 * Declared-apply witness for an element function.
 *
 * [[ColumnarPlanner]] uses [[declaredElement]] when a project or `mapValues` must declare the
 * column's signature. `Int => Int` is `apply$mcII$sp`, `Long => Long` is `apply$mcJJ$sp`,
 * `Double => Double` is `apply$mcDD$sp`, and `String => String` is `apply`. The check reads method
 * descriptors and does not call the function. A primitive specialized apply is not a bridge.
 * `Boolean` has no specialized apply, so [[declaredElement]] is false for that kind and does not
 * inspect the function.
 */
private[execution] object FunctionWitness:

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def declaredElement(kind: PhysicalOp.Kind, function: Any): Boolean = kind match
    case PhysicalOp.Kind.Ints =>
      declaredSignature(function, "apply$mcII$sp", classOf[Int], classOf[Int], allowBridge = false)
    case PhysicalOp.Kind.Longs =>
      declaredSignature(
        function,
        "apply$mcJJ$sp",
        classOf[Long],
        classOf[Long],
        allowBridge = false,
      )
    case PhysicalOp.Kind.Floats =>
      declaredSignature(
        function,
        "apply$mcDD$sp",
        classOf[Double],
        classOf[Double],
        allowBridge = false,
      )
    case PhysicalOp.Kind.Strings =>
      declaredSignature(function, "apply", classOf[String], classOf[String], allowBridge = true)
    case PhysicalOp.Kind.Bools | PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongTriples =>
      false

  /**
   * True when `function` declares `apply` or `specialized` from `argument` to `result`, and every
   * other one-argument `apply` is an erasure bridge. A hand-written `apply(Any): Any` beside that
   * method is not a bridge, so the plan stays on the row path. `getDeclaredMethods` ignores an
   * inherited `apply`, which also stays on the row path. This does not call `function`.
   *
   * `allowBridge` is for `String => String`. A LambdaMetafactory spin is a hidden class whose
   * `apply(String): String` and `apply(Object): Object` are both ordinary methods, not bridges.
   * The erased method counts only as an erasure bridge. A bridge with the declared string
   * signature still counts. A primitive specialized apply is the implementation, so a bridge does
   * not satisfy it.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def declaredSignature(
      function: Any,
      specialized: String,
      argument: Class[_],
      result: Class[_],
      allowBridge: Boolean,
  ): Boolean =
    val methods = function.getClass.getDeclaredMethods
    def matches(method: java.lang.reflect.Method): Boolean =
      isDeclaredApply(method, specialized, argument, result, allowBridge)

    methods.exists(matches) &&
    !methods.exists(method =>
      isDisqualifyingApply(method, specialized, argument, result, allowBridge),
    )

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def isDeclaredApply(
      method: java.lang.reflect.Method,
      specialized: String,
      argument: Class[_],
      result: Class[_],
      allowBridge: Boolean,
  ): Boolean =
    val named = method.getName == "apply" || method.getName == specialized
    val implemented = allowBridge || !method.isBridge
    named &&
    implemented &&
    method.getParameterCount == 1 &&
    method.getReturnType.eq(result) &&
    method.getParameterTypes()(0).eq(argument)

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def isDisqualifyingApply(
      method: java.lang.reflect.Method,
      specialized: String,
      argument: Class[_],
      result: Class[_],
      allowBridge: Boolean,
  ): Boolean =
    method.getName == "apply" &&
      method.getParameterCount == 1 &&
      !isDeclaredApply(method, specialized, argument, result, allowBridge) &&
      !isErasureBridge(method)

  /**
   * Compiler bridge from a specialized apply to `Function1.apply`. A Scala 3 lambda's erased
   * `apply(Object): Object` is not marked as a bridge; that class is hidden, and the method is the
   * same erasure. A hand-written overload is neither.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def isErasureBridge(method: java.lang.reflect.Method): Boolean =
    val parameters = method.getParameterTypes
    val erased =
      method.getParameterCount == 1 &&
        method.getReturnType.eq(classOf[AnyRef]) &&
        parameters(0).eq(classOf[Object])
    val lambdaErasure = method.getDeclaringClass.isHidden
    erased && (method.isBridge || method.isSynthetic || lambdaErasure)
