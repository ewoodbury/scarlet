package com.ewoodbury.scarlet.execution

/**
 * Loads the execution module for suites that call `ExecutionService.get` without a
 * `DistCollection` action. Constructing a collection does not load the module.
 */
object ExecutionModuleSupport:

  def initialize(): Unit =
    ExecutionModuleInit.initialize()
