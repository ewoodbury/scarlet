package com.ewoodbury.scarlet.execution

/**
 * Initialization entry point for the execution module. [[initialize]] registers the default
 * execution service unless one is already installed.
 */
object ExecutionModuleInit {

  /**
   * Ensures the execution module is initialized. Idempotent: a later call does not replace a
   * service that is already registered. The work runs in this method so a failure is raised by the
   * call, not cached as a failed class initialization.
   */
  def initialize(): Unit =
    DefaultExecutionService.initialize()
}
