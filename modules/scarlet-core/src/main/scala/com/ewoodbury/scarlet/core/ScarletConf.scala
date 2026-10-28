package com.ewoodbury.scarlet.core

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/**
 * Centralized configuration for Scarlet runtime defaults.
 */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
final case class ScarletConf(
    /** Number of partitions to use for shuffle outputs when not otherwise specified. */
    defaultShufflePartitions: Int = 4,
    /** Default task parallelism when deriving concurrency without explicit hints. */
    defaultParallelism: Int = 4,
    /** Size of the thread pool backing the local TaskScheduler. */
    threadPoolSize: Int = 4,
    /** Approximate samples per logical partition to estimate sort key distribution. */
    sortSamplePerPartition: Int = 20,
    /** Cap on total number of samples to bound driver memory. */
    sortMaxSample: Int = 20000,
    /** Size threshold (in number of records) below which broadcast-hash join is preferred. */
    broadcastJoinThreshold: Long = 1000L,
    /**
     * Byte budget for the broadcast join's build table only, not the rest of the query. The
     * default is 64 MiB. The check runs after that input is evaluated. An estimate above the
     * budget throws `IllegalStateException` and does not fall back to the row path. A full outer
     * join's matched-row bitmaps are allocated beside the table and are not part of this budget.
     */
    broadcastBuildBudgetBytes: Long = 64L * 1024L * 1024L,
    /**
     * Whether to enable sort-merge join for large datasets. If false, always uses shuffle-hash
     * join.
     */
    enableSortMergeJoin: Boolean = true,

    // === Fault Tolerance Configuration ===
    /** Maximum number of retries for failed tasks */
    maxTaskRetries: Int = 3,
    /** Base delay between retries in milliseconds (exponential backoff) */
    baseRetryDelayMs: Long = 1000L,
    /** Maximum backoff delay to prevent excessive waits */
    maxRetryDelayMs: Long = 30000L,
    /** When true, `Int` and `(Int, Int)` plans run on the columnar kernels. */
    columnarExecution: Boolean = true,
    /** Rows per morsel on the columnar path. */
    columnarBatchSize: Int = 8192,
    /**
     * When true, a grouped-sum bucket, an inner broadcast build, or a sort-merge partition whose
     * estimate exceeds `spillBudgetBytes` is spilled. Off by default, so each stays in memory.
     * Outer joins do not read this flag.
     */
    spillEnabled: Boolean = false,
    /**
     * Byte budget for one grouped-sum bucket, one inner-join build table, and the sorted sides of
     * one sort-merge partition. The default is 64 MiB. A broadcast build over this budget is
     * partitioned with the probe. One partition that is still over it throws. A sort-merge
     * partition over it is written as sorted runs. One equal-key group that is still over it
     * throws.
     */
    spillBudgetBytes: Long = 64L * 1024L * 1024L,
    /**
     * Directory for spill files. Empty selects the spill store default,
     * `java.io.tmpdir/scarlet-spill`, which is outside the repository.
     */
    spillDirectory: String = "",
    /**
     * Exclusive cap on files one spill store may publish. The default is 1024, the same cap as the
     * spill store. A store may publish at most one file fewer than this. A spilled join caps its
     * partition count at one less than this, one file per partition.
     */
    spillMaxFiles: Int = 1024,
    /**
     * Exclusive cap on bytes one spill store may publish. The default is 8 GiB, the same cap as
     * the spill store.
     */
    spillMaxBytes: Long = 8L * 1024L * 1024L * 1024L,
):

  /** Builds the task retry policy from this configuration. */
  def retryPolicy: RetryPolicy = RetryPolicy.ExponentialBackoff(
    maxRetries = maxTaskRetries,
    baseDelay = FiniteDuration(baseRetryDelayMs, MILLISECONDS),
    maxDelay = FiniteDuration(maxRetryDelayMs, MILLISECONDS),
  )

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments", "org.wartremover.warts.Var"))
object ScarletConf {
  @volatile private var current: ScarletConf = ScarletConf()

  /** Returns the active configuration. */
  def get: ScarletConf = current

  /** Overrides the active configuration at runtime. */
  def set(conf: ScarletConf): Unit = {
    current = conf
  }
}
