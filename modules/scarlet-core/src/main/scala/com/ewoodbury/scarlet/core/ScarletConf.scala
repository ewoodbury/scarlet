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
     * When true, a grouped-sum bucket, an int `reduceByKey` bucket, a long `reduceByKey` bucket, a
     * string `reduceByKey` bucket, an int, long, or string `countByKey` bucket, an inner broadcast
     * build, an int shuffle-hash build bucket, a long shuffle-hash build bucket, a string
     * shuffle-hash build bucket, a sort-merge partition, an int-pair sort, a long-pair sort, or a
     * string-pair sort whose estimate exceeds `spillBudgetBytes` is spilled. Off by default, so
     * each stays in memory. Outer hash and broadcast joins do not read this flag. An int outer
     * sort-merge does and writes runs. A long outer sort-merge reads the flag and throws when the
     * present-key estimate is over the budget, and it writes nothing. A long inner sort-merge does
     * the same. A string inner sort-merge reads the flag and throws when the present-key estimate,
     * including the dictionary, is over the budget, and it writes nothing. A spilled int, long, or
     * string `reduceByKey` bucket may change first-seen output order; compare it as a map. An int,
     * long, or string `countByKey` with this flag off still throws `ClosedKernelRefusal` before a
     * table. The long estimate charges 8-byte keys. The string estimate charges dictionary bytes.
     * With the flag on, an over-budget post-exchange bucket spills. A count table that still does
     * not fit throws `ClosedKernelRefusal`. An int, long, or string `groupByKey` bucket over the
     * budget spills when this flag is on: the post-exchange bucket is sliced in input order and
     * grouped one morsel at a time. Spill off keeps the in-memory groups and writes nothing. A
     * morsel whose group table does not fit throws `ClosedKernelRefusal`. The appended groups are
     * not bounded by the budget.
     */
    spillEnabled: Boolean = false,
    /**
     * Byte budget for one grouped-sum bucket, one int `reduceByKey` bucket, one long `reduceByKey`
     * bucket, one string `reduceByKey` bucket, one int, long, or string `countByKey` bucket, one
     * inner-join build table, one int shuffle-hash build bucket, one long shuffle-hash build
     * bucket, one string shuffle-hash build bucket, the sorted sides of one sort-merge partition,
     * and the sorted input of one int-pair, long-pair, or string-pair sort. The default is 64 MiB.
     * A broadcast build over this budget is partitioned with the probe. One partition that is
     * still over it throws. An int or long shuffle-hash build bucket over it is sub-partitioned
     * and probed one piece at a time. A string shuffle-hash build bucket over it is
     * sub-partitioned by the quotient of its hash and the parent width. The estimate includes the
     * dictionary. A piece that is still over it throws. A sort-merge partition over it is written
     * as sorted runs and merged `spillFanIn` at a time. One equal-key group that is still over it
     * throws. A long inner sort-merge, a long outer sort-merge, a string inner sort-merge, and a
     * string outer sort-merge throw instead of writing runs. The string estimate includes the
     * dictionary. An int-pair sort over it writes one sorted run per input partition and merges
     * `spillFanIn` runs at a time. A long-pair sort and a string-pair sort do the same for their
     * pairs. One input partition is still sorted in memory. An int `reduceByKey` bucket over it is
     * sub-partitioned and folded one morsel at a time. A long `reduceByKey` bucket and a string
     * `reduceByKey` bucket over it are sub-partitioned and folded the same way. A spilled string
     * bucket may change first-seen output order. An int, long, or string `countByKey` bucket over
     * it spills when spill is on: the post-exchange bucket is sub-partitioned and folded one
     * morsel at a time. The long estimate charges 8-byte keys. The string estimate charges
     * dictionary bytes. Spill off throws `ClosedKernelRefusal` before a table. A count table that
     * still does not fit throws `ClosedKernelRefusal`. An int, long, or string `groupByKey` bucket
     * over it spills when spill is on, sliced in input order. Spill off stays in memory. A morsel
     * whose group table does not fit throws `ClosedKernelRefusal`. The appended groups are not
     * bounded by this budget. The long group estimate charges 8-byte keys. The string group
     * estimate charges dictionary bytes, including an entry no row uses.
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
    /**
     * Fan-in of a spilled sort-merge, a spilled int-pair sort, a spilled long-pair sort, and a
     * spilled string-pair sort. At most this many sorted runs are merged at once. The default is
     *   8. A side, or a sort, with more runs is merged in passes until one run remains, so a pass
     *      loads at most this many batches. The join then loads one batch from each side. The sort
     *      then reads one batch of the remaining run and scatters it by rank. A value below 2 is
     *      rejected when a partition or a sort spills, because a pass would not shrink the runs.
     */
    spillFanIn: Int = 8,
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
