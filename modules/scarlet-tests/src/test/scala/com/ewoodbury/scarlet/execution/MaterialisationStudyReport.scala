package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import MaterialisationStudySupport.*

/**
 * Prints [[MaterialisationStudy]]. The controlled pair is B4 minus F4. S5 minus B4 is the fifth
 * map and `sumProjected` on a forced boundary. P5 minus F4 is the folded fifth map.
 */
private[execution] object MaterialisationStudyReport:

  final case class Facts(
      fusedSum: Long,
      projectedSum: Long,
      dependentSum: Long,
      groupedSum: Long,
      morsels: Int,
      groupedMorsels: Int,
      allocOn: Boolean,
  )

  private val stageNames: Vector[String] =
    Vector("encode", "partial", "merge", "exchange", "final")

  def printAll(
      queries: Vector[Row],
      stages: Vector[Row],
      permuted: Vector[Row],
      probes: Vector[Row],
      facts: Facts,
  ): Unit =
    println(header(facts))
    println()
    queries.foreach(row => println(tsv(row)))
    stages.foreach(row => println(tsv(row)))
    permuted.foreach(row => println(tsv(row)))
    probes.foreach(row => println(tsv(row)))
    println()
    table("Queries", queries.filter(row => sameText(row.kind, "query")))
    attribution(queries, stages)
    keyOrder()
    table("Grouped stages and replay", groupedTable(queries, stages))
    table("Permuted keys", permutedTable(queries, permuted))
    replayGap(queries, stages)
    table("Caller probes", probes)
    batches(queries, probes, facts)
    allocation(queries, probes)
    println()

  private def header(facts: Facts): String =
    val runtime = ManagementFactory.getRuntimeMXBean
    val alloc = if (facts.allocOn) then estimatedKind else unavailableKind
    val p50 = rankIndex(measure, 50)
    val p95 = rankIndex(measure, 95)
    s"materialisation study warmup=$warmup measure=$measure alloc_samples=$allocSamples " +
      s"batch=$batchSize source_partitions=$sourcePartitions shuffle=$shuffleWidth " +
      s"alloc_bytes=$alloc\n" +
      s"percentile=nearest-rank ceil(p/100*n) no-interpolation n=$measure " +
      s"p50_rank=${p50 + 1} p95_rank=${p95 + 1} max_rank=$measure\n" +
      "width_protocol=one JVM; correctness accept runs at width 1; discarded warm rounds and " +
      "recorded rounds alternate width 1 and width 4, flipping the leader each round; spread is " +
      "the absolute p50 gap between rounds where that width led and rounds where it followed\n" +
      s"F4 checksum=${facts.fusedSum} B4 checksum=${facts.fusedSum} " +
      s"S5 checksum=${facts.projectedSum} P5 checksum=${facts.projectedSum} " +
      s"RAW checksum=${facts.fusedSum} RAWD checksum=${facts.dependentSum}\n" +
      s"GROUPED rows=$groupedRows groups=$groups checksum=${facts.groupedSum} " +
      "keys=row%groups batch_multiple_of_groups cached_slot_merge " +
      s"PERMUTED seed=$permutedSeed shuffled_same_multiset " +
      s"element_morsels=${facts.morsels} grouped_morsels=${facts.groupedMorsels}\n" +
      s"java=${runtime.getVmName} ${runtime.getVmVersion} args=${runtime.getInputArguments}"

  private def attribution(queries: Vector[Row], stages: Vector[Row]): Unit =
    println("Attribution")
    println(
      "B4 and F4 both apply four maps. F4 sums them with ElementSum and does not write the " +
        "column. B4 encodes, slices, writes the pipeline, concatenates, reslices, and sums that " +
        "column with sumInt32. B4-F4 is the boundary cost. S5 uses the same prefix as B4 and " +
        "sums with sumProjected of one +1. S5-B4 is the fifth map plus that sum kernel. P5 folds " +
        "the fifth map into the sum and does not write the pipeline column. P5-F4 is that folded " +
        "map. RAW is the folded-loop floor. Each width is reported on its own. Nothing here " +
        "chooses fuse versus stream.",
    )
    threadCounts.foreach { threads =>
      val fused = findRow(queries, "query", "F4", threads)
      val boundary = findRow(queries, "query", "B4", threads)
      val fifth = findRow(queries, "query", "S5", threads)
      val projected = findRow(queries, "query", "P5", threads)
      val raw = findRow(queries, "query", "RAW", threads)
      val dependent = findRow(queries, "query", "RAWD", threads)
      val grouped = findRow(queries, "query", "GROUPED", threads)
      val boundaryNs = boundary.p50Ns - fused.p50Ns
      val fifthNs = fifth.p50Ns - boundary.p50Ns
      val foldedNs = projected.p50Ns - fused.p50Ns
      println(
        s"CONTROL threads=$threads boundary_ns=$boundaryNs fifth_ns=$fifthNs folded_ns=$foldedNs " +
          s"f4_ns=${fused.p50Ns} b4_ns=${boundary.p50Ns} s5_ns=${fifth.p50Ns} " +
          s"p5_ns=${projected.p50Ns} raw_ns=${raw.p50Ns} rawd_ns=${dependent.p50Ns} " +
          s"spread_f4_ns=${fused.spreadNs} spread_b4_ns=${boundary.spreadNs} " +
          s"spread_s5_ns=${fifth.spreadNs} spread_p5_ns=${projected.spreadNs}",
      )
      println(
        s"threads=$threads boundary B4-F4 is ${signedMs(boundaryNs)} ms " +
          s"(${percent(boundaryNs, boundary.p50Ns)} of B4, ${percent(boundaryNs, fused.p50Ns)} of F4). " +
          s"F4 ${ms(fused.p50Ns)} ms, B4 ${ms(boundary.p50Ns)} ms, spread F4 ${spreadText(fused.spreadNs)}, " +
          s"spread B4 ${spreadText(boundary.spreadNs)}.",
      )
      println(
        s"threads=$threads fifth map plus sumProjected is S5-B4 ${signedMs(fifthNs)} ms " +
          s"(${percent(fifthNs, fifth.p50Ns)} of S5). S5 ${ms(fifth.p50Ns)} ms. " +
          s"P5-F4 is ${signedMs(foldedNs)} ms (P5 ${ms(projected.p50Ns)} ms), the folded fifth map.",
      )
      println(
        s"threads=$threads F4 is ${multiple(fused.p50Ns, raw.p50Ns)}× RAW and " +
          s"${multiple(fused.p50Ns, dependent.p50Ns)}× RAWD. RAW ${ms(raw.p50Ns)} ms is four " +
          "dependent +1 adds, which the JIT can fold into +4, so F4/RAW overstates fused-path " +
          s"overhead against work the JIT still performs. RAWD ${ms(dependent.p50Ns)} ms is four " +
          "updates of current + (current & 1) + 1. The JIT may unroll it. It is not the folded floor. " +
          "Neither ratio is a fuse-versus-stream result.",
      )
      println(
        s"threads=$threads GROUPED is ${nsPerRow(grouped.p50Ns, groupedRows)} ns/row at " +
          s"$groupedRows rows. RAW is ${nsPerRow(raw.p50Ns, elementRows)} ns/row and RAWD is " +
          s"${nsPerRow(dependent.p50Ns, elementRows)} ns/row at $elementRows rows. " +
          "Those floors are not a grouped kernel, so the pair is not a boundary ratio. " +
          s"Spread GROUPED ${spreadText(grouped.spreadNs)}.",
      )
      val replay = findRow(queries, "replay", "REPLAY", threads)
      val stageRows = stageNames.map(name => findRow(stages, "stage", name, threads))
      val described = stageRows.map(row => s"${row.name} ${ms(row.p50Ns)} ms").mkString(", ")
      val stageSum = stageRows.map(_.p50Ns).sum
      val gap = replay.p50Ns - grouped.p50Ns
      val permuted = findRow(queries, "query", "PERMUTED", threads)
      println(
        s"threads=$threads stage p50: $described. Sum of those p50 values is ${ms(stageSum)} ms. " +
          "That sum is not a measurement. Those stages use repeating keys, so the merge p50 is " +
          "the cached-slot path. REPLAY times encode, partial, merge, exchange, and final in one " +
          s"harness call: ${ms(replay.p50Ns)} ms. It is a synthetic stage replay, not a query " +
          s"result. GROUPED p50 is ${ms(grouped.p50Ns)} ms, the query-level result from " +
          s"tryGroupedSum on the repeating keys. PERMUTED p50 is ${ms(permuted.p50Ns)} ms, " +
          s"tryGroupedSum on the shuffled keys. Gap replay-minus-GROUPED is ${signedMs(gap)} ms. " +
          s"Spread replay ${spreadText(replay.spreadNs)}.",
      )
    }
    println(
      "Worker lifecycles match at width 4: engine actions create and join scarlet-morsel-* " +
        "threads inside the call, and RAW/RAWD create and join scarlet-raw-* threads inside the " +
        "sample. The floor is not running during engine samples. Width 1 runs the work on the " +
        "caller for both. Each width's boundary stands on its own. The spread is the remaining " +
        "order effect inside this JVM, not a scaling result.",
    )
    println()

  private def replayGap(queries: Vector[Row], stages: Vector[Row]): Unit =
    println(
      "gc_ms on a row is the sum of collector-time increases around that row's own timed samples. " +
        "A stage row does not repeat a loop-wide total. Stage batch and visit counters are exact. " +
        "Their percentiles are not additive.",
    )
    threadCounts.foreach { threads =>
      val grouped = findRow(queries, "query", "GROUPED", threads)
      val replay = findRow(queries, "replay", "REPLAY", threads)
      val stageRows = stageNames.map(name => findRow(stages, "stage", name, threads))
      println(
        s"threads=$threads GROUPED batches=${grouped.batches} encodes=${grouped.encodes} " +
          s"visits=${grouped.visits} gc_ms=${grouped.gcMs}. REPLAY batches=${replay.batches} " +
          s"encodes=${replay.encodes} visits=${replay.visits} gc_ms=${replay.gcMs}. " +
          s"Stage batches sum to ${stageRows.map(_.batches).sum}. Stage visits sum to " +
          s"${stageRows.map(_.visits).sum}. Stage gc_ms values are ${stageRows.map(_.gcMs).mkString(", ")} " +
          s"and sum to ${stageRows.map(_.gcMs).sum}.",
      )
    }
    println()

  private def batches(queries: Vector[Row], probes: Vector[Row], facts: Facts): Unit =
    println("Batches")
    val encode = findRow(probes, "probe", "encode", 1)
    val slice = findRow(probes, "probe", "slice", 1)
    val output = findRow(probes, "probe", "output", 1)
    val concat = findRow(probes, "probe", "concat", 1)
    val reslice = findRow(probes, "probe", "reslice", 1)
    val fusedAccount = encode.batches + slice.batches
    val boundaryAccount =
      encode.batches + slice.batches + output.batches + concat.batches + reslice.batches
    println(
      s"Probe batches: encode ${encode.batches}, slice ${slice.batches}, output ${output.batches}, " +
        s"concat ${concat.batches}, reslice ${reslice.batches}. Encodes: encode ${encode.encodes}, " +
        s"others ${slice.encodes}, ${output.encodes}, ${concat.encodes}, ${reslice.encodes}. " +
        "Probe bytes are caller-thread totals from a separate pass. They are not subtracted from " +
        "query estimates.",
    )
    threadCounts.foreach { threads =>
      val fused = findRow(queries, "query", "F4", threads)
      val boundary = findRow(queries, "query", "B4", threads)
      val fifth = findRow(queries, "query", "S5", threads)
      val projected = findRow(queries, "query", "P5", threads)
      println(
        s"F4 t$threads batches ${fused.batches} against encode+slice $fusedAccount " +
          s"(${relation(fused.batches, fusedAccount)}). Visits ${fused.visits} against " +
          s"${facts.morsels} morsels (${relation(fused.visits, facts.morsels.toLong)}). " +
          "ElementSum reads the sliced scan and does not write a column.",
      )
      println(
        s"B4 t$threads batches ${boundary.batches} against encode+slice+output+concat+reslice " +
          s"$boundaryAccount (${relation(boundary.batches, boundaryAccount)}). Visits " +
          s"${boundary.visits}. S5 batches ${fifth.batches} " +
          s"(${relation(fifth.batches, boundary.batches)} B4). P5 batches ${projected.batches} " +
          s"(${relation(projected.batches, fused.batches)} F4). The folded sum does not publish " +
          "the pipeline column. S5 ends in sumProjected. B4 ends in sumInt32.",
      )
    }
    println(
      s"Element morsels ${facts.morsels}. Grouped morsels ${facts.groupedMorsels}. " +
        s"Batch $batchSize, $sourcePartitions source partitions.",
    )
    println()

  private def allocation(queries: Vector[Row], probes: Vector[Row]): Unit =
    println("Allocation")
    println(
      "Query, stage, and replay alloc figures are a process-wide estimate from a separate pass, " +
        s"not a timed sample and not a lower bound. The pass repeats each action $allocSamples times " +
        "and is not the alternating latency protocol. The sampler is parked during timing. It counts " +
        "the calling thread, threads named scarlet-morsel-*, and scarlet-raw-* workers. Raw workers " +
        "store getThreadAllocatedBytes before they return. Morsel workers are read while the action " +
        "runs. High bias: the caller total includes planner and harness objects, ThreadMXBean counts " +
        "TLAB reservations, and a worker counter includes startup. Low bias: a morsel worker missed " +
        "between reads is absent, and bytes after the last read are absent. Compare an estimate to " +
        "the caller-only delta only at one thread, where morsel work runs on the caller. Do not " +
        "compare a four-thread estimate to that delta or to the caller probes.",
    )
    queries.filter(_.threads == 1).foreach { row =>
      println(
        s"${row.name} t1 estimate ${bytes(row.allocBytes)} caller-delta ${bytes(row.callerBytes)} " +
          s"residual ${residualText(row.allocBytes, row.callerBytes)}",
      )
    }
    val output = findRow(probes, "probe", "output", 1)
    val concat = findRow(probes, "probe", "concat", 1)
    val reslice = findRow(probes, "probe", "reslice", 1)
    println(
      s"Caller probes, one thread, separate calls: output ${bytes(output.allocBytes)}, " +
        s"concat ${bytes(concat.allocBytes)}, reslice ${bytes(reslice.allocBytes)}. " +
        "These are not a split of B4-F4 or P5-F4.",
    )

  private def table(title: String, rows: Vector[Row]): Unit =
    println(title)
    println(
      "| kind | case | threads | p50 | p95 | max | spread | alloc | batches | encodes | visits | gc_ms |",
    )
    println("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    rows.foreach { row =>
      println(
        s"| ${row.kind} | ${row.name} | ${row.threads} | ${ms(row.p50Ns)} ms | ${ms(row.p95Ns)} ms | " +
          s"${ms(row.maxNs)} ms | ${spreadText(row.spreadNs)} | ${bytes(row.allocBytes)} | " +
          s"${row.batches} | ${row.encodes} | ${row.visits} | ${row.gcMs} |",
      )
    }
    println()

  private def tsv(row: Row): String =
    s"${row.kind}\t${row.name}\tt${row.threads}\tshape=${row.shape}\trows=${row.rowCount}\t" +
      s"p50_ns=${row.p50Ns}\tp95_ns=${row.p95Ns}\tmax_ns=${row.maxNs}\t" +
      s"spread_ns=${row.spreadNs}\talloc_bytes=${row.allocBytes}\t" +
      s"caller_bytes=${row.callerBytes}\talloc_kind=${row.allocKind}\tbatches=${row.batches}\t" +
      s"encodes=${row.encodes}\tvisits=${row.visits}\tgc_ms=${row.gcMs}\tchecksum=${row.checksum}"

  private def keyOrder(): Unit =
    println(
      "Sequential grouped keys are row % group count. The batch size is a multiple of that " +
        "count, so every morsel repeats one key sequence and every dense partial shares it. " +
        "The merge then adds through cached slots. That is a best case.",
    )
    println(
      s"PERMUTED shuffles that key multiset with seed $permutedSeed. Values stay in row order, " +
        "so the checksum matches GROUPED. Partials do not share a key sequence, and the merge " +
        "probes. The partial scan is faster on the repeating keys too, because each morsel " +
        "probes one key order. Most of the query gap is that scan. REPLAY and REPLAY-PERM call " +
        "the five stages from the harness. They are synthetic stage replays, not query results. " +
        "GROUPED p50 is the query-level result for the repeating keys.",
    )
    println()

  private def permutedTable(queries: Vector[Row], stages: Vector[Row]): Vector[Row] =
    val names = stageNames :+ "REPLAY-PERM"
    (stages ++ queries.filter(row => sameText(row.kind, "permuted"))).sortBy { row =>
      val index = names.indexOf(row.name)
      (row.threads, if (index < 0) then names.length else index)
    }

  private def groupedTable(queries: Vector[Row], stages: Vector[Row]): Vector[Row] =
    val names = stageNames :+ "REPLAY"
    (stages ++ queries.filter(row => sameText(row.kind, "replay"))).sortBy { row =>
      val index = names.indexOf(row.name)
      (row.threads, if (index < 0) then names.length else index)
    }

  private def residualText(estimate: Long, caller: Long): String =
    if (estimate < 0L || caller < 0L) then "n/a" else signedBytes(estimate - caller)

  private def findRow(rows: Vector[Row], kind: String, name: String, threads: Int): Row =
    rows
      .find(row => sameText(row.kind, kind) && sameText(row.name, name) && row.threads == threads)
      .getOrElse {
        throw new IllegalStateException(s"missing $kind $name threads=$threads")
      }

  private def sameText(left: String, right: String): Boolean =
    left.compareTo(right) == 0

  private def relation(left: Long, right: Long): String =
    if (left == right) then "matches" else "does not match"
