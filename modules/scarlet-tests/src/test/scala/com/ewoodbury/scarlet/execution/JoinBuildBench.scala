package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale

import scala.util.control.NonFatal

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Attributes `filter-join` and times table-size and output-buffer policies. Not a test, and not
 * part of `make test`. No product code changes.
 *
 * Cross-engine shape: 1e6 pairs, keys `row % 1024`, filters `key % 3 != 0` and `value >= 0`,
 * joined to 10240 rows (10 copies of `0 until 1024`). Spill is off. The build is under the 64 MiB
 * spill budget, so this is the in-memory kernel. Shuffle width stays 4.
 *
 * The engine builds the filtered left and probes the 10240-row right. `probe` builds that right
 * side and probes the filtered rows, writing nothing. `output-exact` replays the match list into
 * exact-capacity columns. `output-grown` starts at the right-hand probe length and doubles.
 * `policy-current` is `ColumnHash.tableSize` (2×, next power of two). `policy-load75` is
 * `ceil(rows / 0.75)` as a power of two. `policy-exact64` is `rows + 64` rounded to 64, probed
 * with `floorMod`. `bp-*` is engine build plus a probe that does not write. `joined-*` is that
 * build plus a probe that writes. Each phase is its own p50, not a difference of other rows.
 * Unique `exact64` at 1e6 rows is not timed: 64 empty slots.
 *
 * p50 is nearest-rank `ceil(50/100*n)` over 40 rounds after 5 warmups. `alloc_mb` is that rank
 * over 5 later rounds, in decimal megabytes. A worker that exits between probe reads is missing.
 *
 * {{{
 * sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.JoinBuildBench"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
object JoinBuildBench:

  import JoinBuildFixture.*
  import JoinBuildCases.*

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(shuffleWidth)
    var status = 0
    try
      configure(savedConf, shuffleWidth)
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val _ = MorselScheduler.map(Vector(0, 1), shuffleWidth)((_, item) => item)
      run(savedConf)
    catch
      case refused: HarnessRefusal =>
        System.err.println(refused.getMessage)
        status = 1
      case NonFatal(thrown) =>
        thrown.printStackTrace(System.err)
        status = 1
    finally
      try scheduler.shutdown()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def run(saved: ScarletConf): Unit =
    assertTableSize()
    val data = prepare()
    verify(data)
    preamble(data)
    val readings = Vector.newBuilder[Reading]
    def keep(reading: Reading): Unit =
      println(line(reading))
      val _ = readings += reading
    threadCounts.foreach { threads =>
      keep(timeE2E(saved, data, threads))
      keep(timeKernel(saved, data, threads))
    }
    val buildSides = Vector(
      ("join", data.joinSide),
      ("survivors", data.survivors),
    ) ++ forcedRows.map(count => (s"forced-$count", forced(count, unique = false)))
    buildSides.foreach { (label, side) =>
      threadCounts.foreach(threads => keep(timeOpen(saved, label, side, threads)))
    }
    threadCounts.foreach { threads =>
      keep(timeProbe(saved, data, threads, engine = false))
      keep(timeProbe(saved, data, threads, engine = true))
      keep(timeOutput(saved, data, threads, grown = false))
      keep(timeOutput(saved, data, threads, grown = true))
    }
    policySides(data).foreach { (keys, side) =>
      Vector("current", "load75", "exact64").foreach { policy =>
        threadCounts.foreach { threads =>
          timePolicy(saved, keys, side, policy, threads).foreach(keep)
        }
      }
    }
    Vector("current", "load75", "exact64").foreach { policy =>
      threadCounts.foreach { threads =>
        keep(timeJoined(saved, data, threads, policy, WriteMode.Count))
        keep(timeJoined(saved, data, threads, policy, WriteMode.Grown))
      }
    }
    threadCounts.foreach { threads =>
      keep(timeJoined(saved, data, threads, "current", WriteMode.Exact))
    }
    System.err.println(table(readings.result()))

  private def preamble(data: Prepared): Unit =
    val runtime = ManagementFactory.getRuntimeMXBean
    System.err.println(
      s"join-build-bench rows=$rows survivors=${data.survivors.keys.length} " +
        s"join_rows=$joinRows output_rows=${data.outputRows} batch=$batchSize " +
        s"partitions=$sourcePartitions shuffle=$shuffleWidth warmup=$warmup rounds=$rounds " +
        s"alloc_rounds=$allocRounds threads=${threadCounts.mkString(",")} spill=false " +
        s"p50=nearest-rank java=${runtime.getVmName} ${runtime.getVmVersion}",
    )
    System.err.println(
      "engine builds the filtered left and probes the 10240-row right. " +
        "probe builds the 10240-row side and probes the filtered rows with no output write. " +
        "output-grown starts at that right-hand probe length and doubles.",
    )

  private def table(readings: Vector[Reading]): String =
    val header = Vector(
      "",
      "| phase | threads | rows | keys | p50 ms | alloc mb | slots | load | occ | probe |",
      "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    )
    val body = readings.map { reading =>
      s"| ${reading.phase} | ${reading.threads} | ${reading.rows} | ${reading.keys} | " +
        s"${fmt(reading.p50Ms)} | ${fmt(reading.allocMb)} | ${reading.slots} | " +
        s"${fmt4(reading.load)} | ${fmt4(reading.occupancy)} | ${fmt4(reading.probeSteps)} |"
    }
    (header ++ body).mkString("\n")

  private def line(reading: Reading): String =
    val note =
      if (reading.note.isEmpty) then ""
      else s""","note":"${reading.note}""""
    val keys =
      if (reading.keys.isEmpty) then ""
      else s""","keys":"${reading.keys}""""
    "{" +
      s""""schema":"$schemaName",""" +
      s""""phase":"${reading.phase}",""" +
      s""""threads":${reading.threads},""" +
      s""""rows":${reading.rows}""" +
      keys +
      s""","p50_ms":${fmt(reading.p50Ms)},""" +
      s""""alloc_mb":${fmt(reading.allocMb)},""" +
      s""""slots":${reading.slots},""" +
      s""""load":${fmt4(reading.load)},""" +
      s""""occupancy":${fmt4(reading.occupancy)},""" +
      s""""probe_steps":${fmt4(reading.probeSteps)},""" +
      s""""warmup":$warmup,""" +
      s""""rounds":$rounds""" +
      note +
      "}"

  private def fmt(value: Double): String =
    if (value < 0.0) then "-1" else String.format(Locale.ENGLISH, "%.3f", value)

  private def fmt4(value: Double): String =
    if (value < 0.0) then "-1" else String.format(Locale.ENGLISH, "%.4f", value)
