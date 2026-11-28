package com.ewoodbury.scarlet.execution
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check

/**
 * Timed phases of [[JoinBuildBench]]. Phase names and `keep` order stay in the bench main.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw"))
private[execution] object JoinBuildCases:

  import JoinBuildFixture.*

  private[execution] def policySides(data: Prepared): Vector[(String, Side)] =
    val dup = Vector(
      "dup" -> data.joinSide,
      "dup" -> data.survivors,
    ) ++ forcedRows.map(count => "dup" -> forced(count, unique = false))
    val uniqueSmall = Vector(joinRows, 100000).map { count =>
      "unique" -> forced(count, unique = true)
    }
    dup ++ uniqueSmall :+ ("unique" -> forced(rows, unique = true))

  private[execution] def timeE2E(saved: ScarletConf, data: Prepared, threads: Int): Reading =
    configure(saved, threads)
    record("e2e", threads, rows.toLong, "", -1L, -1.0, -1.0, -1.0) {
      ScarletRuntime.get.shuffle.clear()
      data.joined.count()
    } { counted =>
      if (counted != data.outputRows) then refuse(s"e2e count $counted != ${data.outputRows}")
    }

  private[execution] def timeKernel(saved: ScarletConf, data: Prepared, threads: Int): Reading =
    configure(saved, threads)
    record("kernel", threads, rows.toLong, "", -1L, -1.0, -1.0, -1.0) {
      ColumnPipeline.innerJoin(
        Vector(data.leftBatch),
        Vector(data.rightBatch),
        shuffleWidth,
        threads,
        batchSize,
      )
    } { batches =>
      val counted = ColumnReduce.count(batches)
      if (counted != data.outputRows) then refuse(s"kernel rows $counted != ${data.outputRows}")
      if (batches.length != shuffleWidth) then
        refuse(s"kernel returned ${batches.length} batches, expected $shuffleWidth")
    }

  private[execution] def timeOpen(
      saved: ScarletConf,
      label: String,
      side: Side,
      threads: Int,
  ): Reading =
    configure(saved, threads)
    val batches = chunks(side, threads).map(chunk => pairBatch(chunk))
    record(s"build-$label", threads, side.keys.length.toLong, "dup", -1L, -1.0, -1.0, -1.0) {
      MorselScheduler.map(batches, threads) { (_, batch) =>
        HashJoin.openInt32(Vector(HashJoin.JoinSide(batch, 0, 1)), budget)
      }
    } { tables =>
      val built = tables.foldLeft(0L)((total, table) => total + table.buildRows.toLong)
      if (built != side.keys.length.toLong) then
        refuse(s"build-$label rows $built != ${side.keys.length}")
    }

  private[execution] def timeProbe(
      saved: ScarletConf,
      data: Prepared,
      threads: Int,
      engine: Boolean,
  ): Reading =
    configure(saved, threads)
    val phase = if (engine) then "probe-engine" else "probe"
    val buildSide = if (engine) then data.survivors else data.joinSide
    val probeSide = if (engine) then data.joinSide else data.survivors
    val slots = tableSize(buildSide.keys.length)
    val table = OpenTable.build(buildSide, slots, powerOfTwo = true, stepBudget * rows)
    table.insert()
    val built = table.tally()
    val ranges = rangesOf(probeSide.keys.length, threads)
    val once = ranges.map { range =>
      table.count(probeSide.keys, probeSide.values, range._1, range._2)
    }
    acceptProbe(phase, data, engine, once)
    val walked = once.foldLeft(0L)((total, tally) => total + tally.steps)
    record(
      phase,
      threads,
      probeSide.keys.length.toLong,
      "dup",
      slots.toLong,
      buildSide.keys.length.toDouble / slots.toDouble,
      built.occupied.toDouble / slots.toDouble,
      walked.toDouble / probeSide.keys.length.toDouble,
    ) {
      MorselScheduler.map(ranges, threads) { (_, range) =>
        table.count(probeSide.keys, probeSide.values, range._1, range._2)
      }
    }(tallies => acceptProbe(phase, data, engine, tallies))

  private[execution] def timeOutput(
      saved: ScarletConf,
      data: Prepared,
      threads: Int,
      grown: Boolean,
  ): Reading =
    configure(saved, threads)
    val phase = if (grown) then "output-grown" else "output-exact"
    val work = data.replays.zip(data.parts)
    record(phase, threads, data.outputRows, "", -1L, -1.0, -1.0, -1.0) {
      MorselScheduler.map(work, threads) { (_, item) =>
        val (replay, part) = item
        if (grown) then copyGrown(replay, part.probeRows) else copyExact(replay)
      }
    } { written =>
      val counted = written.foldLeft(0L)((total, one) => total + one.rows.toLong)
      if (counted != data.outputRows) then refuse(s"$phase rows $counted != ${data.outputRows}")
    }

  private[execution] def timePolicy(
      saved: ScarletConf,
      keys: String,
      side: Side,
      policy: String,
      threads: Int,
  ): Option[Reading] =
    val counted = side.keys.length
    if (Check.same(keys, "unique") && Check.same(policy, "exact64") && counted >= rows) then
      val slots = capacity(policy, counted)
      val load = counted.toDouble / slots.toDouble
      val steps = expectedProbe(load)
      Some(
        Reading(
          s"policy-$policy",
          threads,
          counted.toLong,
          -1.0,
          -1.0,
          slots.toLong,
          load,
          load,
          steps,
          keys,
          "not timed: 64 empty slots at 1e6 unique keys",
        ),
      )
    else
      configure(saved, threads)
      val pieces = chunks(side, threads)
      try
        val pilot = pieces.map { piece =>
          val table =
            OpenTable.build(piece, capacity(policy, piece.keys.length), pow2(policy), cap)
          table.insert()
          table.tally()
        }
        acceptPolicy(policy, side, pilot)
        val slotTotal = pilot.foldLeft(0L)((total, tally) => total + tally.slots.toLong)
        Some(
          record(
            s"policy-$policy",
            threads,
            counted.toLong,
            keys,
            slotTotal,
            counted.toDouble / slotTotal.toDouble,
            occupancyOf(pilot),
            stepsOf(pilot, counted),
          ) {
            MorselScheduler.map(pieces, threads) { (_, piece) =>
              val table =
                OpenTable.build(piece, capacity(policy, piece.keys.length), pow2(policy), cap)
              table.insert()
              table.tally()
            }
          }(tallies => acceptPolicy(policy, side, tallies)),
        )
      catch
        case capped: ProbeCapped =>
          Some(
            Reading(
              s"policy-$policy",
              threads,
              counted.toLong,
              -1.0,
              -1.0,
              -1L,
              -1.0,
              -1.0,
              -1.0,
              keys,
              s"capped at row ${capped.row} after ${capped.steps} steps",
            ),
          )

  private[execution] def cap: Long = stepBudget

  private[execution] def timeJoined(
      saved: ScarletConf,
      data: Prepared,
      threads: Int,
      policy: String,
      mode: WriteMode,
  ): Reading =
    configure(saved, threads)
    val phase = mode match
      case WriteMode.Count => s"bp-$policy"
      case WriteMode.Grown => s"joined-$policy"
      case WriteMode.Exact => "joined-presized"
    record(phase, threads, rows.toLong, "dup", -1L, -1.0, -1.0, -1.0) {
      MorselScheduler.map(data.parts, threads) { (_, part) =>
        val slots = capacity(policy, part.build.keys.length)
        val table = OpenTable.build(part.build, slots, pow2(policy), stepBudget * rows)
        table.insert()
        val outcome: Either[Tally, Written] = mode match
          case WriteMode.Count =>
            Left[Tally, Written](
              table.count(part.probe.keys, part.probe.values, 0, part.probe.keys.length),
            )
          case WriteMode.Grown =>
            Right[Tally, Written](table.write(part.probe.keys, part.probe.values, 0))
          case WriteMode.Exact =>
            Right[Tally, Written](table.write(part.probe.keys, part.probe.values, part.matches))
        outcome
      }
    } { results =>
      val matched = results.foldLeft(0L) { (total, result) =>
        result match
          case Left(tally) => total + tally.rows.toLong
          case Right(written) => total + written.rows.toLong
      }
      if (matched != data.outputRows) then refuse(s"$phase matches $matched != ${data.outputRows}")
    }

  private[execution] def acceptProbe(
      phase: String,
      data: Prepared,
      engine: Boolean,
      tallies: Vector[Tally],
  ): Unit =
    val matched = tallies.foldLeft(0L)((total, tally) => total + tally.rows.toLong)
    val payload = tallies.foldLeft(0L)((total, tally) => total + tally.payload)
    if (matched != data.outputRows) then refuse(s"$phase matches $matched != ${data.outputRows}")
    val expected = if (engine) then data.leftPayload else data.rightPayload
    if (payload != expected) then refuse(s"$phase payload $payload != $expected")

  private[execution] def acceptPolicy(policy: String, side: Side, tallies: Vector[Tally]): Unit =
    val built = tallies.foldLeft(0L)((total, tally) => total + tally.rows.toLong)
    if (built != side.keys.length.toLong) then
      refuse(s"policy-$policy rows $built != ${side.keys.length}")
    if (tallies.exists(_.occupied <= 0)) then refuse(s"policy-$policy occupied no slot")
