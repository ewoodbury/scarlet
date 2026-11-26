package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalShuffleService

/**
 * `count` of a repartition, coalesce, or partitionBy is the child count. No shuffle is written.
 *
 * The fake runs that shuffle. `storedCount` would then be greater than zero. A refused map still
 * runs once per row, and a filter still drops rows. `partitionBy` keeps a null key.
 */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class TestCountPeel extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  it should "count a repartition without writing a shuffle" in {
    ScarletRuntime.get.shuffle.clear()
    val rows = DistCollection(Seq(1, 2, 3), 2)
    rows.repartition(8).count() shouldBe 3L
    rows.coalesce(1).count() shouldBe 3L
    stored shouldBe 0

    DistCollection(Seq(-1, 0, 1), 2).filter((value: Int) => value > 0).count() shouldBe 1L

    val pairs = DistCollection(Seq[(String, Int)]((null, 1), ("k", 2)), 2)
    pairs.partitionBy(4).count() shouldBe 2L
    stored shouldBe 0

    val hits = new AtomicInteger
    val mapped = DistCollection(Seq(1, 2, 3), 1).map { (value: Int) =>
      val _ = hits.incrementAndGet()
      value.toString
    }
    mapped.count() shouldBe 3L
    hits.get() shouldBe 3
  }

  private def stored: Int =
    ScarletRuntime.get.shuffle match
      case local: LocalShuffleService => local.storedCount
      case other => fail(s"expected the local shuffle, found $other")
