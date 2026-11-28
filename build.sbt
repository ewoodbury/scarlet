ThisBuild / organization := "com.ewoodbury"
ThisBuild / scalaVersion := "3.3.5"
ThisBuild / version := "0.1.0-SNAPSHOT"

// AGENTS.md asks for about 1000 lines. This is the hard cap, with room for a focused file to grow.
val maxScalaFileLines = 1200

// scarlet-tests splits into this many JVMs. The ForkedTestGroup cap below matches it.
val testShardCount = 2

// Warnings in library projects. scarlet-tests promotes this same set to errors.
val warnedWarts = Warts.allBut(
  Wart.Any,
  Wart.AsInstanceOf,
  Wart.ImplicitParameter,
  Wart.Overloading,
  Wart.NonUnitStatements,
  Wart.Throw,
  Wart.While,
  Wart.Return,
  Wart.AsInstanceOf,
  Wart.IsInstanceOf,
  Wart.OptionPartial,
  Wart.TryPartial,
  Wart.EitherProjectionPartial,
  Wart.ArrayEquals,
  Wart.ImplicitConversion,
  Wart.Serializable,
  Wart.JavaSerializable,
  Wart.Product,
  Wart.LeakingSealed,
  Wart.PublicInference,
  Wart.Option2Iterable,
  Wart.StringPlusAny,
  Wart.JavaConversions,
  Wart.Recursion,
  Wart.Enumeration,
  Wart.ExplicitImplicitTypes,
  Wart.SizeIs,
  Wart.DefaultArguments,
)

lazy val checkFileLength = taskKey[Unit](
  "Fail if a hand-written Scala source exceeds maxScalaFileLines",
)

lazy val commonSettings = Seq(
  semanticdbEnabled := true,
  scalacOptions ++= List(
    "-Wunused:all",
    // E029: a new enum case without a match arm is a compile error, not a runtime MatchError.
    "-Wconf:name=PatternMatchExhaustivity:e",
  ),
  // Prefer async loggers
  ThisBuild / javaOptions ++= Seq(
    "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector",
  ),
  // Run tests sequentially to avoid cross-suite state interference
  Test / parallelExecution := false,
  // Type erasure policy: asInstanceOf and Any-inference are compile errors. The named
  // boundaries, and the rule for a new site, are in docs/ARCHITECTURE.md ("Type erasure policy").
  wartremoverErrors ++= Seq(Wart.AsInstanceOf, Wart.Any),
  wartremoverWarnings ++= warnedWarts,
  checkFileLength := {
    val maxLines = maxScalaFileLines
    val sources =
      ((Compile / unmanagedSources).value ++ (Test / unmanagedSources).value)
        .filter(_.getName.endsWith(".scala"))
    val over = sources.flatMap { file =>
      val count = sbt.IO.readLines(file).length
      if (count > maxLines) Some(s"${file.getPath} ($count lines)") else None
    }
    if (over.nonEmpty) {
      sys.error(
        s"Scala sources exceed $maxLines lines. Split the file.\n" + over.mkString("\n"),
      )
    }
  },
  Compile / compile := (Compile / compile).dependsOn(checkFileLength).value,
  Test / compile := (Test / compile).dependsOn(checkFileLength).value,
)

lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
lazy val log4jApi = "org.apache.logging.log4j" % "log4j-api" % "2.23.1"
lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % "2.23.1"
lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.23.1"
lazy val disruptor = "com.lmax" % "disruptor" % "3.4.4" // required for Log4j2 async

lazy val scalatest = "org.scalatest" %% "scalatest" % "3.2.17"

// User-facing API (clean interface)
lazy val `scarlet-api` = (project in file("modules/scarlet-api"))
  .settings(
    name := "scarlet-api",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging),
  )
  .dependsOn(`scarlet-core`)

// Core model and logical plan
lazy val `scarlet-core` = (project in file("modules/scarlet-core"))
  .settings(
    name := "scarlet-core",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging, log4jApi, log4jCore, log4jSlf4j, disruptor),
  )

// Runtime SPI (RunnableTask, TaskScheduler, ExecutorBackend, Partitioner, ShuffleService)
lazy val `scarlet-runtime` = (project in file("modules/scarlet-runtime"))
  .settings(
    name := "scarlet-runtime",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging),
  )
  .dependsOn(`scarlet-core`)

// Execution engine (Stage, Task, DAGScheduler, StageBuilder, DistCollection, Executor)
lazy val `scarlet-execution` = (project in file("modules/scarlet-execution"))
  .settings(
    name := "scarlet-execution",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging, log4jApi, log4jCore, log4jSlf4j, disruptor),
  )
  .dependsOn(`scarlet-api`, `scarlet-core`, `scarlet-runtime`, `scarlet-columnar`)

// Column batches. No dependency on the logical plan or the row executor.
// ScarletConf is the sum-spill switch. Plan types in core are not referenced here.
lazy val `scarlet-columnar` = (project in file("modules/scarlet-columnar"))
  .settings(
    name := "scarlet-columnar",
    commonSettings,
  )
  .dependsOn(`scarlet-core`)

// SQL frontend. scarlet-core and scarlet-api do not depend on the parser.
lazy val `scarlet-sql` = (project in file("modules/scarlet-sql"))
  .settings(
    name := "scarlet-sql",
    commonSettings,
    libraryDependencies += "com.github.jsqlparser" % "jsqlparser" % "5.3",
  )
  .dependsOn(`scarlet-api`)

// Test module aggregating all tests
lazy val `scarlet-tests` = (project in file("modules/scarlet-tests"))
  .settings(
    name := "scarlet-tests",
    commonSettings,
    wartremoverErrors ++= warnedWarts,
    wartremoverWarnings --= warnedWarts,
    libraryDependencies ++= Seq(scalatest % Test),
    // Two forked JVMs. Suites inside a JVM stay sequential: ScarletConf is process-global,
    // and testForkedParallel stays false. parallelExecution only schedules those JVMs.
    // Each JVM gets its own temp directory so the default spill directory does not collide.
    // Each fork is 3 GB. A 2 GB fork ran out of heap on the large suites.
    Test / testForkedParallel := false,
    Test / parallelExecution := true,
    Test / fork := true,
    Test / javaOptions ++= Seq("-Xmx3g", "-XX:+ExitOnOutOfMemoryError"),
    Test / testGrouping := {
      val suites = (Test / definedTests).value
      val base = (Test / forkOptions).value
      val shardRoot = (Test / target).value / "shards"
      val width = math.max(1, (suites.size + testShardCount - 1) / testShardCount)
      suites
        .grouped(width)
        .zipWithIndex
        .map { case (group, index) =>
          val directory = shardRoot / s"shard-$index"
          IO.createDirectory(directory)
          val options = base.withRunJVMOptions(
            base.runJVMOptions ++ Vector(s"-Djava.io.tmpdir=${directory.getAbsolutePath}"),
          )
          Tests.Group(
            name = s"shard-$index",
            tests = group,
            runPolicy = Tests.SubProcess(options),
          )
        }
        .toSeq
    },
  )
  .dependsOn(
    `scarlet-api`,
    `scarlet-execution`,
    `scarlet-runtime`,
    `scarlet-columnar`,
    `scarlet-sql`,
  )

// Root aggregator
lazy val root = (project in file("."))
  .aggregate(
    `scarlet-api`,
    `scarlet-core`,
    `scarlet-runtime`,
    `scarlet-execution`,
    `scarlet-columnar`,
    `scarlet-sql`,
    `scarlet-tests`,
  )
  .settings(
    name := "scarlet",
    publish / skip := true,
    Compile / sources := Seq.empty,
    Test / sources := Seq.empty,
    Compile / resourceDirectories := Nil,
    Test / resourceDirectories := Nil,
    // Subprojects do the real check. This definition lets `sbt checkFileLength` run here and
    // aggregate.
    checkFileLength := {},
    // The default cap is one forked test group, which would run the shards one after another.
    Global / concurrentRestrictions := Seq(
      Tags.limitAll(java.lang.Runtime.getRuntime.availableProcessors()),
      Tags.limit(Tags.ForkedTestGroup, testShardCount),
      Tags.exclusiveGroup(Tags.Clean),
    ),
  )
