ThisBuild / organization := "streamloom"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.12"

name := "eod-aggregator-streams"

Compile / mainClass := Some("streamloom.eod.App")
Compile / run / fork := true
scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-release:17")
javacOptions ++= Seq("--release", "17")

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % "3.6.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.16.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.16.1",
  "org.slf4j" % "slf4j-simple" % "2.0.12",
  "org.junit.jupiter" % "junit-jupiter" % "5.10.1" % Test,
  "com.github.sbt.junit" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
  "org.apache.kafka" % "kafka-streams-test-utils" % "3.6.1" % Test,
  "io.cucumber" % "cucumber-java" % "7.15.0" % Test,
  "io.cucumber" % "cucumber-junit-platform-engine" % "7.15.0" % Test,
  "org.junit.platform" % "junit-platform-suite" % "1.10.1" % Test,
  "org.junit.platform" % "junit-platform-launcher" % "1.10.1" % Test
)

Test / fork := true
Test / parallelExecution := false
Test / testOptions += Tests.Argument("-v")
// Les scénarios Cucumber sont exécutés par BddTest, via le moteur de suites JUnit.
Test / testOptions += Tests.Filter(_.startsWith("streamloom.eod."))

lazy val unitTest = taskKey[Unit]("Exécute les tests unitaires et de topologie.")
unitTest := Def.taskDyn {
  val names = (Test / definedTests).value.map(_.name)
    .filter(name => name.startsWith("streamloom.eod.") && !name.startsWith("streamloom.eod.bdd."))
  (Test / testOnly).toTask(" " + names.mkString(" "))
}.value
addCommandAlias("bddTest", "testOnly streamloom.eod.bdd.BddTest")
