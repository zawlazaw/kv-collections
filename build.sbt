import sbt.Keys._

/*
 * GENERAL SETTINGS
 */

lazy val commonSettings = Seq(
  scalaVersion := "2.12.6",
  fork := true,
)

/*
 * DEPENDENCIES
 */

lazy val commonDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.5",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.log4s" %% "log4s" % "1.6.1",
  "org.scala-lang" % "scala-reflect" % "2.12.6",
  "org.apache.commons" % "commons-lang3" % "3.7",
  "commons-io" % "commons-io" % "2.6",
  "com.jsuereth" %% "scala-arm" % "2.0",
  "com.beachape" %% "enumeratum" % "1.5.13",
  "org.json4s" %% "json4s-jackson" % "3.6.0-M4",
  "com.typesafe.play" %% "play-json" % "2.6.9",
  "org.mongodb" % "bson" % "3.7.1"
  )

/*
 * SUB-PROJECTS
 */

lazy val `kv-collections` = (project in file("kv-collections")).
  settings(version := "0.0.1-SNAPSHOT").
  settings(commonSettings: _*).
  settings(libraryDependencies ++= commonDependencies)

lazy val root = (project in file("."))
  .aggregate(`kv-collections`)
