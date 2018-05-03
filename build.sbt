name := "test"

lazy val commonSettings = Seq(
  organization := "com.ingest.data",
  version := "0.0.1",
  scalaVersion := "2.11.8",
  assemblyMergeStrategy in assembly := {
    case PathList("org.apache.spark.sql.sources.DataSourceRegister") => MergeStrategy.concat
    case PathList("META-INF", "services", xs@_) => MergeStrategy.concat
    case PathList("META-INF", "native", xs@_) => MergeStrategy.first
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)

val sparkVersion = "2.1.1"
val emrAwsVersion = "1.11.61"
val scalaTestVersion = "3.0.3"

val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.apache.hadoop" % "hadoop-common" % "2.2.0" % Provided,
  "com.amazonaws" % "aws-java-sdk-core" % emrAwsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-dynamodb" % emrAwsVersion % Provided,
  "com.amazonaws" % "aws-java-sdk-s3" % emrAwsVersion % Provided,
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.joda" % "joda-convert" % "1.8.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.22",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "org.json4s" %% "json4s-native" % "3.2.11"
).map(_.exclude("org.scalatest", "scalatest"))

lazy val common = (project in file("common")).
  settings(commonSettings: _*).
  disablePlugins(sbtassembly.AssemblyPlugin).
  settings(
    name := "common",
    libraryDependencies ++= commonDependencies
  )

lazy val testCommon = (project in file("test-common")).
  settings(commonSettings: _*).
  disablePlugins(sbtassembly.AssemblyPlugin).
  settings(
    name := "test-common",
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  ).dependsOn(common)

lazy val fileTemplate = (project in file("fileTemplate")).
  settings(commonSettings: _*).
  settings(
    name := "fileTemplate",
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.specs2" % "specs2_2.11" % "3.7" % Test
    )
  ).dependsOn(common, testCommon % "test->test")
  .aggregate(common)

lazy val root = (project in file("."))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .settings(name := "data-jobs")
  .aggregate(fileTemplate)

