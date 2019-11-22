name := "Amedeo_Baragiola_hw3"

version := "0.1"

scalaVersion := "2.12.10"

//https://stackoverflow.com/questions/25144484/sbt-assembly-deduplication-found-error
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.6.4"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0"

// https://mvnrepository.com/artifact/com.google.guava/guava
libraryDependencies += "com.google.guava" % "guava" % "14.0.1"
//libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" exclude("org.slf4j", "slf4j-log4j12") exclude("ch.qos.logback", "slf4j-log4j12") exclude("com.google.guava", "guava")

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.4" exclude("org.slf4j", "slf4j-log4j12") exclude("ch.qos.logback", "slf4j-log4j12")

mainClass in (Compile, run) := Some("com.abarag4.hw3.StockSimulatorParallelize")
mainClass in assembly := Some("com.abarag4.hw3.StockSimulatorParallelize")

assemblyJarName in assembly := "amedeo_baragiola_hw3.jar"
