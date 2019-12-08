name := "ImdbAgg"

version := "1.0"

organization := "com.bgcpartners.imdb"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
"org.apache.spark" %% "spark-sql" % "2.2.0" % "provided"
)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v0.16.0" % "test"
