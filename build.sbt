name := "movielens"

version := "0.1"

scalaVersion := "2.13.6"

// set the main class for 'sbt run'
Compile / run / mainClass := Some("org.movielens.Application")

val sparkVersion = "3.2.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion
)