name := "movielens"

version := "0.1"

scalaVersion := "2.12.10"

// set the main class for 'sbt run'
Compile / run / mainClass := Some("org.movielens.Application")

val sparkVersion = "3.1.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)