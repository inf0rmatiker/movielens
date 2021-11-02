
package org.movielens

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.movielens.insights.Insights
import org.movielens.loader.DataFrameLoader

object Application {

  def printArgs(args: Array[String]): Unit = {
    for (i <- args.indices) {
      val arg: String = args(i)
      printf("args[%d]: %s\n", i, arg)
    }
  }

  def printUsage(): Unit = {
    println("USAGE")
    println("\tBuild:\n\t\tsbt package")
    println("\tSubmit as JAR to Spark cluster:\n\t\t$SPARK_HOME/bin/spark-submit <submit_options> \\")
    println("\t\ttarget/scala-2.13/movielens_2.13-0.1.jar <hdfs_file>")
    println("\t\trequires option csv_directory needs to have path to where the data csv files are")
    println()
  }

  def main(args: Array[String]): Unit = {
    printArgs(args)
    if(args.length != 1) {
      printUsage()
      System.exit(1)
    }
    val sparkSession: SparkSession = SparkSession.builder.appName("MovieLens Insights").getOrCreate()
    val csvDataDirectory: String = args(0)
    val outputDirectory: String = args(1)

    val insights: Insights = new Insights(csvDataDirectory, outputDirectory, sparkSession)

    insights.moviesReleasedPerYear()
    //insights.averageNumberOfGenresPerMovie()
    //insights.movieCountTaggedComedy()

    //val dataFrameLoader: DataFrameLoader = new DataFrameLoader(csvDataDirectory, sparkSession)
    //val genomeScoresDf: DataFrame = dataFrameLoader.loadGenomeScores()

    //genomeScoresDf.printSchema()
    //printf("\n>>> Genome Scoring Record Count: %d\n", genomeScoresDf.count())

    //sparkSession.close()
  }

}
