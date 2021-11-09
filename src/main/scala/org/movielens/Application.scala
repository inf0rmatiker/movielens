
package org.movielens

import org.apache.spark.sql.SparkSession
import org.movielens.insights.Insights

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
    if(args.length != 2) {
      printUsage()
      System.exit(1)
    }

    val sparkSession: SparkSession = SparkSession.builder
      .appName("MovieLens Insights")
      .getOrCreate()

    val csvDataDirectory: String = args(0)
    val outputDirectory: String = args(1)

    val insights: Insights = new Insights(csvDataDirectory, outputDirectory, sparkSession)

    // Complete questions. See HW3 PDF under docs for question details.
    //insights.moviesReleasedPerYear() // Question 1
    //insights.averageNumberOfGenresPerMovie() // Question 2
    //insights.rankedGenres() // Question 3
    // TODO: Question 4
    //insights.movieCountTaggedComedy() // Question 5
    //insights.getAllUniqueGenres() // Question 6
    insights.mostPopularMoviesInTimeRange(beginDate = "03/05/2012", endDate = "08/27/2012", n = 10) // Question 7

    sparkSession.close()
  }

}
