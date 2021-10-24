
package org.movielens

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object Application {

  def printUsage(): Unit = {
    println("USAGE")
    println("\tBuild:\n\t\tsbt package")
    println("\tSubmit as JAR to Spark cluster:\n\t\t$SPARK_HOME/bin/spark-submit <submit_options> \\")
    println("\t\ttarget/scala-2.13/movielens_2.13-0.1.jar <hdfs_file>")
    println()
  }

  def main(args: Array[String]): Unit = {

    if(args.length != 1) {
      printUsage()
      System.exit(1)
    }

    val genomeScoresSchema: StructType = new StructType()
      .add("movieId", IntegerType, false)
      .add("tagId", IntegerType, false)
      .add("relevance", DoubleType, false)

    val spark: SparkSession = SparkSession.builder.appName("MovieLens Insights").getOrCreate()
    val csvFileName: String = args(1)
    val csvFile: DataFrame = spark.read
      .format("csv")
      .option("header", true)
      .schema(genomeScoresSchema)
      .load(csvFileName)

    csvFile.printSchema()
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
    //val spark = SparkSession.builder().master(spark_mstr_str).getOrCreate()

    //val df = spark.read.format("csv").load(csv_dir + "/ratings.csv")
    spark.close()
  }

}
