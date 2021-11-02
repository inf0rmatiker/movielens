
package org.movielens

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType, StringType}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._

object Application {
  val TAGS_CSV_STR : String = "tags.csv"
  val RATINGS_CSV_STR : String = "ratings.csv"
  val MOVIES_CSV_STR : String = "movies.csv"
  val LINKS_CSV_STR : String = "links.csv"
  val GENOME_SCORES_CSV_STR : String = "genome-scores.csv"
  val GENOME_TAGS_CSV_STR : String = "genome-tags.csv"

  def getMovieInfoSchema(): StructType = {
    return new StructType()
      .add("movieId", IntegerType, nullable=false)
      .add("title", StringType, nullable = false)
      .add("genres", StringType, nullable=false)
  }

  def getMovieInfoDataFrame( dataDirectory:String, spark:SparkSession ): DataFrame = {
    val csvFileName : String = dataDirectory + "/" + MOVIES_CSV_STR
    val movieInfoSchema: StructType = getMovieInfoSchema()
    return spark.read
      .format("csv")
      .option("header", value = true)
      .schema(movieInfoSchema)
      .load(csvFileName)
  }

  def getMovieRatingsSchema(): StructType = {
    return new StructType()
      .add("userid", IntegerType, nullable=false)
      .add("movieId", IntegerType, nullable=false)
      .add("rating", StringType, nullable = false)
      .add("timestamp", StringType, nullable=false)
  }

  def getMovieRatingsDataFrame(dataDirectory:String, spark:SparkSession): DataFrame = {
    val csvFileName : String = dataDirectory + "/" + RATINGS_CSV_STR
    val movieRatingsSchema: StructType = getMovieRatingsSchema()
    return spark.read
      .format("csv")
      .option("header", value = true)
      .schema(movieRatingsSchema)
      .load(csvFileName)
  }

  def getGenomeScoresSchema(): StructType = {
    return new StructType()
      .add("movieId", IntegerType, nullable = false)
      .add("tagId", IntegerType, nullable = false)
      .add("relevance", DoubleType, nullable =false)
  }

  def getGenomeScoresDataFrame(dataDirectory:String, spark:SparkSession): DataFrame = {
    val csvFileName : String = dataDirectory + "/" + GENOME_SCORES_CSV_STR
    val genomeScoresSchema: StructType = getGenomeScoresSchema()
    return spark.read
      .format("csv")
      .option("header", value = true)
      .schema(genomeScoresSchema)
      .load(csvFileName)
  }

  def printUsage(): Unit = {
    println("USAGE")
    println("\tBuild:\n\t\tsbt package")
    println("\tSubmit as JAR to Spark cluster:\n\t\t$SPARK_HOME/bin/spark-submit <submit_options> \\")
    println("\t\ttarget/scala-2.13/movielens_2.13-0.1.jar <hdfs_file>")
    println()
  }

  def printArgs(args: Array[String]): Unit = {
    for (i <- args.indices) {
      val arg: String = args(i)
      printf("args[%d]: %s\n", i, arg)
    }
  }



  def main(args: Array[String]): Unit = {
    printArgs(args)
    if(args.length != 1) {
      printUsage()
      System.exit(1)
    }

    val genomeScoresSchema: StructType = new StructType()
      .add("movieId", IntegerType, nullable = false)
      .add("tagId", IntegerType, nullable = false)
      .add("relevance", DoubleType, nullable =false)

    val spark: SparkSession = SparkSession.builder.appName("MovieLens Insights").getOrCreate()
    val csvFileName: String = args(0)
    val csvFile: DataFrame = spark.read
      .format("csv")
      .option("header", value = true)
      .schema(genomeScoresSchema)
      .load(csvFileName)


    csvFile.printSchema()
    printf("\n>>> Genome Scoring Record Count: %d\n", csvFile.count())
    spark.close()
  }

}
