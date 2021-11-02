
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
    println("\t\trequires option csv_directory needs to have path to where the data csv files are")
    println()
  }

  def printArgs(args: Array[String]): Unit = {
    for (i <- args.indices) {
      val arg: String = args(i)
      printf("args[%d]: %s\n", i, arg)
    }
  }

  def moviesReleasedPerYearQ1( dataDirectory:String, spark:SparkSession): Unit = {
    import spark.implicits._

    val movieInfo_df : DataFrame = getMovieInfoDataFrame( dataDirectory, spark )

    val columnNames = Seq("release_year", "count")
    val release_df : DataFrame = movieInfo_df.filter($"title" rlike "\\(\\d{4}\\)")
      .withColumn("release_year", regexp_extract($"title", "\\(\\d{4}\\)", 0 ))
      .withColumn("count",  lit(1)).select(columnNames.head, columnNames.tail: _*)

    val release_cnts : DataFrame = release_df.groupBy($"release_year").sum("count").orderBy(asc("release_year"))
    release_cnts.write.option("header", true).csv("output/q1")
    //release_cnts.rdd.collect().foreach(println)

  }

  def averageNumberOfGenresPerMoviesQ2( dataDirectory:String, spark:SparkSession ): Unit = {
    import spark.implicits._

    val selectColumns = Seq("movieId", "genre_cnt")
    val movieGenres_df : DataFrame = getMovieInfoDataFrame( dataDirectory, spark )

    val genres_df : DataFrame = movieGenres_df.withColumn("genre_cnt", size(split($"genres", "\\|")))
      .select(selectColumns.head, selectColumns.tail: _*)

    val avg_genres_df : DataFrame = genres_df.select(avg($"genre_cnt"))
    avg_genres_df.write.option("header", true).csv("output/q2")
    //avg_genres_df.show()
  }

  def movieCntTaggedComedyQ5( dataDirectory:String, spark:SparkSession ): Unit = {
    import spark.implicits._

    val movieInfo_df : DataFrame = getMovieInfoDataFrame( dataDirectory, spark )

    val selectColumns = Seq("movieId", "genres")
    val genres_df : DataFrame = movieInfo_df.filter($"genres".rlike("(?i)\\bcomedy\\b"))
      .select(selectColumns.head, selectColumns.tail: _*)
      .withColumn("isComedy", lit(1))

    //val comedy_cnt : DataFrame = genres_df.groupBy($"movieId").sum("isComedy").orderBy(asc("release_year"))
    val comedy_sum_df : DataFrame = genres_df.select(sum($"isComedy"))
    comedy_sum_df.write.option("header", true).csv("output/q5")
    //avg_genres_df.show()
  }

  def main(args: Array[String]): Unit = {
    printArgs(args)
    if(args.length != 1) {
      printUsage()
      System.exit(1)
    }
    val spark: SparkSession = SparkSession.builder.appName("MovieLens Insights").getOrCreate()

    val csvDataDirectory: String = args(0)

    moviesReleasedPerYearQ1(csvDataDirectory, spark)
    averageNumberOfGenresPerMoviesQ2(csvDataDirectory, spark)
    movieCntTaggedComedyQ5(csvDataDirectory, spark)

    val csvFile: DataFrame = getGenomeScoresDataFrame(csvDataDirectory, spark)

    csvFile.printSchema()
    printf("\n>>> Genome Scoring Record Count: %d\n", csvFile.count())
    spark.close()
  }

}
