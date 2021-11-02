package org.movielens.insights

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, avg, lit, regexp_extract, size, split, sum}
import org.movielens.loader.DataFrameLoader

class Insights(val dataDirectory: String, val outputDirectory: String, val sparkSession: SparkSession) {

  val dataFrameLoader: DataFrameLoader = new DataFrameLoader(dataDirectory, sparkSession)

  def moviesReleasedPerYear(): Unit = {
    import sparkSession.implicits._

    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()

    val columnNames = Seq("release_year", "count")

    // Example title: "Toy Story (1995)"
    // Add two columns, "release_year" and "count" to the Dataframe
    // - release_year: filters by titles containing a year; extracting the year from the title
    // - count: just a literal 1 at each row
    // Select only those two columns, discarding the rest
    val releaseDf : DataFrame = movieInfoDf.filter($"title" rlike "\\(\\d{4}\\)")
      .withColumn("release_year", regexp_extract($"title", "\\(\\d{4}\\)", 0))
      .withColumn("count",  lit(1))
      .select(columnNames.head, columnNames.tail: _*)

    releaseDf.show(5)

    //
    //val releaseCountsDf: DataFrame = releaseDf.groupBy($"release_year").sum("count").orderBy(asc("release_year"))
    //releaseCountsDf.write.option("header", true).csv("output/q1")
  }

  def averageNumberOfGenresPerMovie(): Unit = {
    import sparkSession.implicits._

    val selectColumns = Seq("movieId", "genre_cnt")
    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()

    val genresDf: DataFrame = movieInfoDf.withColumn("genre_cnt", size(split($"genres", "\\|")))
      .select(selectColumns.head, selectColumns.tail: _*)

    val averageGenresDf: DataFrame = genresDf.select(avg($"genre_cnt"))
    averageGenresDf.write.option("header", true).csv("output/q2")
  }

  def movieCountTaggedComedy(): Unit = {
    import sparkSession.implicits._

    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()

    val selectColumns = Seq("movieId", "genres")
    val genresDf : DataFrame = movieInfoDf.filter($"genres".rlike("(?i)\\bcomedy\\b"))
      .select(selectColumns.head, selectColumns.tail: _*)
      .withColumn("isComedy", lit(1))

    val comedySumDf : DataFrame = genresDf.select(sum($"isComedy"))
    comedySumDf.write.option("header", true).csv("output/q5")
  }

}
