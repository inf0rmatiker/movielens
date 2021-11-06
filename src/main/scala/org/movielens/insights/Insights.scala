package org.movielens.insights

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, avg, col, explode, lit, regexp_extract, size, split, sum}
import org.movielens.loader.DataFrameLoader

class Insights(val dataDirectory: String, val outputDirectory: String, val sparkSession: SparkSession) {

  val dataFrameLoader: DataFrameLoader = new DataFrameLoader(dataDirectory, sparkSession)

  def getUniqueGenresList( ): List[String] = {
    import sparkSession.implicits._
    val genres_df: DataFrame = dataFrameLoader.loadMovieInfo()
    val unique_genres_df: DataFrame = genres_df.withColumn("genre_list", split(col("genres"), "\\|" ))
      .select($"movieId", explode($"genre_list").as("genre_type"))
      .select($"genre_type").dropDuplicates(Seq("genre_type"))

    return unique_genres_df.select($"genre_type").map(genre => genre.getString(0)).collect().toList
  }

  def moviesReleasedPerYear(): Unit = {
    import sparkSession.implicits._
    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()
    val columnNames = Seq("release_year", "count")

    /*
      Example title: "Toy Story (1995)"
      Add two columns, "release_year" and "count" to the Dataframe
      - release_year: filters by titles containing a year; extracting the year from the title
      - count: just a literal 1 at each row
      Select only those two columns, discarding the rest.
      Resulting DataFrame:
      +------------+-----+
      |release_year|count|
      +------------+-----+
      |      (1995)|    1|
      |      (1995)|    1|
      |      (1995)|    1|
      |      (1995)|    1|
      |      (1995)|    1|
      +------------+-----+
     */
    val releaseDf : DataFrame = movieInfoDf.filter($"title" rlike "\\(\\d{4}\\)")
      .withColumn("release_year", regexp_extract($"title", "\\(\\d{4}\\)", 0))
      .withColumn("count",  lit(1))
      .select(columnNames.head, columnNames.tail: _*)

    /*
      Group by "release_year", using the aggregate sum from the "count" column.
      Order by the "release_year" field.
      Resulting DataFrame:

      +------------+----------+
      |release_year|sum(count)|
      +------------+----------+
      |      (1891)|         1|
      |      (1893)|         1|
      |      (1894)|         2|
      |      (1895)|         2|
      |      (1896)|         2|
      +------------+----------+
     */
    val releaseCountsDf: DataFrame = releaseDf
      .groupBy($"release_year")
      .sum("count")
      .orderBy(asc("release_year"))

    releaseCountsDf.write.option("header", value = true).csv(s"$outputDirectory/q1")
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

  def rankedGenres(): Unit =
  {
    import sparkSession.implicits._

    val unique_genres : List[String] = getUniqueGenresList( )
    for ( genre <- unique_genres) {
      val selectGenreColumns = Seq("movieId", "genres")
      val movieGenresDF: DataFrame = dataFrameLoader.loadMovieInfo()
        .select(selectGenreColumns.head, selectGenreColumns.tail: _*)

      val selectRatingColumns = Seq("userId", "movieId", "rating")
      val movieRatingsDF: DataFrame = dataFrameLoader.loadRatings()
        .select(selectRatingColumns.head, selectRatingColumns.tail: _*)

      val ratings_DF: DataFrame = movieRatingsDF.join(movieGenresDF, Seq("movieId"), "left").filter($"genres".rlike(s"(?i)\\b$genre\\b"))
      val avg_ratings_DF: DataFrame = ratings_DF.select(avg($"rating"))
      avg_ratings_DF.show(10, false)
    }
  }

  def movieCountTaggedComedy(): Unit = {
    import sparkSession.implicits._

    val movieTagsDf: DataFrame = dataFrameLoader.loadTags()

    val selectColumns = Seq("movieId", "tag")
    val genres_df : DataFrame = movieTagsDf.filter($"tag".rlike("(?i)\\bcomedy\\b"))
      .select(selectColumns.head, selectColumns.tail: _*)
      .withColumn("isComedy", lit(1))
      .dropDuplicates(Seq("movieId"))

    val comedySumDf : DataFrame = genres_df.select(sum($"isComedy"))
    comedySumDf.write.option("header", true).csv("output/q5")
  }

}
