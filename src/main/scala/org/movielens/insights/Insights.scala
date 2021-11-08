package org.movielens.insights

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, avg, col, explode, explode_outer, lit, regexp_extract, size, split, sum}
import org.movielens.loader.DataFrameLoader
import org.movielens.saver.DataFrameSaver
import org.apache.spark.sql.functions._

class Insights(val dataDirectory: String, val outputDirectory: String, val sparkSession: SparkSession) {

  val dataFrameLoader: DataFrameLoader = new DataFrameLoader(dataDirectory, sparkSession)
  val dataFrameSaver: DataFrameSaver = new DataFrameSaver(outputDirectory)

  def getUniqueGenresList(): List[String] = {
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

    releaseDf.select(split(col("genres"), ""))

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

    dataFrameSaver.saveAsCsv("question_1", releaseCountsDf)
  }

  def averageNumberOfGenresPerMovie(): Unit = {
    import sparkSession.implicits._

    val selectColumns = Seq("movieId", "genre_cnt")
    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()

    val genresDf: DataFrame = movieInfoDf.withColumn("genre_cnt", size(split($"genres", "\\|")))
      .select(selectColumns.head, selectColumns.tail: _*)

    val averageGenresDf: DataFrame = genresDf.select(avg($"genre_cnt"))
    dataFrameSaver.saveAsCsv("question_2", averageGenresDf)
  }

  def rankedGenres(): Unit = {
    import sparkSession.implicits._

    var ranked_Genres: Map[String, Double] = Map()
    val unique_genres : List[String] = getUniqueGenresList()

    for ( genre <- unique_genres) {
      val selectGenreColumns = Seq("movieId", "genres")
      val movieGenresDF: DataFrame = dataFrameLoader.loadMovieInfo()
        .select(selectGenreColumns.head, selectGenreColumns.tail: _*)

      val selectRatingColumns = Seq("userId", "movieId", "rating")
      val movieRatingsDF: DataFrame = dataFrameLoader.loadRatings( )
        .select(selectRatingColumns.head, selectRatingColumns.tail: _*)

      val ratings_DF: DataFrame = movieRatingsDF.join(movieGenresDF, Seq("movieId"), "left").filter($"genres".rlike(s"(?i)\\b$genre\\b"))
      val avg_rating_list: List[Double] = ratings_DF.select(avg($"rating")).map(genre => genre.getDouble(0)).collect().toList

      ranked_Genres += (genre -> avg_rating_list.head )
    }

    val orderdRankedGenres : DataFrame = ranked_Genres.toSeq.toDF("genre", "avg_rating").sort(desc("avg_rating"))
    dataFrameSaver.saveAsCsv("question_3", orderdRankedGenres)
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
    dataFrameSaver.saveAsCsv("question_5", comedySumDf)
  }

  /**
   * Retrieves all unique genres as a DataFrame.
   * @return The unique genres as a DataFrame with column "genre"
   */
  def getAllUniqueGenres(): Unit = {

    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()

    /*
    +--------------------+
    |              genres|
    +--------------------+
    |Adventure|Animati...|
    |Adventure|Childre...|
    |      Comedy|Romance|
    |Comedy|Drama|Romance|
    |              Comedy|
    |Action|Crime|Thri...|
    |      Comedy|Romance|
    +--------------------+
     */
    val allGenres: DataFrame = movieInfoDf.select("genres")

    /*
    +----------------------+
    |          genres_array|
    +----------------------+
    |  [Adventure, Anima...|
    |  [Adventure, Child...|
    |     [Comedy, Romance]|
    |  [Comedy, Drama, R...|
    |              [Comedy]|
    +----------------------+
     */
    val splitGenres: DataFrame = allGenres.select(
      split(col("genres"), "[|]").as("genres_array") // Create array column from split string
    )

    /*
    +-----------+-----+
    |      genre|count|
    +-----------+-----+
    |      Drama|13344|
    |     Comedy| 8374|
    |   Thriller| 4178|
    |    Romance| 4127|
    |     Action| 3520|
    |      Crime| 2939|
    +-----------------+
     */
    val uniqueGenres: DataFrame = splitGenres.select(
      col("genres_array"), explode_outer(col("genres_array")) // Create row for each array element
        .as("genre")) // Name this column as "genre"
      .drop("genres_array") // Drop the original array column
      .groupBy(col("genre")) // Group by genres of the same value
      .count() // Group under the distinct genre counts
      .filter(!col("genre").contains("(no genres listed)")) // Remove movies without genres
      .sort(col("genre").desc) // Sort by highest -> lowest

    // Save results
    dataFrameSaver.saveAsCsv("question_6", uniqueGenres)
  }

  def getTopNGenreCombinations(n: Integer): Unit = {
    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()
    val ratingsDf: DataFrame = dataFrameLoader.loadRatings()

    // Leave only "movieId" and "rating" columns
    val movieIdsAndRatingsDf: DataFrame = ratingsDf.drop("userId", "timestamp")

    // Leave only "movieId" and "genres"
    val movieIdsAndGenresDf: DataFrame = movieInfoDf.drop("title")

    // Inner-join the two on "movieId", then drop "movieId" leaving only "rating" and "genres"
    val joinedDf: DataFrame = movieIdsAndRatingsDf.join(
      movieIdsAndGenresDf, usingColumns = Seq("movieId"), joinType = "inner"
    ).drop("movieId")

    // Group by key "genres", averaging the rating
    val averagedRatingsDf: DataFrame = joinedDf.groupBy("genres").avg("rating")

    // Sort by average rating, descending, and limit to top N entries
    val topNRatingsDf: DataFrame = averagedRatingsDf.sort(col("avg(rating)").desc).limit(n)

    // Save results
    dataFrameSaver.saveAsCsv("question_4", topNRatingsDf)
  }

}
