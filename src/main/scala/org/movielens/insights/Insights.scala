package org.movielens.insights

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, avg, col, explode, explode_outer, lit, regexp_extract, size, split, sum}
import org.movielens.loader.DataFrameLoader
import org.movielens.saver.DataFrameSaver
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Date

class Insights(val dataDirectory: String, val outputDirectory: String, val sparkSession: SparkSession) {

  val dataFrameLoader: DataFrameLoader = new DataFrameLoader(dataDirectory, sparkSession)
  val dataFrameSaver: DataFrameSaver = new DataFrameSaver(outputDirectory)

  def getUniqueGenresList(): List[String] = {
    import sparkSession.implicits._
    val genresDf: DataFrame = dataFrameLoader.loadMovieInfo()
    val uniqueGenresDf: DataFrame = genresDf.withColumn("genre_list", split(col("genres"), "\\|" ))
      .select($"movieId", explode($"genre_list").as("genre_type"))
      .select($"genre_type").dropDuplicates(Seq("genre_type"))

    return uniqueGenresDf.select($"genre_type").map(genre => genre.getString(0)).collect().toList
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

    dataFrameSaver.saveAsCsv("question_1", releaseCountsDf)
  }

  def averageNumberOfGenresPerMovie(): Unit = {
    import sparkSession.implicits._

    val selectColumns = Seq("movieId", "genre_count")
    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()

    val genresDf: DataFrame = movieInfoDf.withColumn("genre_count", size(split($"genres", "\\|")))
      .select(selectColumns.head, selectColumns.tail: _*)

    val averageGenresDf: DataFrame = genresDf.select(avg($"genre_count"))
    dataFrameSaver.saveAsCsv("question_2", averageGenresDf)
  }

  def rankedGenres(): Unit = {
    import sparkSession.implicits._

    var rankedGenres: Map[String, Double] = Map()
    val uniqueGenres : List[String] = getUniqueGenresList()

    for (genre <- uniqueGenres) {
      val selectGenreColumns = Seq("movieId", "genres")
      val movieGenresDF: DataFrame = dataFrameLoader.loadMovieInfo()
        .select(selectGenreColumns.head, selectGenreColumns.tail: _*)

      val selectRatingColumns = Seq("userId", "movieId", "rating")
      val movieRatingsDF: DataFrame = dataFrameLoader.loadRatings( )
        .select(selectRatingColumns.head, selectRatingColumns.tail: _*)

      val ratingsDf: DataFrame = movieRatingsDF.join(movieGenresDF, Seq("movieId"), "left").filter($"genres".rlike(s"(?i)\\b$genre\\b"))
      val avgRatingList: List[Double] = ratingsDf.select(avg($"rating")).map(genre => genre.getDouble(0)).collect().toList

      rankedGenres += (genre -> avgRatingList.head)
    }

    val orderedRankedGenres : DataFrame = rankedGenres.toSeq.toDF("genre", "avg_rating").sort(desc("avg_rating"))
    dataFrameSaver.saveAsCsv("question_3", orderedRankedGenres)
  }

  def movieCountTaggedComedy(): Unit = {
    import sparkSession.implicits._

    val movieTagsDf: DataFrame = dataFrameLoader.loadTags()

    val selectColumns = Seq("movieId", "tag")
    val genresDf: DataFrame = movieTagsDf.filter($"tag".rlike("(?i)\\bcomedy\\b"))
      .select(selectColumns.head, selectColumns.tail: _*)
      .withColumn("isComedy", lit(1))
      .dropDuplicates(Seq("movieId"))

    val comedySumDf : DataFrame = genresDf.select(sum($"isComedy"))
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

  /**
   * Takes a string date in the form: "MM/DD/YYYY" and converts a timestamp
   * in the format to milliseconds since 1970
   * @param date String in format MM/DD/YYYY
   * @return Long timestamp in milliseconds since 1970
   */
  def convertStringDateToMillisTimestamp(date: String): Long = {
    val format: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val dateObject: Date = format.parse(date)
    dateObject.getTime()
  }

  def mostPopularMoviesInTimeRange(beginDate: String, endDate: String, n: Integer): Unit = {
    val beginTs: Long = convertStringDateToMillisTimestamp(beginDate)
    val endTs: Long = convertStringDateToMillisTimestamp(endDate)

    val movieInfoDf: DataFrame = dataFrameLoader.loadMovieInfo()
    val ratingsDf: DataFrame = dataFrameLoader.loadRatings()

    // Select only ratings that occurred between the specified time bounds,
    // Then drop "userId" and "timestamp" leaving only "rating" and "movieId"
    val ratingsWithinPeriodDf: DataFrame = ratingsDf.filter(
      col("timestamp").between(beginTs, endTs)
    ).drop("userId", "timestamp")

    // Group by movieId, taking the average of the rating as the accumulation
    val averageMovieRatingDf: DataFrame = ratingsWithinPeriodDf.groupBy(col("movieId")).avg("rating")

    // Sort by average ratings, descending, and select only top N entries
    val topNMoviesIdsDf: DataFrame = averageMovieRatingDf.sort(col("avg(rating)").desc).limit(n)

    // Add movie title, genres, etc to the top N movies
    val withMovieInfoDf: DataFrame = topNMoviesIdsDf.join(movieInfoDf, usingColumn = "movieId")

    // Save results
    dataFrameSaver.saveAsCsv("question_7", withMovieInfoDf)
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
      movieIdsAndGenresDf, usingColumn = "movieId"
    ).drop("movieId")

    // Group by key "genres", averaging the rating
    val averagedRatingsDf: DataFrame = joinedDf.groupBy("genres").avg("rating")

    // Sort by average rating, descending, and limit to top N entries
    val topNRatingsDf: DataFrame = averagedRatingsDf.sort(col("avg(rating)").desc).limit(n)

    // Save results
    dataFrameSaver.saveAsCsv("question_4", topNRatingsDf)
  }

}
