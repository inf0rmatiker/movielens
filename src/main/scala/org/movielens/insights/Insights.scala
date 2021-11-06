package org.movielens.insights

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, avg, col, explode_outer, lit, regexp_extract, size, split, sum}
import org.movielens.loader.DataFrameLoader
import org.movielens.saver.DataFrameSaver

class Insights(val dataDirectory: String, val outputDirectory: String, val sparkSession: SparkSession) {

  val dataFrameLoader: DataFrameLoader = new DataFrameLoader(dataDirectory, sparkSession)
  val dataFrameSaver: DataFrameSaver = new DataFrameSaver(outputDirectory)

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

  /**
   * Retrieves all unique genres as a DataFrame.
   * @return The unique genres as a DataFrame with column "genre"
   */
  def getAllUniqueGenres(): Unit = {
    import sparkSession.implicits._

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
      .filter(!col("genres").contains("(no genres listed)")) // Remove movies without genres
      .sort(col("genre").desc) // Sort by highest -> lowest

    // Save results
    dataFrameSaver.saveAsCsv("question_6.csv", uniqueGenres)
  }

}
