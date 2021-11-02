package org.movielens.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.movielens.schemas

class DataFrameLoader(val dataDirectory: String, val sparkSession: SparkSession) {

  val TAGS_CSV_STR : String = "tags.csv"
  val RATINGS_CSV_STR : String = "ratings.csv"
  val MOVIES_CSV_STR : String = "movies.csv"
  val LINKS_CSV_STR : String = "links.csv"
  val GENOME_SCORES_CSV_STR : String = "genome-scores.csv"
  val GENOME_TAGS_CSV_STR : String = "genome-tags.csv"

  def loadTags(): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", value = true)
      .schema(schemas.tagsSchema)
      .load(s"$dataDirectory/$TAGS_CSV_STR")
  }

  def loadRatings(): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", value = true)
      .schema(schemas.ratingsSchema)
      .load(s"$dataDirectory/$RATINGS_CSV_STR")
  }

  def loadMovieInfo(): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", value = true)
      .schema(schemas.movieInfoSchema)
      .load(s"$dataDirectory/$MOVIES_CSV_STR")
  }

  def loadLinks(): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", value = true)
      .schema(schemas.linksSchema)
      .load(s"$dataDirectory/$LINKS_CSV_STR")
  }

  def loadGenomeScores(): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", value = true)
      .schema(schemas.genomeScoreSchema)
      .load(s"$dataDirectory/$GENOME_SCORES_CSV_STR")
  }

  def loadGenomeTags(): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", value = true)
      .schema(schemas.genomeTagSchema)
      .load(s"$dataDirectory/$GENOME_TAGS_CSV_STR")
  }

}
