package org.movielens

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

package object schemas {

  val tagsSchema: StructType = new StructType()
    .add("userId", IntegerType, nullable = false)
    .add("movieId", IntegerType, nullable = false)
    .add("tag", StringType, nullable = false)
    .add("timestamp", LongType, nullable = false)

  val ratingsSchema: StructType = new StructType()
    .add("userId", IntegerType, nullable = false)
    .add("movieId", IntegerType, nullable = false)
    .add("rating", DoubleType, nullable = false)
    .add("timestamp", LongType, nullable = false)

  val movieInfoSchema: StructType = new StructType()
    .add("movieId", IntegerType, nullable = false)
    .add("title", StringType, nullable = false)
    .add("genres", StringType, nullable = false)

  val linksSchema: StructType = new StructType()
    .add("movieId", IntegerType, nullable = false)
    .add("imdbId", IntegerType, nullable = false)
    .add("tmdbId", IntegerType, nullable = false)

  val genomeScoreSchema: StructType = new StructType()
    .add("movieId", IntegerType, nullable = false)
    .add("tagId", IntegerType, nullable = false)
    .add("relevance", DoubleType, nullable = false)

  val genomeTagSchema: StructType = new StructType()
    .add("tagId", IntegerType, nullable = false)
    .add("tag", StringType, nullable = false)

}
