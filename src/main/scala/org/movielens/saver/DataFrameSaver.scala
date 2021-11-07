package org.movielens.saver

import org.apache.spark.sql.DataFrame

class DataFrameSaver(val outputDirectory: String) {

  def saveAsCsv(filename: String, df: DataFrame): Unit = {
    df.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(s"$outputDirectory/$filename")
  }

}
