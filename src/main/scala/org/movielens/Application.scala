
package org.movielens

//import org.apache.spark
//import org.apache.spark.sql.SparkSession

object Application {

  def main(args: Array[String]): Unit = {
    println("Launching Application with args:");
    if(args.length == 0) {
      println("need to provide spark master location and data csv directory")
      return
    }
    args.foreach(println);
    val spark_mstr_str = args(0)
    val csv_dir = args(1)
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
    //val spark = SparkSession.builder().master(spark_mstr_str).getOrCreate()

    //val df = spark.read.format("csv").load(csv_dir + "/ratings.csv")
  }

}
