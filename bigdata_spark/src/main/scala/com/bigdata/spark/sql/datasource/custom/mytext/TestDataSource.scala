package com.bigdata.spark.sql.datasource.custom.mytext

import org.apache.spark.sql.SparkSession

object TestDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.format("com.bigdata.spark.sql.datasource.custom.mytext")
      .option("path", "file:///D:/code/bigdata/hadoop_offline/logs/access.log")
//      .option("path", "/bigdata/log/access.log")
      .load()
//    df.
    df.show(false)

//spark.sparkContext.textFile("file:///D:/code/bigdata/hadoop_offline/logs/access.log").foreach(println(_))





    spark.stop()


  }
}
