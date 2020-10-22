package com.bigdata.spark.sql.datasource

import org.apache.spark.sql.SparkSession

object HiveSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sql("show tables").show();
//    spark.sql("create database testdatabase").show();

    spark.close()
  }

}
