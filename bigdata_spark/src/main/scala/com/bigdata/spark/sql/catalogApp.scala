package com.bigdata.spark.sql

import org.apache.spark.sql.SparkSession

object catalogApp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val catalog = spark.catalog
    catalog.listDatabases().show()

    catalog.currentDatabase

    spark.stop()
  }

}
