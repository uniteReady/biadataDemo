package com.bigdata.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @author PK哥
 **/
object CatalogApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._


    val catalog = spark.catalog

    catalog.listDatabases().show(true)
    catalog.currentDatabase
    catalog.listTables("ruozedata_dw").filter(_.name.contains("emp")).show(true)
    catalog.listColumns("emp").show(true)
    catalog.listFunctions("default").show(true)

    spark.udf.register("str_length", (input:String)=>input.trim.length)

    catalog.listFunctions("default").filter('name === "str_length").show(true)

    spark.stop()
  }
}
