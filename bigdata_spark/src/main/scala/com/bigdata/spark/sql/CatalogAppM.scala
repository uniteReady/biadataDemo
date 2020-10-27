package com.bigdata.spark.sql

import org.apache.spark.sql.SparkSession

object CatalogAppM {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.parallelize(List(1,2,3)).foreach(println(_))

    //获取数据库元数据
    val catalog = spark.catalog
    catalog.listDatabases().show()

    catalog.currentDatabase
    catalog.listTables("bigdata").filter(_.name.contains("access")).show(false)
    catalog.listColumns("bigdata.user").show(false)
    catalog.listFunctions("default").show(false)

    spark.stop()
  }

}
