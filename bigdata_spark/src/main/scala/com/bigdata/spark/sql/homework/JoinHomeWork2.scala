package com.bigdata.spark.sql.homework

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinHomeWork2 {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()


    val df: DataFrame = spark.read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("sep", ",")
      .load("D:\\code\\bigdata\\hadoop_offline\\data\\product.csv")

    val productCategoryDF: DataFrame = spark.read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("sep", ",")
      .load("D:\\code\\bigdata\\hadoop_offline\\data\\product.csv")

    df.createOrReplaceTempView("user_info")
    productCategoryDF.createOrReplaceTempView("product_info")


    val joinedDF: DataFrame = broadcast(spark.table("product_info"))
      .join(spark.table("user_info"), "product_id")

    joinedDF.select("user_id", "category_id").show()


    spark.stop()


  }


}
