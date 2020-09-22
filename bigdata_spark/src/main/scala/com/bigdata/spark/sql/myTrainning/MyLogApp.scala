package com.bigdata.spark.sql.myTrainning


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object MyLogApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    //218.88.0.0,Android,11645,-,POST,https://www.941jackson163.com/?htt=f7,200,411,7281,MISS,四川省,成都市,电信,https,www.941jackson163.com,?htt=f7,2020,09,03
    val df = spark.sparkContext.textFile("D:\\code\\bigdata\\hadoop_offline\\data\\access.log")
      .map(line => {
        val splits = line.split(",")
        val platform = splits(1)
        val traffic = splits(8).toLong
        val province = splits(10)
        val city = splits(11)
        val isp = splits(12)
        (platform, traffic, province, city, isp)
      })
      .toDF("platform", "traffic", "province", "city", "isp")

    df.createOrReplaceTempView("traffic")

    spark.sql(
      """
        |
        |""".stripMargin)




    spark.stop()

  }
}
