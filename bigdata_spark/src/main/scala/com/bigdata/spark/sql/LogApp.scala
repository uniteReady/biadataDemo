package com.bigdata.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
 * @author PK哥
 **/
object LogApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

   //218.88.0.0,Android,11645,-,POST,https://www.941jackson163.com/?htt=f7,200,411,7281,MISS,四川省,成都市,电信,https,www.941jackson163.com,?htt=f7,2020,09,03
    val df = spark.sparkContext.textFile("D:\\code\\bigdata\\hadoop_offline\\data\\access.log")
      .map(x => {
        val splits = x.split(",")
        val platform = splits(1)
        val traffic = splits(8).toLong
        val province = splits(10)
        val city = splits(11)
        val isp = splits(12)
        (platform, traffic, province, city, isp)
      }).toDF("platform", "traffic", "province", "city", "isp")



    //TODO... 基于platform分组，求每个province访问次数最多的TopN
    df.createOrReplaceTempView("log")
    spark.sql(
      """
        |
        |select a.* from
        |(
        | select t.*, row_number() over(partition by platform order by cnt desc) as r
        | from
        | (select platform,province, count(1) cnt from log group by platform,province) t
        |) a where a.r<=3
        |
        |""".stripMargin).show(100)

    df.groupBy("platform","province")
        .agg(count("*").as("cnt"))
        .select('platform,'province,'cnt,
          row_number().over(Window.partitionBy("platform").orderBy('cnt.desc)).as("r"))
        .filter("r <= 3").show()

//      df.groupBy("platform", "province", "city")
//          .agg(sum("traffic").as("traffics"))
//          .orderBy('traffics.desc)
//          .show()

//    df.createOrReplaceTempView("log")
//    spark.sql(
//      """
//        |
//        |select
//        |platform,province,city,sum(traffic) as traffics
//        |from
//        |log
//        |group by platform,province,city
//        |
//        |""".stripMargin).show()

    spark.stop()
  }
}
