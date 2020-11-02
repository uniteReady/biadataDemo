package com.bigdata.spark.sql.homework.m20200920

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object MyTextOutPut {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").enableHiveSupport().getOrCreate()
    val csvPath = "D:\\code\\bigdata\\hadoop_offline\\homework\\normal\\20200920\\access.csv"
    val outputPath = "D:\\code\\bigdata\\hadoop_offline\\bigdata_spark\\src\\main\\scala\\com\\bigdata\\spark\\sql\\homework\\m20200920\\output"
    val accessDf: DataFrame = spark
      .read
      .option("header", "true")
      .option("sep", ",")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .format("csv")
      .load(csvPath)

    accessDf.show(false)
    import spark.implicits._
    val accessDs: Dataset[Access] = accessDf.as[Access]
    accessDs.map(_.toString).write.mode(SaveMode.Overwrite).format("text").save(outputPath)


    spark.stop()
  }

}

case class Access(ip: String, method: String, url: String, province: String, city: String, isp: String) {
  override def toString: String = ip + "\t" + method + "\t" + url + "\t" + province + "\t" + city + "\t" + isp

}
