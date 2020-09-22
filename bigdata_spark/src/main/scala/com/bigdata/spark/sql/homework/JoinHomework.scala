package com.bigdata.spark.sql.homework

import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinHomework {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local").
      getOrCreate()


    val userDf: DataFrame = spark.read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("sep", ",")
      .load("D:\\code\\bigdata\\hadoop_offline\\data\\user.csv")

    val productDF: DataFrame = spark.read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("sep", ",")
      .load("D:\\code\\bigdata\\hadoop_offline\\data\\product.csv")

    userDf.createOrReplaceTempView("user_info")
    productDF.createOrReplaceTempView("product_info")

    val sql =
      """
        |select
        |a.user_id
        |,b.category_id
        |from user_info a left join product_info b
        |on a.product_id = b.product_id
        |where b.category_id is not null
        |""".stripMargin



    userDf.printSchema()
    productDF.printSchema()

    spark.sql(sql).show()


    spark.stop()
  }

}
