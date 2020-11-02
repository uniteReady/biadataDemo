package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSQLStreamingApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("bigdata", 9527)

    lines.flatMap(_.split(","))
      .foreachRDD(rdd => {
        val spark= SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val df: DataFrame = rdd.toDF("word")
        df.createOrReplaceTempView("words")

        spark.sql(
          """
            |select word,count(1)
            |from words
            |group by word
            |""".stripMargin
        ).show(false)
      })

    ssc.start()
    ssc.awaitTermination()

  }

}
