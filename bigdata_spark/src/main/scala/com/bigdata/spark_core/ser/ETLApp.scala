package com.bigdata.spark_core.ser

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/**
 * SimpleDateFormat是线程不安全的
 * *FastDateFormat是线程安全的
 * 但使用mapPartitions,并在分布式算子里new SimpleDateFormat就能避免闭包的问题和线程不安全问题
 **/
object ETLApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val logs = sc.textFile("ruozedata-spark-core/data/ck.log")
//    logs.map(x => {
//      (x,DateUtils.parse(x))
//    }).foreach(println)
    logs.mapPartitions(partition => {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      partition.map(x => {
        (x,format.parse(x).getTime)
      })
    }).foreach(println)

    // ==> IPParser

    sc.stop()
  }
}
