package com.bigdata.spark_core.homework

import org.apache.spark.{SparkConf, SparkContext}

object IpAddressTop2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    sc.textFile("D:\\code\\bigdata\\hadoop_offline\\data\\ipAddress.txt")
      .map((_,1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2,false)
      .take(2)
      .foreach(println(_))


    sc.stop()

  }

}
