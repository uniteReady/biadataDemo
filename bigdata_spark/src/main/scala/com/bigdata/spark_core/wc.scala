package com.bigdata.spark_core

import org.apache.spark.{SparkConf, SparkContext}

object wc {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("demo").setMaster("local")
    val sc = new SparkContext(conf)

    sc.parallelize(List(1,2,3))
//      .aggregate()




  }

}
