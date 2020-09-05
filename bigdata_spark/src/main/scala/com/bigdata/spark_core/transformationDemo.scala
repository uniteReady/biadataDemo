package com.bigdata.spark_core

import org.apache.spark.{SparkConf, SparkContext}

object transformationDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("demo").setMaster("local")
    val sc = new SparkContext(conf)
    sc.makeRDD(List(1,2,3,4))
      .aggregate(0)(_ + _,_ + _)


    

  }

}
