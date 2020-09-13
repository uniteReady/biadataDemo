package com.bigdata.spark_core.basic

import java.util

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val counter = sc.longAccumulator("counter")

    sc.parallelize(List(1,2,3,4))
      .map(counter.add(_))
      .collect()

    println(counter.value)
    sc.stop()

  }

}
