package com.bigdata.spark_core.util

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {

  def setMaster(mode:String) = mode match {
    case "yarn"  => new SparkContext(new SparkConf())
    case    _    => new SparkContext(new SparkConf().setMaster("local").setAppName("localApp"))


  }

}
