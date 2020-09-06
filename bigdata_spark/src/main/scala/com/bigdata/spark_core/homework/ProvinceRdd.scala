package com.bigdata.spark_core.homework

import org.apache.spark.{SparkConf, SparkContext}

object ProvinceRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("provinceRdd").setMaster("local")
    val sc = new SparkContext(conf)

    //61.189.128.0,-,69305,-,GET,https://www.941jacksonsina.com.cn/?m=7&method=ff,200,367,4943,MISS,贵州省,贵阳市,电信,https,www.941jacksonsina.com.cn,?m=7&method=ff,2020,09,03
    sc.textFile("output\\part-00000")
      .map(x => {
        val splits = x.split(",")
        val province = splits(10)
        val responseSize = splits(8)
        (province,responseSize)
      })
      .reduceByKey(_ + _)
      .foreach(println(_))


  }

}
