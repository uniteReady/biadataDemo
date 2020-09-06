package com.bigdata.spark_core.homework

import org.apache.spark.{SparkConf, SparkContext}

object GroupBykeyWork {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("provinceRdd").setMaster("local")
    val sc = new SparkContext(conf)

    sc.parallelize(List("100000,一起看|电视剧|军旅|士兵突击,1,1", "100000,一起看|电视剧|军旅|士兵突击,1,0","100001,一起看|电视剧|军旅|我的团长我的团,1,1"))
      .flatMap(x =>{
        val splits = x.split(",")
        val id = splits(0)
        val nav = splits(1)
        val imp = splits(2).toInt
        val click = splits(3).toInt

        val navs = nav.split("\\|")

        navs.map(x => ((id + x,(imp,click))))
      })
      .groupByKey()
      .mapValues(x =>{
        var imp = 0
        var click = 0
        for (elem <- x) {
          imp += elem._1
          click += elem._2
        }
        (imp,click)
      })
      .foreach(println(_))




  }
}
