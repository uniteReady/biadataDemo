package com.bigdata.spark.sql.homework.m20200920

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ContinuousDay {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val  COUNT = 2
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()

    val lines: RDD[String] = sc.textFile("file:///D:/code/bigdata/hadoop_offline/homework/normal/20200920/data.txt")

  lines.map(line => {
      val splits = line.split(",")
      (splits(0), splits(1))
    }).groupByKey()
      .mapValues(dates => {
        //toSet去重重复日期
        val dateAndIndexList: List[(String, Int)] = dates.toSet.toList.sorted.zipWithIndex
        dateAndIndexList.map(x => {
          calendar.setTime(formatter.parse(x._1))
          calendar.add(Calendar.DAY_OF_MONTH, -(x._2))
          (formatter.format(calendar.getTime),x._1)
        }).groupBy(_._1)
        //Map[String, List[(String, String)]]
          .filter(_._2.size >= COUNT)
          .map(x => (x._2.head._2,x._2.last._2,x._2.size))
      })
        .foreach(println(_))

    //(ruoze,List((2020-09-24,2020-09-25,2)))
    //(pk,List((2020-09-20,2020-09-22,3), (2020-09-24,2020-09-25,2)))
    sc.stop()

  }

}
