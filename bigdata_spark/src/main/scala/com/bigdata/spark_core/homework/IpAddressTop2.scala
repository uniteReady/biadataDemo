package com.bigdata.spark_core.homework

import org.apache.spark.{SparkConf, SparkContext}

object IpAddressTop2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    sc.textFile("D:\\code\\bigdata\\hadoop_offline\\data\\ipAddress.txt")
          .map((_,1))
          .reduceByKey(_ + _)
      .map(x => {
                val splists = x._1.split(",")
                (splists(0),(splists(1),x._2))
              })
          .sortBy(x => x._2._2,false)
        .foreach(println(_))




//      .map((_,1))
//      .reduceByKey(_ + _)
//      .sortBy(x => x._2,false)
//      .take(2)
//      .foreach(println(_))
//

//      .map(x => {
//        val splists = x.split(",")
//        (splists(0),(splists(1),1))
//      })
//      .reduceByKey((x,y) => {
//        (x._1,x._2 +y._2)
//      })
//      .foreach(println(_))


    sc.stop()

  }

}
