package com.bigdata.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer

/**
 *
 * 1) 测试数据进来
 * 2) 白名单、黑名单  www.ruozedata.com    www.ruozedata.com-test
 *
 **/
object TransformApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)


    val blacks = new ListBuffer[(String,Boolean)]()
    blacks.append(("ruoze",true))
    val blacksRDD = sc.parallelize(blacks)


    val logs = new ListBuffer[(String, String)]()
    logs.append(("ruoze", "ruoze,123456789" ))
    logs.append(("pk", "pk,987654321" ))
    val logsRDD = sc.parallelize(logs)

    val joinRDD: RDD[(String, (String, Option[Boolean]))] = logsRDD.leftOuterJoin(blacksRDD)

    joinRDD.filter(x => {
      x._2._2.getOrElse(false) != true
    }).map(x=>x._2._1).foreach(println)

    sc.stop

  }
}
