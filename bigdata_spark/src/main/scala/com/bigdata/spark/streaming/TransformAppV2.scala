package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object TransformAppV2 extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName).setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val blacks = List("ruoze")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map((_,true))

    // ruoze,123456789     pk,987654321
    val lines = ssc.socketTextStream("ruozedata001", 9527)
    lines.map(x => {
      val splits = x.split(",")
      (splits(0), x)
    }).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => {
          x._2._2.getOrElse(false) != true
        }).map(x=>x._2._1)
    }).print()



    // TODO... RDD JOIN RDD


    ssc.start()
    ssc.awaitTermination()

  }

}
