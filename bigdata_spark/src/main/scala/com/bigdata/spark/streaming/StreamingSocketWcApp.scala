package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingSocketWcApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // nc -lk 9527
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata", 9527)
    dstream.flatMap(_.split(","))
        .map((_,1))
        .reduceByKey(_ + _)
        .print()

//    ssc.textFileStream()


    ssc.start()
    ssc.awaitTermination()

  }

}
