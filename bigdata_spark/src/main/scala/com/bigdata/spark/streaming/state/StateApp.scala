package com.bigdata.spark.streaming.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //设置checkPoint存储目录，否则报错The checkpoint directory has not been set
    ssc.checkpoint("chk")

    ssc.socketTextStream("bigdata",9527)
        .flatMap(_.split(","))
        .map((_,1))
        .updateStateByKey(updatefunc)
        .print()


    ssc.start()
    ssc.awaitTermination()
  }


  def updatefunc(newValue : Seq[Int],preValue : Option[Int]): Option[Int] = {
    val sum = newValue.sum
    val pre = preValue.getOrElse(0)
    Some(pre + sum)
  }

}
