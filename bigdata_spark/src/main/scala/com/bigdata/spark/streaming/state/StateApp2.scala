package com.bigdata.spark.streaming.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object StateApp2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //设置checkPoint存储目录，否则报错The checkpoint directory has not been set
    ssc.checkpoint("chk")

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    ssc.socketTextStream("bigdata",9527)
        .flatMap(_.split(","))
        .map((_,1))
        .mapWithState(StateSpec.function(mappingFunc).timeout(Seconds(100)))
        .print()



    ssc.start()
    ssc.awaitTermination()
  }


}
