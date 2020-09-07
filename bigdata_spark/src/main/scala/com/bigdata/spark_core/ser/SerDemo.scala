package com.bigdata.spark_core.ser

import java.net.InetAddress

import bigdata.spark_core.etl.util.FileUtils
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object SerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

//    val output = "output"
//    FileUtils.deleteOutput(sc.hadoopConfiguration,output)

//    val provice = Province
    val provice = new Province2
    sc.textFile("D:\\code\\bigdata\\hadoop_offline\\data\\province.txt")
      .map(x =>{
        val city = provice.map.getOrElse(x, "-")
        val partitionId = TaskContext.getPartitionId()
        val threadId = Thread.currentThread().getId
        val hostName = InetAddress.getLocalHost.getHostName
        (x,city,partitionId,threadId,hostName,provice.toString)
      }).foreach(println(_))


  }

}
