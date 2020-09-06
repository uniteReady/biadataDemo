package com.bigdata.spark_core.homework

import bigdata.spark_core.etl.util.FileUtils
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec}

object OutputCompress {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("outputCompress").setMaster("local[1]")
    val sc = new SparkContext(conf)

  //[06/Sep/2020:19:55:06 +0800]	123.184.0.0	-	60033	-	POST	https://www.941jacksonjd.com/?m=7&method=ff	404	128	6679	HIT	Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)	text/html
    val path = "D:\\code\\bigdata\\hadoop_offline\\logs\\access.log"
    val output1 = "outputlog1"
    val output2 = "outputlog2"
    FileUtils.deleteOutput(sc.hadoopConfiguration,output1)
    FileUtils.deleteOutput(sc.hadoopConfiguration,output2)
    val rdd = sc.textFile(path)

    /**
     * logStartTime + "\t" + ip + "\t" + proxyIP + "\t" + responsetime + "\t" + referer + "\t" +
     * method + "\t" + url + "\t" + httpcode + "\t" + requestsize + "\t" + responsesize +
     * "\t" + cacheState + "\t" + ua + "\t" + fileType ;
     */

//    rdd.take(3).map(_.split("\t").length).foreach(println(_))
//    rdd.take(3).foreach(println(_))

val filterRDD: RDD[(String, String, String, String)] = rdd.map(line => {
  val splits = line.split("\t")
  val time = splits(0)
  val url = splits(6)
  val traffic = splits(9)
  val urlSplists = url.split("://")
  val doaminAndPath = urlSplists(1).split("/", 2)
  val domain = doaminAndPath(0)
  val path = doaminAndPath(1)

  (time, domain, traffic, path)
})
  .filter(x => x._2 == "ruoze.ke.qq.com" | x._2 == "www.ruozedata.com")

    val keRDD = filterRDD.filter(_._2 == "ruoze.ke.qq.com")
      .map(x => (x._2,x._1 + " " + x._2 + " " + x._3 + " " + x._4))

    val dataRDD = filterRDD.filter(_._2 == "www.ruozedata.com")
      .map(x => (x._2,x._1 + " " + x._2 + " " + x._3))

    keRDD.saveAsHadoopFile(output1, classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat], classOf[BZip2Codec])

    dataRDD.saveAsHadoopFile(output2, classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat], classOf[GzipCodec])

    sc.stop()

  }



}


class MyMultipleTextOutputFormat extends MultipleTextOutputFormat[String, String] {

  //1)文件名：根据key和value自定义输出文件名
  override def generateFileNameForKeyValue(key: String, value: String, name: String): String = {
    if (key == "ruoze.ke.qq.com") {
      "ruozekeqq.log"
    } else if (key == "www.ruozedata.com") {
      "ruozedata.log"
    } else {
      "error"
    }
  }


  //2)文件内容：默认同时输出key和value。这里指定不输出key。
  override def generateActualKey(key: String, value: String): String = {
    null
  }


}