package com.bigdata.spark_core.bigdata.etl



import java.util.logging.Logger

import bigdata.spark_core.etl.util.{Access, FileUtils, LogParser}
import com.bigdata.spark_core.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}


object RddEtl {
//  val ipDbPath = "D:\\code\\bigdata\\ip2region\\ip2region\\data\\ip2region.db";
private val logger: Logger = Logger.getLogger("RddEtl")

  def main(args: Array[String]): Unit = {
//    if (args.length != 2){
//        logger.severe("need 2 args")
//        System.exit(1)
//    }
    val input= "D:\\code\\bigdata\\hadoop_offline\\logs\\access.log"
    val output = "./output"
//    val input = args(0)
//    val output = args(1)

//    val sc = SparkUtil.setMaster("yarn")
    val sc = SparkUtil.setMaster("local")

    var access = new Access

    val configuration = sc.hadoopConfiguration
    //若输出路径存在删除输出路径
    FileUtils.deleteOutput(configuration,output)
     sc.textFile(input)
//       .map(_.split("\t"))
//       .filter(x=> !(x(9).equals("-")|x(9).equals("脏数据")))
       .map(log => {
         ////995|中国|0|上海|上海市|电信|125682
//         val ipSplits = IPUtil.parseIP(log(1), ipDbPath).split("\\|")
//         access.setIp(log(1))
         LogParser.etlParse(log)

       }
    ).filter(_ != null).saveAsTextFile(output)



  }

}
