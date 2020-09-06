package com.bigdata.spark_core.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TwiceSortWork{
   def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("twiceSortWork").setMaster("local")
     val sc = new SparkContext(conf)

     val userRdd: RDD[String] = sc.textFile("D:\\code\\bigdata\\hadoop_offline\\data\\user.txt")
     val accessRdd: RDD[String] = sc.textFile("D:\\code\\bigdata\\hadoop_offline\\data\\access.txt")

     //1000001 bj douyin
     val userPairRdd = userRdd.map(line => {
       val splits = line.split(" ")
       (splits(0), (splits(1), splits(2)))
     })

     //1000001 2019 9 90
     val accessPairRdd: RDD[(String, (String, String, String))] = accessRdd.map(line => {
       val splits = line.split(" ")
       (splits(0), (splits(1), splits(2), splits(3)))
     })

     val joinRdd: RDD[(String, ((String, String), Option[(String, String, String)]))] = userPairRdd.leftOuterJoin(accessPairRdd)

      joinRdd.map(x => {
        val id = x._1
        val city = x._2._1._1
        val name = x._2._1._2
        if(x._2._2.isEmpty){
          (new TwiceSortKey(-1,-1),(id + " " + city + " " + name,"null null null"))
        }else{
          val year = x._2._2.get._1
          val month = x._2._2.get._2
          val traffic = x._2._2.get._3
          (new TwiceSortKey(year.toInt,month.toInt),(id + " " + city + " " + name,year + " " + month + " " + traffic))
        }

      })
        .sortByKey()
       .map(_._2)
       .reduceByKey((x,y) => x + "|" +y)
       .foreach(println(_))

     /**结果是这样，用了reduceByKey后排序又乱了，除非再增加key排序一次,然后再去掉key
      * (1000003 bj douyu,2019 7 5|2019 9 4|2017 8 6)
      * (1000004 sz qqmusic,null null null)
      * (1000002 sh yy,2019 12 20)
      * (1000005 gz huya,null null null)
      * (1000001 bj douyin,2019 9 90)
      */



   }
 }


class TwiceSortKey(val year : Int,val month : Int) extends Ordered[TwiceSortKey] with Serializable{
  override def compare(that: TwiceSortKey): Int = {
    if(this.year != that.year){
      that.year - this.year
    }else{
      this.month - that.month
    }

  }

}
