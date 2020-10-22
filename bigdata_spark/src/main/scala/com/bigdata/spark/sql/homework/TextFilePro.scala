package com.bigdata.spark.sql.homework

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class TextFilePro(val sc: SparkContext) {

  def defaultMinPartitions: Int = sc.defaultMinPartitions

  def textFile(path: String, paths: String*)(minPartitions: Int): RDD[String] = {
    val pathsArray = new Array[String](paths.length + 1)
    pathsArray(0) = path
    var i = 1
    for (elem <- paths.iterator) {
      pathsArray(i) = elem
      i += 1
    }
    sc.union(pathsArray.map(x => {
      sc.textFile(x, minPartitions)
    }))
  }

  def textFile(paths: String*)(minPartitions: Int): RDD[String] = {
    val firstPath = paths(0)
    paths.slice(1, paths.length)
    textFile(firstPath, paths.slice(1, paths.length).toArray: _*)(minPartitions)
  }


  def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {
    val paths: Array[String] = path.split("|")
    textFile(paths: _*)(minPartitions)
  }

}

object TextFilePro{
  implicit def tf2tfPro(sc: SparkContext): TextFilePro = new TextFilePro(sc)

  def main(args: Array[String]): Unit = {
    val path1 = "D:\\code\\bigdata\\hadoop_offline\\data\\user.txt"
    val path2 = "D:\\code\\bigdata\\hadoop_offline\\data\\user2.txt"
    val paths = path1 :: path2  :: Nil

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.textFile(paths.mkString("|")).foreach(println)
    sc.stop()
  }
}