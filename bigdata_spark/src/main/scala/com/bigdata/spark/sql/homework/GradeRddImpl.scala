package com.bigdata.spark.sql.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 分别使用RDD和Spark SQL实现所有数学课程成绩 大于 语文课程成绩的学生的学号
 */

object GradeRddImpl {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    //1    1     chinese  43
    val pairRdd: RDD[(String, (String, String, String, Int))] = sc.textFile("D:\\code\\bigdata\\hadoop_offline\\data\\grade.txt")
      .map(line => {
        val splits = line.split(",")
        (splits(1), (splits(0), splits(1), splits(2), splits(3).toInt))
      })

    pairRdd.join(pairRdd)
      //(2,((3,2,chinese,77),(3,2,chinese,77)))
        .filter(x => !x._2._1._3.equals(x._2._2._3))
        .groupByKey()
        .mapValues(iter =>{
          iter.head
        })
      /**
      (3,((5,3,chinese,98),(6,3,math,65)))
      (2,((3,2,chinese,77),(4,2,math,88)))
      (1,((1,1,chinese,43),(2,1,math,55)))
       */
      .filter(x => x._2._2._4 > x._2._1._4)
        .foreach(println(_))

    sc.stop()

  }

}
