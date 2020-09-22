package com.bigdata.spark.sql.homework

import org.apache.spark.sql.SparkSession

object GradeSqlImpl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "false")
      .load("D:\\code\\bigdata\\hadoop_offline\\data\\grade.txt")

    /***
     * 1,1,chinese,43
     * 2,1,math,55
     * 3,2,chinese,77
     * 4,2,math,88
     * 5,3,chinese,98
     * 6,3,math,65
     */
    val df2 = df.toDF("id", "code", "course", "score")
    df2.createOrReplaceTempView("grade_table")

//    df2.schema.printTreeString()

    spark.sql(
      """
        |select
        |a.*
        |,b.course
        |,b.score
        |from grade_table a
        |join grade_table b
        |on a.code = b.code
        |where a.course != b.course
        |and a.course = 'chinese'
        |and a.score < b.score
        |""".stripMargin).show()

    spark.close()

  }

}
