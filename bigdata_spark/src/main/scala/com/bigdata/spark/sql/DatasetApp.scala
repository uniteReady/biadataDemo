package com.bigdata.spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author PK哥
 **/
object DatasetApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._


    val df = spark.read.option("header", "true").option("inferSchema", "true")
      .format("csv")
      .load("ruozedata-spark-sql/data/sales.csv")

    //df.show(true)
    //df.select("transac4tionId").show(true)


    val ds: Dataset[Sales] = spark.read.option("header", "true").option("inferSchema", "true")
      .format("csv")
      .load("ruozedata-spark-sql/data/sales.csv").as[Sales]
    //ds.show(true)

    //ds.map(_.transactionId).show(true)

    println(df.queryExecution.optimizedPlan.numberedTreeString)
    println("-------------------------")
    println(ds.queryExecution.optimizedPlan.numberedTreeString)

    //spark.table("emp").join(spark.table("dept"),"deptno").explain()

    spark.table("emp").createOrReplaceTempView("emp")
    spark.sql(
      """
        |select deptno,count(1) from emp group by deptno
        |
        |""".stripMargin).show(true)

    spark.stop()
  }

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)
}
