package com.bigdata.spark_core.datasource

import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object HbaseRead {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "jackson")

    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val tableName = "bigdata:pc"

    val configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM,"bigdata")
    configuration.set(TableInputFormat.INPUT_TABLE, tableName)

    val admin = new HBaseAdmin(configuration)
    if(!admin.tableExists(tableName)){
      println(s"$tableName not exists!")
    }

    sc.newAPIHadoopRDD(configuration,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
      .foreach{ case (_,result) =>{
        //获取行键
        val key = Bytes.toString(result.getRow())

        //通过列族和列名获取列
        val lennovo =Bytes.toString(result.getValue("laptop".getBytes(),"lennovo".getBytes()))
        println("Row key:"+key+"\tlaptop.lennovo:"+lennovo)
    }}

    admin.close()
    sc.stop()

  }
}
