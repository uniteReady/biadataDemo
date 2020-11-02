package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyForeachRDDApp extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[3]")



    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata", 9527)
    val result: DStream[(String, Long)] = lines.flatMap(_.split(",")).countByValue()

    save2MySQL06(result)

    ssc.start()
    ssc.awaitTermination()

  }

//报org.apache.spark.SparkException: Task not serializable
  def save2MySQL01(result: DStream[(String, Long)]): Unit ={

    result.foreachRDD( rdd => {
      //运行在driver
      val connection = MySQLUtils.getConnection()
      logError(" " + connection)
      rdd.foreach( pair => {
        val sql = s" insert into  wc(word,cnt) values('${pair._1}',${pair._2})"
        //运行在woker
        connection.createStatement().execute(sql)

        MySQLUtils.closeConnection(connection)
      })
    })
  }


  def save2MySQL02(result: DStream[(String, Long)]): Unit ={

    result.foreachRDD( rdd => {

      rdd.foreach( pair => {
        val connection = MySQLUtils.getConnection()
        logError(" " + connection)
        val sql = s"insert into  wc(word,cnt) values('${pair._1}',${pair._2})"
        connection.createStatement().execute(sql)

        MySQLUtils.closeConnection(connection)
      })
    })
  }


  def save2MySQL03(result: DStream[(String, Long)]): Unit ={

    result.foreachRDD( rdd => {
      if(!rdd.isEmpty()){
        rdd.foreachPartition(partition =>{
          val connection = MySQLUtils.getConnection()
          logError(" " + connection)

          partition.foreach( pair => {
            val sql = s"insert into  wc(word,cnt) values('${pair._1}',${pair._2})"
            connection.createStatement().execute(sql)
          })

          MySQLUtils.closeConnection(connection)

        })
      }

    })
  }

//使用c3p0连接池
  def save2MySQL04(result: DStream[(String, Long)]): Unit ={

    result.foreachRDD( rdd => {
      if(!rdd.isEmpty()){
        rdd.foreachPartition(partition =>{
          val connection = ConnectionPool.getConnection()
          logError(" " + connection)

          partition.foreach( pair => {
            val sql = s"insert into  wc(word,cnt) values('${pair._1}',${pair._2})"
            connection.createStatement().execute(sql)
          })
          ConnectionPool.returnConnection(connection)

        })
      }

    })
  }

//批量提交
  def save2MySQL05(result: DStream[(String, Long)]): Unit ={

    result.foreachRDD( rdd => {
      if(!rdd.isEmpty()){
        rdd.foreachPartition(partition =>{
          val connection = ConnectionPool.getConnection()
          logError(" " + connection)

          val sql = s"insert into  wc(word,cnt) values(?,?)"
          val pstmt = connection.prepareStatement(sql)

          partition.foreach{
          case(word,cnt) => {
            pstmt.setString(1,word)
            pstmt.setInt(2,cnt.toInt)
            pstmt.addBatch()
          }}

          pstmt.executeBatch()
          pstmt.close()
          ConnectionPool.returnConnection(connection)

        })
      }

    })
  }



  //计数批量提交
  def save2MySQL06(result: DStream[(String, Long)]): Unit ={

    result.foreachRDD( rdd => {
      if(!rdd.isEmpty()){
        rdd.foreachPartition(partition =>{
          val connection = ConnectionPool.getConnection()
          //关闭自动提交
          connection.setAutoCommit(false)
          logError(" " + connection)

          val sql = s"insert into  wc(word,cnt) values(?,?)"
          val pstmt = connection.prepareStatement(sql)

          partition.zipWithIndex.foreach{
            case((word,cnt),index) => {
              pstmt.setString(1,word)
              pstmt.setInt(2,cnt.toInt)
              pstmt.addBatch()

              if(index != 0 && index % 100 == 0){
                pstmt.executeBatch()
                connection.commit()
              }
            }}

          pstmt.executeBatch()
          connection.commit()

          pstmt.close()
          ConnectionPool.returnConnection(connection)

        })
      }

    })
  }


}
