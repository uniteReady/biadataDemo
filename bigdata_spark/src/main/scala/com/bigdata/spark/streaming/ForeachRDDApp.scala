package com.bigdata.spark.streaming

//import com.ruozedata.utils.ConnectionPool
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDDApp extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName).setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    val lines = ssc.socketTextStream("ruozedata001", 9527)
    val result: DStream[(String, Long)] = lines.flatMap(_.split(",")).countByValue()


    /**
     * TODO... 把DStream保存到数据库中
     *
     * MySQL为例
     */
//    save2MySQL06(result)



    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * DStream==>DB : foreachRDD
   * RDD==>DB:  foreachPartition
   */
  def save2MySQL(result: DStream[(String, Long)]): Unit = {
    result.foreachRDD(rdd => {
      val connection = MySQLUtils.getConnection()  // driver

      logError("...." + connection)

      rdd.foreach(pair => {
        val sql = s"insert into wc(word,cnt) values('${pair._1}', ${pair._2})"
        connection.createStatement().execute(sql)  // executor
      })
      MySQLUtils.closeConnection(connection)
    })
  }

  def save2MySQL02(result: DStream[(String, Long)]): Unit = {
    result.foreachRDD(rdd => {
      rdd.foreach(pair => {
        val connection = MySQLUtils.getConnection()  // executor
        logError("...." + connection)

        val sql = s"insert into wc(word,cnt) values('${pair._1}', ${pair._2})"
        connection.createStatement().execute(sql)  // executor
        MySQLUtils.closeConnection(connection)
      })

    })
  }


  def save2MySQL03(result: DStream[(String, Long)]): Unit = {
    result.foreachRDD(rdd => {
      if(!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          val connection = MySQLUtils.getConnection()  // executor
          logError("...." + connection)

          partition.foreach(pair => {
            val sql = s"insert into wc(word,cnt) values('${pair._1}', ${pair._2})"
            connection.createStatement().execute(sql)  // executor
          })

          MySQLUtils.closeConnection(connection)

        })
      }
    })
  }


  def save2MySQL04(result: DStream[(String, Long)]): Unit = {
    result.foreachRDD(rdd => {
      if(!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          val connection = ConnectionPool.getConnection()  // executor
          logError("...." + connection)

          partition.foreach(pair => {
            val sql = s"insert into wc(word,cnt) values('${pair._1}', ${pair._2})"
            connection.createStatement().execute(sql)  // executor
          })

          ConnectionPool.returnConnection(connection)

        })
      }
    })
  }


  def save2MySQL05(result: DStream[(String, Long)]): Unit = {
    result.foreachRDD(rdd => {
      if(!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          val connection = ConnectionPool.getConnection()  // executor
          logError("...." + connection)

          val sql = s"insert into wc(word,cnt) values(?,?)"
          val pstmt = connection.prepareStatement(sql)

          partition.foreach{
            case (word, cnt) => {
              pstmt.setString(1, word)
              pstmt.setInt(2, cnt.toInt)
              pstmt.addBatch()
            }
          }

          pstmt.executeBatch()
          pstmt.close()
          ConnectionPool.returnConnection(connection)
        })
      }
    })
  }


  def save2MySQL06(result: DStream[(String, Long)]): Unit = {
    result.foreachRDD(rdd => {
      if(!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          val connection = ConnectionPool.getConnection()  // executor
          connection.setAutoCommit(false)
          logError("...." + connection)

          /**
           * index 计数： 1w条一个batch
           * 计数？
           *
           * 可能一个partition的数据比较多，1w的batch对partition进行拆开，然后以多次的batch保存到DB
           */
          val sql = s"insert into wc(word,cnt) values(?,?)"
          val pstmt = connection.prepareStatement(sql)


          partition.zipWithIndex.foreach{
            case ((word, cnt), index) => {
              pstmt.setString(1, word)
              pstmt.setInt(2, cnt.toInt)
              pstmt.addBatch()

              if(index !=0 && index % 5 ==0) {
                pstmt.executeBatch()
                connection.commit()
              }
            }
          }

          pstmt.executeBatch()
          connection.commit()

          pstmt.close()
          ConnectionPool.returnConnection(connection)
        })
      }
    })
  }

}
