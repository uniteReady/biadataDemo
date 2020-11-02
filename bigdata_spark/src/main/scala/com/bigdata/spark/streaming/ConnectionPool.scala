package com.bigdata.spark.streaming

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource

object ConnectionPool {
  private val dataSource = new ComboPooledDataSource
  dataSource.setJdbcUrl("jdbc:mysql://bigdata:3306/bigdata")
  dataSource.setUser("root")
  dataSource.setPassword("hadoop")
  dataSource.setMinPoolSize(5)
  dataSource.setMaxPoolSize(30)
  dataSource.setInitialPoolSize(10)
  //超时时间
  dataSource.setMaxStatements(100)

  def getConnection(): Connection = {
    val connection: Connection = dataSource.getConnection()
    return connection
  }

  def returnConnection(connection : Connection ): Unit ={
    if(null != connection){
      connection.close()
    }

  }

}
