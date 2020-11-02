package com.bigdata.spark.streaming

import java.sql.{DriverManager,Connection}

object MySQLUtils {

  def getConnection() ={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://bigdata:3306/bigdata","root","hadoop")
  }

  def closeConnection(connection : Connection): Unit ={
    if(null != connection ){
      connection.close()
    }
  }

}
