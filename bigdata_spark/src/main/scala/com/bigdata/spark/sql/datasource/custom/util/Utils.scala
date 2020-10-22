package com.bigdata.spark.sql.datasource.custom.util

import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType}

object Utils {

  def castTo(value:String, dataType:DataType) = {
    dataType match {
      case _: DoubleType => value.toDouble
      case _: LongType => value.toLong
      case _: StringType => value
    }
  }

}
