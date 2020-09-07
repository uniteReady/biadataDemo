package com.bigdata.spark_core.ser

import org.apache.commons.lang3.time.FastDateFormat

/**
 *
 **/
object DateUtils {
//  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time:String) = {
    format.parse(time).getTime
  }

}
