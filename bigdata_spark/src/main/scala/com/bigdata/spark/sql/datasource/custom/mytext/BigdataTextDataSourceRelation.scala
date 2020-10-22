package com.bigdata.spark.sql.datasource.custom.mytext

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import com.bigdata.spark.sql.datasource.custom.util.{IPUtil, Utils}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class BigdataTextDataSourceRelation( @transient val sqlContext: SQLContext,
                                     path: String) extends BaseRelation
with TableScan
with InsertableRelation
with Logging{

  final val IPDBPATH = "D:\\code\\bigdata\\ip2region\\ip2region\\data\\ip2region.db"
  final val TIMEPATTERN = "[dd/MMM/yyy:HH:mm:ss +0800]"




  override def schema: StructType = StructType(
      StructField("ip", StringType, false) ::
      StructField("proxyIp", StringType, true) ::
      StructField("responseTime", StringType, true) ::
      StructField("referer", StringType, true) ::
      StructField("method", StringType, true) ::
      StructField("url", StringType, true) ::
      StructField("httpCode", StringType, true) ::
      StructField("requestSize", StringType, true) ::
      StructField("responseSize", StringType, true) ::
      StructField("cache", StringType, true) ::
      StructField("uaHead", StringType, true) ::
      StructField("fileType", StringType, true) ::
      StructField("province", StringType, true) ::
      StructField("city", StringType, true) ::
      StructField("operator", StringType, true) ::
      StructField("http", StringType, true) ::
      StructField("domain", StringType, true) ::
      StructField("path", StringType, true) ::
      StructField("year", StringType, true) ::
      StructField("month", StringType, true) ::
      StructField("day", StringType, true) ::
      Nil
  )

  override def buildScan(): RDD[Row] = {
    val logRdd: RDD[String] = sqlContext.sparkContext.textFile(path)
    val structfieldsArray: Array[StructField] = schema.fields

    val splitsRdd : RDD[Array[String]] = logRdd.map(_.split("\t").map(_.trim))
    splitsRdd.map(
      splits =>{
        try {
          val time = splits(0)
          val ip = splits(1)
          val proxyIp = splits(2)
          val responseTime = splits(3)
          val referer = splits(4)
          val method = splits(5)
          val url = splits(6)
          val httpCode = splits(7)
          val requestSize = splits(8)
          val responseSize = splits(9)
          val cache = splits(10)
          val uaHead = splits(11)
          val fileType = splits(12)

          //解析url
          val urlSplits = url.split("://")
          val http = urlSplits(0)
          val domainAndPath = urlSplits(1).split("/", 2)
          val domain = domainAndPath(0)
          val path = domainAndPath(1)

          //解析IP
          val ipMessage = IPUtil.parseIP(ip, IPDBPATH)
          if(ipMessage.contains("内网IP")){
            throw new Exception("内网IP")
          }
          //995|中国|0|上海|上海市|电信|125682
          val ipSplits = ipMessage.split("\\|")
          val province = ipSplits(3)
          val city = ipSplits(4)
          val operator = ipSplits(5)

          //解析时间
          //[24/Aug/2020:17:14:39 +0800]
          val simpleDateFormat = new SimpleDateFormat(TIMEPATTERN, Locale.ENGLISH)
          val date = simpleDateFormat.parse(time)
          val calendar = Calendar.getInstance()
          calendar.setTime(date)
          val year = calendar.get(Calendar.YEAR).toString
          val monthTemp = calendar.get(Calendar.MONTH)
          var dayTemp = calendar.get(Calendar.DAY_OF_MONTH)

          val month = if(monthTemp < 10) "0" + monthTemp else monthTemp.toString
          val day = if(dayTemp < 10) "0" + dayTemp else dayTemp.toString

          List(ip, proxyIp, responseTime, referer, method, url, httpCode, requestSize, responseSize, cache, uaHead, fileType, province, city, operator, http, domain, path,  year, month, day)
        }catch {
          case e : Exception =>{
            e.printStackTrace()
            null
          }
        }
      }
    ).filter(null != _)
      //将数据类型转换成spark的数据类型
      .map(x => x.zipWithIndex.map{
        case(value,index) =>{
          val colDatatype = structfieldsArray(index).dataType
          Utils.castTo(value,colDatatype)
        }
      })
      .map(x => Row.fromSeq(x))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write.mode(if(overwrite) SaveMode.Overwrite else SaveMode.Append).save(path)
  }
}
