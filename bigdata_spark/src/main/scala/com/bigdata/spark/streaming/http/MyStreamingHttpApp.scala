package com.bigdata.spark.streaming.http

import com.alibaba.fastjson.JSON
import com.bigdata.spark.streaming.Keys
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * pk,1111111,116.480881,39.989410
 * ruoze,222222,116.397499,39.908722
 **/
object MyStreamingHttpApp extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("bigdata", 9527)

    lines.foreachRDD( rdd => {
      rdd.foreachPartition( partition => {
//        val httpClient = HttpClients.createDefault()
        //使用连接池
        val httpClient = HttpClients.createMinimal(new PoolingHttpClientConnectionManager())

        partition.foreach( line =>{
          val splits = line.split(",")
          val userId = splits(0)
          val time = splits(1)
          val longtitude = splits(2).toDouble
          val latitude = splits(3).toDouble
          var province = ""

          val url = s"https://restapi.amap.com/v3/geocode/regeo?key=${Keys.password}&location=$longtitude,$latitude"
          val get = new HttpGet(url)
          var response : CloseableHttpResponse = null

          try {
             response = httpClient.execute(get)

            if (null != response) {
              val statusCode = response.getStatusLine.getStatusCode
              val entity = response.getEntity
              if (200 == statusCode) {
                val result = EntityUtils.toString(entity)
                val json = JSON.parseObject(result)

                val regeocode = json.getJSONObject("regeocode")
                if (null != regeocode) {
                  val addressComponent = regeocode.getJSONObject("addressComponent")
                  province = addressComponent.getString("province")
                  logError("###" + province)
                }
              }
            }
          } catch {
            case exception: Exception => exception.printStackTrace()
          } finally {
            if(null != response){
              response.close()
            }
          }
//          Accesss(userId,time,longtitude,latitude,province)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  case class Accesss(userId:String,time:String,longtitude:Double,latitude:Double,province:String)

}
