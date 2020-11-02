package com.bigdata.spark.streaming.http

import com.alibaba.fastjson.JSON
import com.bigdata.spark.streaming.Keys
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object MyHttpCilentApp {
  def main(args: Array[String]): Unit = {
    val longtitude = 116.480881
    val latitude = 39.989410

    val url = s"https://restapi.amap.com/v3/geocode/regeo?key=${Keys.password}&location=$longtitude,$latitude"

    val httpClient = HttpClients.createDefault()
    val get = new HttpGet(url)
    val response: CloseableHttpResponse = httpClient.execute(get)

    val statusCode = response.getStatusLine.getStatusCode
    val entity: HttpEntity = response.getEntity

    if(200 == statusCode){
      val result = EntityUtils.toString(entity)
      val json = JSON.parseObject(result)
      //注意key是regeocode 不是regeocodes,看API说明
      val regeocode = json.getJSONObject("regeocode")
      if(null != regeocode){
        val addressComponent = regeocode.getJSONObject("addressComponent")
        if(null != addressComponent){
          val province = addressComponent.getString("province")
          println(province)

        }
      }

    }
//    println(response)
  }
}
