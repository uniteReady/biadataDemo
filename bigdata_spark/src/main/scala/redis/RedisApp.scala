package redis

/**
 * @author PK哥
 **/
object RedisApp {

  def main(args: Array[String]): Unit = {

    val jedis = RedisUtils.getJedis
    jedis.select(2)

    val pipeline = jedis.pipelined()
    pipeline.multi()

    try{
      pipeline.set("name","ruoze1")
      val a = 1/0
      pipeline.set("age", "31")

      pipeline.exec()
      pipeline.sync()
      println("写入成功")
    } catch {
      case e:Exception => {
        e.printStackTrace()
        pipeline.discard()
      }
    } finally {
      if(null != pipeline) {
        pipeline.close()
      }
    }


  }
}
