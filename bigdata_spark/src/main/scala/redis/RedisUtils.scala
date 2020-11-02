package redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * @author PK哥
 **/
object RedisUtils {

  val config = new JedisPoolConfig
  config.setMaxIdle(10)
  config.setMaxTotal(1000)
  config.setMaxWaitMillis(1000)
  config.setTestOnBorrow(true)

  private val pool = new JedisPool(config, "ruozedata001", 16379)

  def getJedis = pool.getResource

  def main(args: Array[String]): Unit = {
    println(getJedis)
  }
}
