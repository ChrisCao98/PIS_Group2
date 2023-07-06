package Util

import org.apache.spark.sql.ForeachWriter
import redis.clients.jedis.Jedis

class RedisWriter extends ForeachWriter[String] {
  private var jedisClient: Jedis = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    jedisClient = RedisUtil.getJedisClient
    jedisClient != null && jedisClient.isConnected
  }

  override def process(value: String): Unit = {
    val words = value.split("\\W+")
    val key = words(0)
    val vakue = words(1)
    key match {
      case "SPEED" =>
        jedisClient.set(key, vakue)
      case "LOCATION" =>
        jedisClient.set(key, vakue)
      case "IMAGE" =>
        jedisClient.set(key, vakue)
      case _ =>
        println("Wrong command.")
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (jedisClient != null && jedisClient.isConnected) {
      jedisClient.close()
    }
  }
}

