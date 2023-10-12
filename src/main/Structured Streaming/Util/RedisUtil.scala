package Util


import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


/**
 * This object is to creates a Jedis pool.
 * In order to be able to use it as and when you need to use it.
 */
object RedisUtil{
  private val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
  jedisPoolConfig.setMaxTotal(20)
  jedisPoolConfig.setMaxIdle(10)
  jedisPoolConfig.setMinIdle(5)
  jedisPoolConfig.setBlockWhenExhausted(true)
  private val jedisPool : JedisPool = new JedisPool(jedisPoolConfig,"localhost", 6379)

  def getJedisClient : Jedis = synchronized {jedisPool.getResource}

}
