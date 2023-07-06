package Util


import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}



object RedisUtil{
  private val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
  jedisPoolConfig.setMaxTotal(20)
  jedisPoolConfig.setMaxIdle(10)
  jedisPoolConfig.setMinIdle(5)
  jedisPoolConfig.setBlockWhenExhausted(true)
  private val jedisPool : JedisPool = new JedisPool(jedisPoolConfig,"localhost", 6379)

  def getJedisClient : Jedis = synchronized {jedisPool.getResource}

}
