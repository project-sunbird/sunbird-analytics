package org.ekstep.analytics.api.util

import java.time.Duration

import com.typesafe.config.{Config, ConfigFactory}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  private val config: Config = ConfigFactory.load()
  private val redis_host = config.getString("redis.host")
  private val redis_port = config.getInt("redis.port")

  private def buildPoolConfig = {
    val poolConfig = new JedisPoolConfig
    poolConfig.setMaxTotal(config.getInt("redis.connection.max"))
    poolConfig.setMaxIdle(config.getInt("redis.connection.idle.max"))
    poolConfig.setMinIdle(config.getInt("redis.connection.idle.min"))
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)
    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(config.getInt("redis.connection.minEvictableIdleTimeSeconds")).toMillis)
    poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(config.getInt("redis.connection.timeBetweenEvictionRunsSeconds")).toMillis)
    poolConfig.setNumTestsPerEvictionRun(3)
    poolConfig.setBlockWhenExhausted(true)
    poolConfig
  }

  private var jedisPool = new JedisPool(buildPoolConfig, redis_host, redis_port)

  def getConnection: Jedis = jedisPool.getResource

  def getConnection(database: Int): Jedis = {
    val conn = jedisPool.getResource
    conn.select(database)
    conn
  }

  def resetConnection(): Unit = {
    jedisPool.close()
    jedisPool = new JedisPool(buildPoolConfig, redis_host, redis_port)
  }
}
