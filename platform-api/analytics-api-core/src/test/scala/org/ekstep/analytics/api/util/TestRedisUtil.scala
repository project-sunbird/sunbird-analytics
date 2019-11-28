package org.ekstep.analytics.api.util

import java.util

import org.ekstep.analytics.api.BaseSpec
import redis.clients.jedis.Jedis
import org.mockito.Mockito._

class TestRedisUtil extends BaseSpec {
  val jedisMock = mock[Jedis]
  implicit val jedisConnection: Jedis = jedisMock

  val redisUtil = new RedisUtil() {
    override def getConnection(database: Int): Jedis = jedisMock

    override def getConnection: Jedis = jedisMock
  }

  "Redis util " should "add key/value to cache" in {
    redisUtil.addCache("foo", "bar", 89000)
    verify(jedisMock, times(1)).set("foo", "bar")
    verify(jedisMock, times(1)).expire("foo", 89000)

    when(jedisMock.set("foo", "bar")).thenThrow(new RuntimeException("connection failure"))
    redisUtil.addCache("foo", "bar", 89000)
  }

  it should "get key from cache" in {
    when(jedisMock.get("foo")).thenReturn("bar")
    redisUtil.getKey("foo")
    verify(jedisMock, times(1)).get("foo")

    when(jedisMock.get("foo")).thenThrow(new RuntimeException("connection failure"))
    redisUtil.getKey("foo")
  }

  it should "get all by key from cache" in {
    val keyValue = new util.HashMap[String, String]()
    keyValue.put("foo", "bar")
    when(jedisMock.hgetAll("key1")).thenReturn(keyValue)
    redisUtil.getAllByKey("key1")
    verify(jedisMock, times(1)).hgetAll("key1")

    when(jedisMock.hgetAll("key1")).thenThrow(new RuntimeException("connection failure"))
    redisUtil.getAllByKey("key1")
  }

  it should "establish connection" in {
    val conn = redisUtil.getConnection(1)
    conn.isInstanceOf[Jedis] should be (true)
  }

}