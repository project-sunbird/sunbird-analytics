package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import org.ekstep.analytics.api.util.{APILogger, ElasticsearchService, JSONUtils, RedisUtil}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisException

import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext
import scala.util.hashing.MurmurHash3

case class ExperimentRequest(fields: Map[String, String])
case class ExperimentData(id: String, name: String, startDate: String, endDate: String, key: String, platform: String, expType: String, userId: String, deviceId: String, userIdMod: Long, deviceIdMod: Long, var modResolved: Boolean = false)

class ExperimentService extends Actor {
  implicit val ec: ExecutionContext = context.system.dispatchers.lookup("experiment-actor")
  implicit val className = "org.ekstep.analytics.api.service.ExperimentService"
  val config: Config = ConfigFactory.load()
  val databaseIndex: Int = config.getInt("redis.experimentIndex")
  val emptyValueExpirySeconds = config.getInt("experimentService.redisEmptyValueExpirySeconds")

  private var jedisConnection: Option[Jedis] = None

  def receive: Receive = {
    case ExperimentRequest(fields: Map[String, String]) => {
      if (fields.nonEmpty) sender() ! getExperiment(fields)
      else sender() ! None
    }
  }

  def getExperiment(fields: Map[String, String]): Option[ExperimentData] = {
    val sorted = SortedMap[String, String]() ++ fields
    val key = sorted.values.mkString(":")
    val experimentStr: String = getCache.get(key)

    if (experimentStr != null) {
      try {
        if (experimentStr.isEmpty) {
          APILogger.log("", Option(Map("comments" -> s"No experiment assigned for key $key")), "ExperimentService")
          None
        } else {
          val data = JSONUtils.deserialize[ExperimentData](experimentStr)
          if (data.expType != null && data.expType.equalsIgnoreCase("modulus")) {
            if (resolve(data)) Some(data) else None
          } else
            Some(data)
        }
      } catch {
        case ex: Exception => {
          APILogger.log("", Option(Map("comments" -> s"Exception while getting experiment data! ${ex.getMessage}")), "ExperimentService")
          None
        }
      }
    } else {
      val result = ElasticsearchService.searchExperiment(fields)

      result match {
        case Right(success) => {
          val res = success.result.hits.hits
          if (res.length > 0 && res.head.sourceAsString.nonEmpty) {
            val data = JSONUtils.deserialize[ExperimentData](res.head.sourceAsString)
            if (data.expType != null && data.expType.equalsIgnoreCase("modulus")) {
              if (resolve(data)) {
                data.modResolved = true
                addCache(key, JSONUtils.serialize(data))
                Some(data)
              } else None
            } else {
              addCache(key, JSONUtils.serialize(data))
              Some(data)
            }
          } else {
            addCache(key, "", emptyValueExpirySeconds)
            None
          }
        }
        case Left(error) => {
          APILogger.log("", Option(Map("comments" -> s"Elasticsearch exception ! ${error.error.reason}")), "ExperimentService")
          None
        }
      }
    }
  }

  def getHashOf(value: String) = {
    MurmurHash3.stringHash(value)
  }

  def moduloOf(value: Long, divisor: Long) = {
    if (divisor > 0 && (value % divisor) == 0) true else false
  }

  def resolve(data: ExperimentData): Boolean = {
    if (!data.modResolved) {
      if (data.userId != null && data.userId.nonEmpty) {
        val userHash = getHashOf(data.userId)
        moduloOf(userHash, data.userIdMod)
      } else if(data.deviceId != null && data.deviceId.nonEmpty) {
        val deviceHash = getHashOf(data.deviceId)
        moduloOf(deviceHash, data.deviceIdMod)
      } else {
        false
      }
    } else if(data.modResolved){
      true
    } else {
      false
    }
  }

  def addCache(key: String, value: String, ttl: Int = 0) = {
    getCache.set(key, value)
    if (ttl > 0) getCache.expire(key, ttl)
  }

  def getCache: Jedis = {
    jedisConnection match {
      case Some(conn) => conn
      case None => {
        try {
          jedisConnection = Some(RedisUtil.getConnection(databaseIndex))
          jedisConnection.get
        } catch {
          case jex: JedisException => {
            APILogger.log("", Option(Map("comments" -> s"redis connection exception trying to reconnect!  ${jex.getMessage}")), "ExperimentService")
            RedisUtil.resetConnection()
            try {
              jedisConnection = Some(RedisUtil.getConnection(databaseIndex))
              jedisConnection.get
            }
          }
        }
      }
    }
  }

}
