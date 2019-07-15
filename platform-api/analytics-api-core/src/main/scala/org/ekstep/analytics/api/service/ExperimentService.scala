package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import org.ekstep.analytics.api.util.{APILogger, ElasticsearchService, JSONUtils, RedisUtil}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisException

import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext

case class ExperimentRequest(fields: Map[String, String])
case class ExperimentData(id: String, name: String, startDate: String, endDate: String, key: String)

class ExperimentService extends Actor {
  implicit val ec: ExecutionContext = context.system.dispatchers.lookup("experiment-actor")
  implicit val className = "org.ekstep.analytics.api.service.ExperimentationService"
  val config: Config = ConfigFactory.load()
  val databaseIndex: Int = config.getInt("redis.experimentIndex")
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
    val experimentStr = getCache.get(key)

    if (experimentStr != null && !experimentStr.isEmpty) {
      try {
        Some(JSONUtils.deserialize[ExperimentData](experimentStr))
      } catch {
        case ex: Exception => {
          APILogger.log("", Option(Map("comments" -> s"JSON deserialize exception ! ${ex.getMessage}")), "ExperimentationService")
          None
        }
      }
    } else {
      val result = ElasticsearchService.searchExperiment(fields)

      result match {
        case Right(success) => {
          val res = success.result.hits.hits
          if (res.length > 0 && res.head.sourceAsString.nonEmpty) {
            getCache.set(key, res.head.sourceAsString)
            Some(JSONUtils.deserialize[ExperimentData](res.head.sourceAsString))
          }
          else None
        }
        case Left(error) => {
          APILogger.log("", Option(Map("comments" -> s"Elasticsearch exception ! ${error.error.reason}")), "ExperimentationService")
          None
        }
      }
    }
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
            APILogger.log("", Option(Map("comments" -> s"redis connection exception trying to reconnect!  ${jex.getMessage}")), "ExperimentationService")
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
