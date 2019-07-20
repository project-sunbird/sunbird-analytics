package org.ekstep.analytics.api.service.experiment

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import org.ekstep.analytics.api.util.{APILogger, ElasticsearchService, JSONUtils, RedisUtil}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

case class ExperimentRequest(deviceId: String, userId: String, url: String, producer: String)
case class ExperimentData(id: String, name: String, startDate: String, endDate: String, key: String, platform: String, expType: String, userId: String, deviceId: String, userIdMod: Long, deviceIdMod: Long)

class ExperimentService(redisUtil: RedisUtil, elasticsearchService :ElasticsearchService) extends Actor {
  implicit val ec: ExecutionContext = context.system.dispatchers.lookup("experiment-actor")
  implicit val className = "org.ekstep.analytics.api.service.experiment.ExperimentService"
  val config: Config = ConfigFactory.load()
  val databaseIndex: Int = config.getInt("redis.experimentIndex")
  val emptyValueExpirySeconds = config.getInt("experimentService.redisEmptyValueExpirySeconds")

  def receive: Receive = {
    case ExperimentRequest(deviceId, userId, url, producer) => {
      val result = getExperiment(deviceId, userId, url, producer)
      sender ! result
    }
  }

  def getExperiment(deviceId: String, userId: String, url: String, producer: String): Option[ExperimentData] = {
    try {
      val key = keyGen(deviceId, userId, url, producer)
      val experimentStr: String = redisUtil.getKey(key, databaseIndex)

      if (experimentStr != null && experimentStr.isEmpty) {
        APILogger.log("", Option(Map("comments" -> s"No experiment assigned for key $key")), "ExperimentService")
        None
      } else if (experimentStr != null) {
        val data = JSONUtils.deserialize[ExperimentData](experimentStr)
        resolveExperiment(data)
      } else {
        val data = searchExperiment(deviceId, userId, url, producer)

        data match {
          case Some(value) => {
            redisUtil.addCache(key, JSONUtils.serialize(value), databaseIndex)
            resolveExperiment(value)
          }
          case None => {
            redisUtil.addCache(key, "", databaseIndex, emptyValueExpirySeconds)
            None
          }
        }
      }
    } catch {
      case ex: Exception => {
        //ex.printStackTrace()
        APILogger.log("", Option(Map("comments" -> s"Exception while getting experiment data! ${ex.getMessage}")), "ExperimentService")
        None
      }
    }
  }

  def resolveExperiment(data: ExperimentData): Option[ExperimentData] = {
    val typeResolver = ExperimentResolver.getResolver(data.expType)
    typeResolver match {
      case Some(resolver) => if (resolver.resolve(data)) { Some(data) } else None
      case None => Some(data)
    }
  }


  def searchExperiment(deviceId: String, userId: String, url: String, producer: String): Option[ExperimentData]  = {
    val fields = getFieldsMap(deviceId, userId, url, producer)
    val result = elasticsearchService.searchExperiment(fields)

    result match {
      case Right(success) => {
        val res = success.result.hits.hits
        if (res.length > 0 && res.head.sourceAsString.nonEmpty) {
          Some(JSONUtils.deserialize[ExperimentData](res.head.sourceAsString))
        } else None
      }
      case Left(error) => {
        APILogger.log("", Option(Map("comments" -> s"Elasticsearch exception ! ${error.error.reason}")), "ExperimentService")
        None
      }
    }
  }

  def isNullorEmpty(value: String): Boolean = if (value != null && value.nonEmpty) false else true

  def keyGen(deviceId: String, userId: String, url: String, producer: String): String = {
    // key format "deviceId:userId:url:producer"
    var value = ""
    if (isNullorEmpty(deviceId)) value += "NA" else value += deviceId
    if (isNullorEmpty(userId)) value += ":NA" else value += s":$userId"
    if (isNullorEmpty(url)) value += ":NA" else value += s":$url"
    if (isNullorEmpty(producer)) value += ":NA" else value += s":$producer"
    value
  }

  def getFieldsMap(deviceId: String, userId: String, url: String, producer: String): Map[String, String] = {
    var value: mutable.Map[String, String] = mutable.Map()
    if (!isNullorEmpty(deviceId)) value += ("deviceId" -> deviceId)
    if (!isNullorEmpty(userId)) value += ("userId" -> userId)
    if (!isNullorEmpty(url)) value += ("url" -> url)
    if (!isNullorEmpty(producer)) value += ("platform" -> producer)
    value.toMap
  }

}
