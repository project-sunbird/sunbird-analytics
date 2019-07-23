package org.ekstep.analytics.api.service.experiment

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import org.ekstep.analytics.api.util.{APILogger, ElasticsearchService, JSONUtils, RedisUtil}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

case class ExperimentRequest(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String])
case class ExperimentData(id: String, name: String, startDate: String, endDate: String, key: String, platform: String, expType: String, userId: String, deviceId: String, userIdMod: Long, deviceIdMod: Long)

class ExperimentService(redisUtil: RedisUtil, elasticsearchService :ElasticsearchService) extends Actor {

  implicit val ec: ExecutionContext = context.system.dispatchers.lookup("experiment-actor")
  implicit val className: String = "org.ekstep.analytics.api.service.experiment.ExperimentService"
  val config: Config = ConfigFactory.load()
  val databaseIndex: Int = config.getInt("redis.experimentIndex")
  val emptyValueExpirySeconds: Int = config.getInt("experimentService.redisEmptyValueExpirySeconds")
  implicit val jedisConnection: Jedis = redisUtil.getConnection(databaseIndex)
  val NoExperiemntAssigned = "NO_EXPERIMENT_ASSIGNED"

  def receive: Receive = {
    case ExperimentRequest(deviceId, userId, url, producer) => {
      val result = getExperiment(deviceId, userId, url, producer)
      val reply = sender()
      result.onComplete {
        case Success(value) => reply ! value
        case Failure(error) => reply ! None
      }
    }
  }

  def getExperiment(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): Future[Option[ExperimentData]] = {
    val key = keyGen(deviceId, userId, url, producer)
    val experimentCachedData = redisUtil.getKey(key)

    experimentCachedData.map {
      expData =>
        if (NoExperiemntAssigned.equals(expData)) {
          APILogger.log("", Option(Map("comments" -> s"No experiment assigned for key $key")), "ExperimentService")
          Future(None)
        } else
          Future(resolveExperiment(JSONUtils.deserialize[ExperimentData](expData)))
    }.getOrElse {
      val data = searchExperiment(deviceId, userId, url, producer)
      data.map { result =>
        result.map { res =>
          redisUtil.addCache(key, JSONUtils.serialize(res))
          resolveExperiment(res)
        }.getOrElse {
          redisUtil.addCache(key, NoExperiemntAssigned, emptyValueExpirySeconds)
          None
        }
      }
    }

  }

  def resolveExperiment(data: ExperimentData): Option[ExperimentData] = {
    val typeResolver = ExperimentResolver.getResolver(data.expType)
    if (typeResolver.resolve(data)) Some(data) else None
  }


  def searchExperiment(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): Future[Option[ExperimentData]]  = {
    val fields = getFieldsMap(deviceId, userId, url, producer)
    elasticsearchService.searchExperiment(fields)
  }

  def keyGen(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): String = {
    // key format "deviceId:userId:url:producer"
    List(
      deviceId.getOrElse("NA"),
      userId.getOrElse("NA"),
      url.getOrElse("NA"),
      producer.getOrElse("NA")
    ).mkString(":").toString
  }

  def getFieldsMap(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): Map[String, String] = {
    val value: mutable.Map[String, String] = mutable.Map()
    if (deviceId.isDefined) value += ("deviceId" -> deviceId.get)
    if (userId.isDefined) value += ("userId" -> userId.get)
    if (url.isDefined) value += ("url" -> url.get)
    if (producer.isDefined) value += ("platform" -> producer.get)
    value.toMap
  }

  override def postStop(): Unit = {
    jedisConnection.close()
  }

}
