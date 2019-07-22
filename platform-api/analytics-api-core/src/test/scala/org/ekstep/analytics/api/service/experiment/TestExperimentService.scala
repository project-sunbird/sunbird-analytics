package org.ekstep.analytics.api.service.experiment

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import com.sksamuel.elastic4s.http.{ElasticError, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.http.search.SearchResponse
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.service.experiment.Resolver.ModulusResolver
import org.ekstep.analytics.api.util.{ElasticsearchService, JSONUtils, RedisUtil}
import org.mockito.Mockito._
import redis.clients.jedis.Jedis

import scala.concurrent.Future
import scala.util.{Failure, Success}

class TestExperimentService extends BaseSpec {
  val redisUtilMock = mock[RedisUtil]
  val elasticsearchServiceMock = mock[ElasticsearchService]
  implicit val actorSystem = ActorSystem("testActorSystem", config)

  val experimentService = TestActorRef(new ExperimentService(redisUtilMock, elasticsearchServiceMock)).underlyingActor
  val redisIndex: Int = config.getInt("redis.experimentIndex")
  val emptyValueExpirySeconds = config.getInt("experimentService.redisEmptyValueExpirySeconds")
  implicit val executor =  scala.concurrent.ExecutionContext.global
  implicit val jedisConnection = redisUtilMock.getConnection(redisIndex)

  override def beforeAll() {
    ExperimentResolver.register(new ModulusResolver())
  }

  "Experiment Service" should "return experiment if it is defined for UserId/DeviceId" in {
    val userId = "user1"
    val deviceId = "device1"
    val url = "http://diksha.gov.in/home"
    val experimentData: ExperimentData = JSONUtils.deserialize[ExperimentData](Constants.EXPERIMENT_DATA)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), Some(url), None)
    val key = experimentService.keyGen(Some(deviceId), Some(userId), Some(url), None)

    when(elasticsearchServiceMock.searchExperiment(fields))
      .thenReturn(Future(Some(experimentData)))

    when(redisUtilMock.getKey(key)).thenReturn(None)

    val result = experimentService.getExperiment(Some(deviceId), Some(userId), Some(url), None)

    result onComplete {
      case Success(data) => data match {
        case Some(value) => {
          value.userId should be("user1")
          value.key should be("325324123413")
          value.id should be("exp1")
          value.name should be("first-exp")

          verify(redisUtilMock, times(1)).addCache(key, JSONUtils.serialize(result))
        }
      }
      case Failure(exception) => exception.printStackTrace()
    }

  }

  it should "return None if no experiment is defined" in {
    reset(elasticsearchServiceMock)
    reset(redisUtilMock)
    // no experiment defined for this input
    val userId = "user45"
    val deviceId = "device45"
    val key = experimentService.keyGen(Some(deviceId), Some(userId), None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), None, None)

    when(elasticsearchServiceMock.searchExperiment(fields))
      .thenReturn(Future(None))
    when(redisUtilMock.getKey(key)).thenReturn(None)

    val result = experimentService.getExperiment(Some(deviceId), Some(userId), None, None)

    result onComplete {
      case Success(data) =>
        data should be(None)
        verify(redisUtilMock, times(1)).addCache(key, "NO_EXPERIMENT_ASSIGNED", emptyValueExpirySeconds)(null)
      case Failure(exception) => exception.printStackTrace()
    }

  }

  it should "should evaluate 'modulus' type experiment and return response" in {
    reset(elasticsearchServiceMock)
    reset(redisUtilMock)
    // no experiment defined for this input
    val userId = "user3"
    val deviceId = "device3"
    val key = experimentService.keyGen(Some(deviceId), Some(userId), None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), None, None)
    val experimentData = JSONUtils.deserialize[ExperimentData](Constants.MODULUS_EXPERIMENT_DATA)

    when(elasticsearchServiceMock.searchExperiment(fields))
      .thenReturn(Future(Some(experimentData)))
    when(redisUtilMock.getKey(key)).thenReturn(None)

    val result = experimentService.getExperiment(Some(deviceId), Some(userId), None, None)
    result onComplete {
      case Success(data) => data match {
        case Some(value) => {
          value.userId should be("user3")
          value.key should be("modulus-exp-key-2")
          value.expType should be("modulus")
          value.id should be("modulus-exp-2")
          value.name should be("modulus-exp-2")

          verify(redisUtilMock, times(1)).addCache(key, JSONUtils.serialize(value))
        }
      }
      case Failure(exception) => exception.printStackTrace()
    }
  }

  it should "should evaluate 'modulus' type experiment and return none if modulus is false" in {
    reset(elasticsearchServiceMock)
    reset(redisUtilMock)
    // no experiment defined for this input
    val userId = "user4"
    val deviceId = "device4"
    val key = experimentService.keyGen(Some(deviceId), Some(userId), None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), None, None)
    val experimentData = JSONUtils.deserialize[ExperimentData](Constants.MODULUS_EXPERIMENT_DATA_NON_ZERO)

    when(elasticsearchServiceMock.searchExperiment(fields))
      .thenReturn(Future(Some(experimentData)))
    when(redisUtilMock.getKey(key)).thenReturn(None)

    val result = experimentService.getExperiment(Some(deviceId), Some(userId), None, None)

    result onComplete {
      case Success(data) => {
        data should be(None)

        verify(redisUtilMock, times(1)).addCache(key, JSONUtils.serialize(result))
      }
      case Failure(exception) => exception.printStackTrace()
    }
  }

  it should "return data from cache if the experiment result is cached" in {
    reset(elasticsearchServiceMock)
    reset(redisUtilMock)
    // no experiment defined for this input
    val userId = "user1"
    val deviceId = "device1"
    val key = experimentService.keyGen(Some(deviceId), Some(userId), None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), None, None)

    when(redisUtilMock.getKey(key)).thenReturn(Option(Constants.EXPERIMENT_DATA))

    val result = experimentService.getExperiment(Some(deviceId), Some(userId), None, None)



    result onComplete {
      case Success(data) => data match {
        case Some(value) => {
          value.userId should be("user1")
          value.key should be("325324123413")
          value.id should be("exp1")
          value.name should be("first-exp")

          // should not call elasticsearch when data is present in redis
          verify(elasticsearchServiceMock, times(0)).searchExperiment(fields)
        }
      }
      case Failure(exception) => exception.printStackTrace()
    }
  }
}