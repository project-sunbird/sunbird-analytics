package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.api.util.JSONUtils

case object SaveMetrics

case object IncrementApiCalls
case object IncrementLocationDbHitCount
case object IncrementLocationDbMissCount
case object IncrementLocationDbSuccessCount
case object IncrementLocationDbErrorCount
case object IncrementLogDeviceRegisterSuccessCount
case object IncrementDeviceDbSaveSuccessCount
case object IncrementDeviceDbSaveErrorCount

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SaveMetricsActor extends Actor {

  val config = ConfigFactory.load()
  private val logger = LogManager.getLogger("metrics-logger")

  private var apiCalls: Int = 0
  private var locationDbHitCount: Int = 0
  private var locationDbMissCount: Int = 0
  private var locationDbSuccessCount: Int = 0
  private var locationDbErrorCount: Int = 0
  private var logDeviceRegisterSuccessCount: Int = 0


  override def preStart(): Unit = {
    val metricsPublishInterval: Int = config.getInt("metrics.time.interval.min")
    context.system.scheduler.schedule(initialDelay = 0.seconds, interval = metricsPublishInterval.minutes, self, SaveMetrics)
  }

  def receive = {
    case IncrementApiCalls => apiCalls += 1
    case IncrementLocationDbHitCount => locationDbHitCount += 1
    case IncrementLocationDbMissCount => locationDbMissCount += 1
    case IncrementLocationDbSuccessCount => locationDbSuccessCount += 1
    case IncrementLocationDbErrorCount => locationDbErrorCount += 1
    case IncrementLogDeviceRegisterSuccessCount => logDeviceRegisterSuccessCount += 1
    case SaveMetrics => writeMetricsToLog()
  }

  def resetCounts() = {
    apiCalls = 0
    locationDbHitCount = 0
    locationDbMissCount = 0
    locationDbSuccessCount = 0
    locationDbErrorCount = 0
    logDeviceRegisterSuccessCount = 0
  }

  def writeMetricsToLog() = {
    val data = Map(
      "job-name" -> "DeviceRegisterAPI",
      "api-version" -> "v1",
      "timestamp" -> System.currentTimeMillis(),
      "api-calls" -> apiCalls,
      "location-db-hit-count" -> locationDbHitCount,
      "location-db-success-count" -> locationDbSuccessCount,
      "location-db-miss-count" -> locationDbMissCount,
      "location-db-error-count" -> locationDbErrorCount,
      "log-device-register-success-count" -> logDeviceRegisterSuccessCount)
    logger.info(JSONUtils.serialize(data))
    resetCounts()
  }

}