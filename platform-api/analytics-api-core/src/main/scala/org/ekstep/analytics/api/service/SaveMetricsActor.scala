package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.api.util.JSONUtils

case object SaveMetrics

case object IncrementApiCalls
case object IncrementLocationDbHitCount
case object IncrementLocationDbMissCount
case object IncrementLocationDbSuccessCount
case object IncrementLocationDbErrorCount
case object IncrementDeviceDbSaveSuccessCount
case object IncrementDeviceDbSaveErrorCount

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class SaveMetricsActor extends Actor {

  private val logger = LogManager.getLogger("metrics-logger")

  private var apiCalls: Int = 0
  private var locationDbHitCount: Int = 0
  private var locationDbMissCount: Int = 0
  private var locationDbSuccessCount: Int = 0
  private var locationDbErrorCount: Int = 0
  private var deviceDbSaveSuccessCount: Int = 0
  private var deviceDbSaveErrorCount: Int = 0


  override def preStart(): Unit = {
    val config: Config = ConfigFactory.load()
    val metricsPublishInterval: Int = config.getInt("metrics.time.interval.min")
    context.system.scheduler.schedule(initialDelay = 0.seconds, interval = metricsPublishInterval.minutes, self, SaveMetrics)
  }

  def receive = {
    case IncrementApiCalls => apiCalls += 1
    case IncrementLocationDbHitCount => locationDbHitCount += 1
    case IncrementLocationDbMissCount => locationDbMissCount += 1
    case IncrementLocationDbSuccessCount => locationDbSuccessCount += 1
    case IncrementLocationDbErrorCount => locationDbErrorCount += 1
    case IncrementDeviceDbSaveSuccessCount => deviceDbSaveSuccessCount += 1
    case IncrementDeviceDbSaveErrorCount => deviceDbSaveErrorCount += 1
    case SaveMetrics => writeMetricsToLog()

  }

  def resetCounts() = {
    apiCalls = 0
    locationDbHitCount = 0
    locationDbMissCount = 0
    locationDbSuccessCount = 0
    locationDbErrorCount = 0
    deviceDbSaveSuccessCount = 0
    deviceDbSaveErrorCount = 0
  }

  def writeMetricsToLog() = {
    val data = Map("job-name" -> "DeviceRegisterAPI", "api-version" -> "v1", "timestamp" -> System.currentTimeMillis(),
      "api-calls" -> apiCalls, "db-hit-count" -> locationDbHitCount, "db-success-count" -> locationDbSuccessCount,
      "db-miss-count" -> locationDbMissCount, "db-error-count" -> locationDbErrorCount,
      "db-save-success-count" -> deviceDbSaveSuccessCount, "db-save-error-count" -> deviceDbSaveErrorCount)
    logger.info(JSONUtils.serialize(data))
    resetCounts()
  }

}