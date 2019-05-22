package org.ekstep.analytics.api.service

import akka.actor.Actor
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.api.util.APIMetrics._
import org.ekstep.analytics.api.util.JSONUtils

case class SaveMetrics(jobName: String)

class SaveMetricsActor extends Actor {

  private val logger = LogManager.getLogger("metrics-logger")

  def receive = {
    case SaveMetrics(jobName: String) => {

      val data = Map("job-name" -> jobName, "api-version" -> "v1", "timestamp" -> System.currentTimeMillis(),
        "api-calls" -> apiCalls, "db-hit-count" -> dbHitCount, "db-success-count" -> dbSuccessCount,
        "db-miss-count" -> dbMissCount, "db-error-count" -> dbErrorCount, "db-save-success-count" -> dbSaveSuccessCount,
        "db-save-error-count" -> dbSaveErrorCount)
      logger.info(JSONUtils.serialize(data))
      // reset counts
      resetCounts()
    }
  }

}