package org.ekstep.analytics.audit

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework._
import org.joda.time.DateTime


@scala.beans.BeanInfo
case class EventsCount(total: Long)

object DruidVsPipelineEventsDailyAudit  extends IAuditTask {
  implicit val className: String = "org.ekstep.analytics.audit.DruidVsPipelineEventsDailyAudit"

  override def name(): String = "DruidVsPipelineEventsDailyAudit"

  val TelemetryEvents = "telemetry-events"
  val SummaryEvents = "summary-events"
  val DenormalizedRaw = "telemetry-denormalized/raw/"
  val DenormalizedSummary = "telemetry-denormalized/summary/"

  val DruidToBlobMapper = Map(
    DenormalizedRaw -> TelemetryEvents,
    DenormalizedSummary -> SummaryEvents
  )

  override def computeAuditMetrics(auditConfig: AuditConfig)(implicit sc: SparkContext): AuditOutput = {
    val auditDetails = List(computeTelemetryEventsCount(auditConfig)).flatten
    val overallStatus = if(auditDetails.count(_.status == AuditStatus.RED) > 0) AuditStatus.RED else AuditStatus.GREEN
    AuditOutput(name = "Pipeline Vs Druid events audit", status = overallStatus, details = auditDetails)
  }

  def computeTelemetryEventsCount(config: AuditConfig)(implicit sc: SparkContext): List[AuditDetails] = {
    JobLogger.log("Computing events count from Blob and Druid datasource...")
    val params = config.params.getOrElse(Map())
    val bucket = params("bucket").asInstanceOf[String]
    val folderList = params.getOrElse("prefix", List()).asInstanceOf[List[String]]
    val endDate = params("endDate").asInstanceOf[String]
    val startDate = params("startDate").asInstanceOf[String]

    val eventsCount = Map(folderList map { folder =>
      val queryConfig = s"""{ "type": "azure", "queries": [{ "bucket": "$bucket", "prefix": "$folder", "endDate": "$endDate", "startDate": "$startDate" }] }"""
      val events = DataFetcher.fetchBatchData[V3Event](JSONUtils.deserialize[Fetcher](queryConfig)).cache()
      (DruidToBlobMapper(folder), events.count)
    }: _*)

    val druidCount = getDruidEventsCount(config)

    val countDiff = eventsCount flatMap { case (source, count) => {
        val druidEventCount = druidCount(source)
        val diff = if (count > 0) (count.toDouble - druidEventCount) * 100 / count.toDouble else 0
        Map(source -> diff)
      }
    }

    val result = countDiff map { case (source, percentage) => {
         AuditDetails(
           rule = config.name,
           stats = Map(source -> percentage),
           difference = percentage,
           status = computeStatus(config.threshold, percentage)
         )
      }
    }

    JobLogger.log("Computing events count from Blob and Druid datasource job completed...")
    result.toList
  }

  def getDruidEventsCount(auditConfig: AuditConfig): Map[String, Double] = {
    val apiURL = AppConf.getConfig("druid.sql.host")
    val params = auditConfig.params.getOrElse(Map())
    val endDate = params("endDate")
    val daysMinus = params("syncMinusDays").asInstanceOf[Int]
    val daysPlus = params("syncPlusDays").asInstanceOf[Int]

    val countQuery = "{ \"query\": \"SELECT COUNT(*) AS \\\"total\\\" FROM \\\"druid\\\".\\\"%s\\\" WHERE TIME_FORMAT(MILLIS_TO_TIMESTAMP(\\\"syncts\\\"), 'yyyy-MM-dd HH:mm:ss.SSS', 'Asia/Kolkata') BETWEEN TIMESTAMP '%s' AND '%s' AND  \\\"__time\\\" BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'\" }"
    val queryMap: Map[String, String] = Map(
      TelemetryEvents -> countQuery.format(
        TelemetryEvents,
        s"$endDate 00:00:00",
        s"$endDate 23:59:00",
        new DateTime().minusDays(daysMinus).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"),
        new DateTime().plusDays(daysPlus).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")
      ),
      SummaryEvents -> countQuery.format(
        SummaryEvents,
        s"$endDate 00:00:00",
        s"$endDate 23:59:00",
        new DateTime().minusDays(daysMinus).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"),
        new DateTime().plusDays(daysPlus).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")
      )
    )

    queryMap flatMap { case (name, query) =>
      val response = RestUtil.post[List[EventsCount]](apiURL, query)
      if(response.nonEmpty) Map[String, Double](name -> response.head.total.toDouble) else Map[String, Double](name -> 0)
    }
  }

  def computeStatus(threshold: Double, diff: Double): AuditStatus.Value = {
    if(diff < threshold) AuditStatus.GREEN
    else if((diff - threshold).abs < 0.5) AuditStatus.AMBER
    else AuditStatus.RED
  }
}
