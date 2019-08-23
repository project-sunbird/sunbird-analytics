package org.ekstep.analytics.audit

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


@scala.beans.BeanInfo
case class DataSourceMetrics(datasource: String, blobStorageCount: Double = 0d, druidEventsCount: Double = 0d, percentageDiff: Double = 0d) extends CaseClassConversions

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

    val blobStorageMetrics = config.search.map { fetcherList =>
      fetcherList.map { fetcher =>
        val events = DataFetcher.fetchBatchData[V3Event](fetcher)
        val blobPrefix = fetcher.queries.getOrElse(Array[Query]()).head.prefix.getOrElse("")
        DataSourceMetrics(DruidToBlobMapper(blobPrefix), blobStorageCount = events.count)
      }
    }.getOrElse(List[DataSourceMetrics]())

    /*
    val eventsCount = Map(folderList map { folder =>
      val queryConfig = s"""{ "type": "azure", "queries": [{ "bucket": "$bucket", "prefix": "$folder", "endDate": "$endDate", "startDate": "$startDate" }] }"""
      val events = DataFetcher.fetchBatchData[V3Event](JSONUtils.deserialize[Fetcher](queryConfig))
      (DruidToBlobMapper(folder), events.count)
    }: _*)
    */

    val druidStorageMetrics = getDruidEventsCount(config)

    println(druidStorageMetrics.mkString("\n"))

    val auditMetrics = blobStorageMetrics.map { blobMetrics => {
        val druidMetrics = druidStorageMetrics.find(_.datasource == blobMetrics.datasource).getOrElse(DataSourceMetrics(blobMetrics.datasource))
        val blobCount = blobMetrics.blobStorageCount
        val druidCount = druidMetrics.druidEventsCount
        val diff = if (blobCount > 0) (blobCount - druidCount) * 100 / blobCount else 0
        DataSourceMetrics(blobMetrics.datasource, blobStorageCount = blobCount, druidEventsCount = druidCount, percentageDiff = diff)
      }
    }

    val result = auditMetrics.map { metrics => {
         AuditDetails(
           rule = config.name,
           stats = metrics.toMap,
           difference = metrics.percentageDiff,
           status = computeStatus(config.threshold, metrics.percentageDiff)
         )
      }
    }

    JobLogger.log("Computing events count from Blob and Druid datasource job completed...")
    result
  }

  def getDruidEventsCount(auditConfig: AuditConfig): List[DataSourceMetrics] = {
    val apiURL = AppConf.getConfig("druid.sql.host")
    val countQuery = AppConf.getConfig("druid.datasource.count.query")

    val params = auditConfig.params.getOrElse(Map())
    val daysMinus = params("syncMinusDays").asInstanceOf[Int]
    val daysPlus = params("syncPlusDays").asInstanceOf[Int]
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val startDateTime = formatter.parseDateTime(auditConfig.startDate)
    val endDateTime = formatter.parseDateTime(auditConfig.endDate)

    val queryMap: Map[String, String] = Map(
      TelemetryEvents -> countQuery.format(
        TelemetryEvents,
        s"${auditConfig.startDate} 00:00:00",
        s"${auditConfig.endDate} 23:59:00",
        startDateTime.minusDays(daysMinus).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"),
        endDateTime.plusDays(daysPlus).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")
      ),
      SummaryEvents -> countQuery.format(
        SummaryEvents,
        s"${auditConfig.startDate} 00:00:00",
        s"${auditConfig.endDate} 23:59:00",
        startDateTime.minusDays(daysMinus).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"),
        endDateTime.plusDays(daysPlus).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")
      )
    )

    queryMap.flatMap { case (datasource, query) =>
      val response = RestUtil.post[List[Long]](apiURL, query).headOption
      response.map {
        result => DataSourceMetrics(datasource, druidEventsCount = result.toDouble)
      }
    }.toList

  }

  def computeStatus(threshold: Double, diff: Double): AuditStatus.Value = {
    if(diff < threshold) AuditStatus.GREEN
    else if((diff - threshold).abs < 0.5) AuditStatus.AMBER
    else AuditStatus.RED
  }
}
