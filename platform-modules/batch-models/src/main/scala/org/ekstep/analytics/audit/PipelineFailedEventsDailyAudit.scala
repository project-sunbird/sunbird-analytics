package org.ekstep.analytics.audit

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{AuditConfig, AuditDetails, AuditOutput, AuditStatus, DataFetcher, Fetcher, IAuditTask, V3Event}

object PipelineFailedEventsDailyAudit extends IAuditTask {
  implicit val className: String = "org.ekstep.analytics.audit.PipelineFailedEventsDailyAudit"

  override def name(): String = "PipelineFailedEventsDailyAudit"

  override def computeAuditMetrics(auditConfig: AuditConfig)(implicit sc: SparkContext): AuditOutput = {
    val auditDetails = List(computeFailedEventsCount(auditConfig)).flatten
    val overallStatus = if(auditDetails.count(_.status == AuditStatus.RED) > 0) AuditStatus.RED else AuditStatus.GREEN
    AuditOutput(name = "Pipeline failed events audit", status = overallStatus, details = auditDetails)
  }

  def computeFailedEventsCount(config: AuditConfig)(implicit sc: SparkContext): List[AuditDetails] = {
    JobLogger.log("Computing events count from failed backup...")
    val params = config.params.getOrElse(Map())
    val bucket = params("bucket").asInstanceOf[String]
    val endDate = params("endDate").asInstanceOf[String]
    val startDate = params("startDate").asInstanceOf[String]

    val queryConfig = s"""{ "type": "azure", "queries": [{ "bucket": "$bucket", "prefix": "failed/", "endDate": "$endDate", "startDate": "$startDate" }] }"""
    val events = DataFetcher.fetchBatchData[V3Event](JSONUtils.deserialize[Fetcher](queryConfig)).cache()
    val totalEvent = events.count()

    val result  = events
      .map(event => event.context.pdata)
      .map {
        case Some(pdata) => ((pdata.id, pdata.pid.getOrElse("UNKNOWN")), 1)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2)
      .collect
      .toList

    val auditDetailsList = result map { value => {
        val diff = if (totalEvent > 0) (value._2.toDouble * 100) / totalEvent.toDouble else 0
        AuditDetails(rule = config.name, stats = Map(value._1.toString() -> value._2), difference = diff, status = computeStatus(config.threshold ,diff))
      }
    }
    JobLogger.log("Computing events count from failed backup job complete...")
    auditDetailsList
  }

  def computeStatus(threshold: Double, diff: Double): AuditStatus.Value = {
    if(diff < threshold) AuditStatus.GREEN
    else if((diff - threshold).abs < 0.5) AuditStatus.AMBER
    else AuditStatus.RED
  }
}
