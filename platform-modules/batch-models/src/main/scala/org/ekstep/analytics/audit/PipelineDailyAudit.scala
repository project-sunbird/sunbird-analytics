package org.ekstep.analytics.audit

import org.joda.time.format.DateTimeFormatter
import org.apache.spark.SparkContext
import org.ekstep.analytics.audit.PipelineMetricEnum._
import org.ekstep.analytics.framework.AuditStatus.AuditStatus
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JobLogger, RestUtil}
import org.ekstep.analytics.framework._
import org.joda.time.format.DateTimeFormat

object PipelineMetricEnum {
  case class PipelineMetricValue(lookup: String, label: String)

  object SUCCESS_MESSAGE_COUNT extends PipelineMetricValue("success$minusmessage$minuscount", "success")
  object FAILED_MESSAGE_COUNT extends PipelineMetricValue("failed$minusmessage$minuscount", "faield")
  object ERROR_MESSAGE_COUNT extends PipelineMetricValue("error$minusmessage$minuscount", "error")
  object BATCH_SUCCESS_COUNT extends PipelineMetricValue("batch$minussuccess$minuscount", "success")
  object BATCH_ERROR_COUNT extends PipelineMetricValue("batch$minuserror$minuscount", "error")
  object DUPLICATE_EVENT_COUNT extends PipelineMetricValue("duplicate$minusevent$minuscount", "duplicates")
  object CACHE_ERROR_COUNT extends PipelineMetricValue("cache$minuserror$minuscount", "cache-error")
  object PRIMARY_ROUTE_SUCCESS_COUNT extends PipelineMetricValue("primary$minusroute$minussuccess$minuscount", "success")
  object SECONDARY_ROUTE_SUCCESS_COUNT extends PipelineMetricValue("secondary$minusroute$minussuccess$minuscount", "success")
}

object PipelineDailyAudit extends IAuditTask {

  implicit val className: String = "org.ekstep.analytics.audit.PipelineDailyAudit"

  private val apiURL = AppConf.getConfig("druid.sql.host")

  override def name(): String = "PipelineDailyAudit"

  override def computeAuditMetrics(auditConfig: AuditConfig)(implicit sc: SparkContext): AuditOutput = {
    val auditDetails = List(computeMetricsTelemetryValidatorJob(auditConfig),
      computeMetricsDeduplicationJob(auditConfig),
      computeMetricsTelemetryLocationUpdaterJob(auditConfig),
      computeMetricsDenormalisationJob(auditConfig),
      computeMetricsDruidValidatorJob(auditConfig),
      computeMetricsEventsRouterJob(auditConfig)
    )

    val overallStatus = if(auditDetails.count(_.status == AuditStatus.RED) > 0) AuditStatus.RED else AuditStatus.GREEN
    AuditOutput(name = "Pipeline Audit", status = overallStatus, details = auditDetails)
  }

  def queryMetrics(jobName: String, startDateStr: String, endDateStr: String): PipelineMetric = {

    val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val startDate = formatter.parseDateTime(startDateStr)
    val endDate = formatter.parseDateTime(endDateStr)

    val pipelineMetricQuery = AppConf.getConfig("druid.pipeline_metrics.audit.query")
      .format(s"'$jobName'", startDate.toString("yyyy-MM-dd HH:mm:ss"), endDate.plusDays(1).toString("yyyy-MM-dd HH:mm:ss"))

    val response = RestUtil.post[List[PipelineMetric]](apiURL, pipelineMetricQuery)
    response.headOption.getOrElse(PipelineMetric(`job-name` = jobName))
  }

  def computeStatus(threshold: Double, percentageDifference: Double, precision: Double = 0.5): AuditStatus = {
    if (percentageDifference.abs < threshold) AuditStatus.GREEN
    else if ((percentageDifference.abs - threshold).abs < 0.5) AuditStatus.AMBER
    else AuditStatus.RED
  }

  def computeMetricsForJob(sourceJobName: String, sourceJobMetricList: List[String],
                           jobName: String, jobMetricList: List[String], auditConfig: AuditConfig) = {
    val sourceJobMetricValues = queryMetrics(sourceJobName, auditConfig.startDate, auditConfig.endDate).toMap
    val jobMetricValues = queryMetrics(jobName, auditConfig.startDate, auditConfig.endDate).toMap

    val sourceJobOutputCount = sourceJobMetricList.foldLeft(0d){ (sum, element) => sum + sourceJobMetricValues.getOrElse(element, 0d).asInstanceOf[Double] }
    val jobOutputCount = jobMetricList.foldLeft(0d){ (sum, element) => sum + jobMetricValues.getOrElse(element, 0d).asInstanceOf[Double] }

    val percentageDiff = if(sourceJobOutputCount > 0) {
      (sourceJobOutputCount - jobOutputCount) * 100.0 / sourceJobOutputCount
    } else 0d
    val status = computeStatus(auditConfig.threshold, percentageDiff)
    AuditDetails(rule = jobName, stats = Map("input" -> sourceJobOutputCount, "output" -> jobOutputCount), difference = percentageDiff, status)

  }

  def computeMetricsTelemetryValidatorJob(auditConfig: AuditConfig): AuditDetails = {
    JobLogger.log("Computing audit metrics for TelemetryValidator job...")
    val extractorJobMetricList = List(BATCH_SUCCESS_COUNT.lookup)
    val validatorJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup, FAILED_MESSAGE_COUNT.lookup, ERROR_MESSAGE_COUNT.lookup)
    val auditOutput = computeMetricsForJob("TelemetryExtractor", extractorJobMetricList, "TelemetryValidator", validatorJobMetricList, auditConfig)
    JobLogger.log("Audit metrics computation for TelemetryValidator job completed...")
    auditOutput
  }


  def computeMetricsDeduplicationJob(auditConfig: AuditConfig): AuditDetails = {
    JobLogger.log("Computing audit metrics for DeDuplication job...")
    val validatorJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup)
    val dedupJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup, FAILED_MESSAGE_COUNT.lookup,
      DUPLICATE_EVENT_COUNT.lookup, CACHE_ERROR_COUNT.lookup)
    val auditOutput = computeMetricsForJob("TelemetryValidator", validatorJobMetricList, "DeDuplication", dedupJobMetricList, auditConfig)
    JobLogger.log("Audit metrics computation for DeDuplication job completed...")
    auditOutput
  }

  def computeMetricsTelemetryLocationUpdaterJob(auditConfig: AuditConfig): AuditDetails = {
    JobLogger.log("Computing audit metrics for TelemetryLocationUpdater job...")
    val routerJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup)
    val locationUpdaterJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup, FAILED_MESSAGE_COUNT.lookup)
    val auditOutput = computeMetricsForJob("TelemetryRouter", routerJobMetricList, "TelemetryLocationUpdater", locationUpdaterJobMetricList, auditConfig)
    JobLogger.log("Audit metrics computation for TelemetryLocationUpdater job completed...")
    auditOutput
  }

  def computeMetricsDenormalisationJob(auditConfig: AuditConfig): AuditDetails = {
    JobLogger.log("Computing audit metrics for Denormalisation job...")
    val locationUpdaterJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup)
    val denormJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup, FAILED_MESSAGE_COUNT.lookup)
    val auditOutput = computeMetricsForJob("TelemetryLocationUpdater", locationUpdaterJobMetricList, "DeNormalization", denormJobMetricList, auditConfig)
    JobLogger.log("Audit metrics computation for Denormalisation job completed...")
    auditOutput
  }

  def computeMetricsDruidValidatorJob(auditConfig: AuditConfig): AuditDetails = {
    JobLogger.log("Computing audit metrics for DruidValidator job...")
    val denormUpdaterJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup)
    val druidValidatorJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup, FAILED_MESSAGE_COUNT.lookup)
    val auditOutput = computeMetricsForJob("DeNormalization", denormUpdaterJobMetricList, "DruidValidator", druidValidatorJobMetricList, auditConfig)
    JobLogger.log("Audit metrics computation for Denormalisation job completed...")
    auditOutput
  }

  def computeMetricsEventsRouterJob(auditConfig: AuditConfig): AuditDetails = {
    JobLogger.log("Computing audit metrics for EventsRouter job...")
    val druidValidatorJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup)
    val eventRouterJobMetricList = List(SUCCESS_MESSAGE_COUNT.lookup, FAILED_MESSAGE_COUNT.lookup, DUPLICATE_EVENT_COUNT.lookup)
    val auditOutput = computeMetricsForJob("DruidValidator", druidValidatorJobMetricList, "EventsRouter", eventRouterJobMetricList, auditConfig)
    JobLogger.log("Audit metrics computation for EventsRouter job completed...")
    auditOutput
  }

}
