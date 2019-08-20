package org.ekstep.analytics.audit

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JobLogger, RestUtil}
import org.ekstep.analytics.framework._
import org.joda.time.DateTime

case class PipelineSequence(sourceJobName: String, jobName: String)

object PipelineDailyAudit extends IAuditTask {

  implicit val className: String = "org.ekstep.analytics.audit.PipelineDailyAudit"

  override def name(): String = "PipelineDailyAudit"

  override def computeAuditMetrics(auditConfig: AuditConfig)(implicit sc: SparkContext): List[AuditOutput] = {
    println("executing pipeline audit")
    JobLogger.log("Computing auditing metrics for TelemetryValidator job...")
    List(computeInput(PipelineSequence(sourceJobName = "TelemetryValidator", jobName = "DeDuplication")),
      computeInput(PipelineSequence(sourceJobName = "DeDuplication", jobName = "TelemetryRouter"))
    )
  }

  def computeInput(jobDetails: PipelineSequence): AuditOutput = {

    val apiURL = AppConf.getConfig("druid.sql.host")
    val pipelineMetricQuery = AppConf.getConfig("druid.pipeline_metrics.audit.query")
      .format(s"'${jobDetails.sourceJobName}', '${jobDetails.jobName}'", new DateTime().minusDays(17).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"),
        new DateTime().minusDays(16).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"))

    val response = RestUtil.post[List[PipelineMetric]](apiURL, pipelineMetricQuery)
    val jobMetrics = response.find(_.`job-name` == jobDetails.jobName).getOrElse(PipelineMetric(`job-name` = jobDetails.jobName))
    val sourceJobMetrics = response.find(_.`job-name` == jobDetails.sourceJobName).getOrElse(PipelineMetric(`job-name` = jobDetails.sourceJobName))

    val sourceJobOutputCount = sourceJobMetrics.`success-message-count`
    val jobSuccessCount = jobMetrics.`success-message-count`
    val jobFailedCount = jobMetrics.`failed-message-count`

    val percentageDiff = (sourceJobOutputCount - (jobSuccessCount + jobFailedCount)) * 100.0 / sourceJobOutputCount

    AuditOutput(name = jobDetails.jobName,
      stats = Map("input" -> sourceJobOutputCount, "output" -> jobSuccessCount, "failed" -> jobFailedCount),
      status = AuditStatus.GREEN,
      details = Some(s"""{"rule":"DeDuplication", "diff": "%.2f%", "status": "green"}""".format(percentageDiff)))

  }

}
