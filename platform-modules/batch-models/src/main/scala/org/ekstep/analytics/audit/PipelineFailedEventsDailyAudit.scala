package org.ekstep.analytics.audit

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework._

case class FailedEvents(pdataId: String, pid: String, failedCount: Long) extends CaseClassConversions

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
    
    val auditResults = config.search.map { fetcherList =>
      fetcherList.flatMap {fetcher =>
        val events = DataFetcher.fetchBatchData[V3Event](fetcher)
        val totalEvent = events.count()

        val result = events
          .map(event => event.context.pdata)
          .map {
            case Some(pdata) => ((pdata.id, pdata.pid.getOrElse("UNKNOWN")), 1)
            case None => (("EmptyID", "EmptyPID"), 1)
          }
          .reduceByKey(_ + _)
          .sortBy(_._2)
          .collect
          .toList

        val auditDetailsList = result.map {
          value => {
            val producerStat = FailedEvents(value._1._1, value._1._2, value._2)
            val percentageDiff = if (totalEvent > 0) (producerStat.failedCount.toDouble * 100) / totalEvent.toDouble else 0d
            AuditDetails(rule = config.name, stats = producerStat.toMap, difference = percentageDiff,
              status = computeStatus(config.threshold, percentageDiff))
          }
        }
        auditDetailsList
      }
    }.getOrElse(List[AuditDetails]())

    JobLogger.log("Computing events count from failed backup job complete...")
    auditResults
  }

  def computeStatus(threshold: Double, diff: Double): AuditStatus.Value = {
    if(diff < threshold) AuditStatus.GREEN
    else if((diff - threshold).abs < 0.5) AuditStatus.AMBER
    else AuditStatus.RED
  }
}
