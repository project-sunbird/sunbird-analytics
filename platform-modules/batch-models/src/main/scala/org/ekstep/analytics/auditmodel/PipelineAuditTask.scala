package org.ekstep.analytics.auditmodel

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.{IAuditTask, PipelineMetric, AuditOutput}


object PipelineAuditTask extends IAuditTask[PipelineMetric, AuditOutput] {
  def computeStatus(data: RDD[PipelineMetric], thresholds: Map[String, Any])(implicit sc: SparkContext): RDD[AuditOutput] = {
    // TODO: Implementation to calculate the status
    sc.emptyRDD[AuditOutput]
  }
}