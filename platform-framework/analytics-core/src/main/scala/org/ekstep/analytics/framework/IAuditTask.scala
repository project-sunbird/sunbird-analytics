package org.ekstep.analytics.framework

import org.apache.spark.SparkContext

object AuditStatus extends Enumeration {
  type AuditStatus = Value
  val RED, GREEN, ORANGE = Value
}

case class AuditOutput(name: String, stats: Map[String, Any], status: AuditStatus.AuditStatus, details: Option[AnyRef]) extends AlgoOutput with Output

trait IAuditTask {

  def name() : String = "AuditTask"
  def computeAuditMetrics(auditConfig: AuditConfig)(implicit sc: SparkContext): List[AuditOutput]

}
