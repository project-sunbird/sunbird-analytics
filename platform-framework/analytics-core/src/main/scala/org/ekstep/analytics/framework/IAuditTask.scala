package org.ekstep.analytics.framework

import org.apache.spark.SparkContext

object AuditStatus extends Enumeration {
  type AuditStatus = Value
  val RED, GREEN, AMBER = Value
}

trait CaseClassConversions extends Product {
  def toMap: Map[String, Any] = getClass.getDeclaredFields.map(_.getName).zip(productIterator.toList).toMap
}

case class AuditOutput(name: String, status: AuditStatus.AuditStatus, details: List[AuditDetails]) extends AlgoOutput with Output
case class AuditDetails(rule: String, stats: Map[String, Any], difference: Double, status: AuditStatus.AuditStatus) {
  private val formattedStats: String = stats.map{ case (key,value) => s""""$key":"$value"""" }.mkString(",")
  override def toString = s"""{"rule": "$rule", "stats": {$formattedStats}, "percentage_diff": %.2f, "status": "$status"}""".format(difference)
}

trait IAuditTask {

  def name() : String = "AuditTask"
  def computeAuditMetrics(auditConfig: AuditConfig)(implicit sc: SparkContext): AuditOutput

}
