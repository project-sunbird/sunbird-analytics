package org.ekstep.analytics.framework

import org.apache.spark.SparkContext


object AuditStatus extends Enumeration {
  type AuditStatus = Value
  val RED = Value("RED")
  val GREEN = Value("GREEN")
  val AMBER = Value("AMBER")
}

trait CaseClassConversions extends Product {
  def toMap: Map[String, Any] = getClass.getDeclaredFields.map(_.getName).zip(productIterator.toList).toMap
}

case class AuditOutput(name: String, status: AuditStatus.AuditStatus, details: List[AuditDetails]) extends AlgoOutput with Output

case class AuditDetails(rule: String, stats: Map[String, Any], difference: Double, status: AuditStatus.AuditStatus) {
  private val formattedStats = stats.map{ case (key,value) => s""" "$key": "$value" """ }.toList
  override def toString = s"""{"rule": "$rule", "stats": {${formattedStats.mkString(",")}}, "percentage_diff": %.2f, "status": "$status"}""".format(difference)
  def toSlackFields: Array[String] = {
    val emojiMap = Map("GREEN" -> ":white_check_mark:", "RED" -> ":x:", "AMBER" -> ":warning:")
    Array(s"""
       |{
       | "title": "Rule name",
       | "value": "$rule",
       | "short": true
       |}
    """.stripMargin,
    s"""
       |{
       | "title": "Stats",
       | "value": "${formattedStats.mkString.replace("\"", "") }",
       | "short": false
       |}
    """.stripMargin,
    s"""
       |{
       | "title": "Percentage diff",
       | "value": "%.2f%%",
       | "short": true
       |}
  """.stripMargin.format(difference),
    s"""
       |{
       | "title": "Status",
       | "value": "${emojiMap(status.toString)}",
       | "short": true
       |}
  """.stripMargin,
    s"""
       |{
       | "title": "",
       | "value": "---------------------------------",
       | "short": false
       |}
  """.stripMargin
    )
  }
}

trait IAuditTask {

  def name() : String = "AuditTask"
  def computeAuditMetrics(auditConfig: AuditConfig)(implicit sc: SparkContext): AuditOutput

}
