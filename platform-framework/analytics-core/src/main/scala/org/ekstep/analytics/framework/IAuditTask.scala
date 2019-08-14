package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils


object AuditStatus extends Enumeration {
  type AuditStatus = Value
  val RED, GREEN, ORANGE = Value
}

case class AuditOutput(name: String, stats: Map[String, String], status: AuditStatus.AuditStatus, details: Option[AnyRef]) extends Output

trait IAuditTask[T, R <: AuditOutput] extends IBatchModel[T, R] {
  def computeStatus(data: RDD[T], thresholds: Map[String, Any])(implicit sc: SparkContext): RDD[R]
  override def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[R] = {
    val params = jobParams.getOrElse(Map[String, AnyRef]())
    val thresholds = JSONUtils.deserialize[Map[String, Any]](params.getOrElse("thresholds", "{}").asInstanceOf[String])
    computeStatus(events, thresholds)(sc)
  }
  override def name() : String = "AuditTask"
}