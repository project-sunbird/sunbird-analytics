package org.ekstep.analytics.framework

import org.apache.spark.SparkContext

import scala.reflect.runtime.{universe => ru}

case class AuditConfig(name: String, description: String, thresholds: String, model: String, priority: Int)

object AuditTaskRunner {

  def execute(auditConfig: AuditConfig)(implicit sparkContext: SparkContext): List[AuditOutput] = {
    val auditRuleModel = auditConfig.model
    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = runtimeMirror.staticModule(auditRuleModel)
    runtimeMirror.reflectModule(classSymbol).instance.asInstanceOf[IAuditTask].computeAuditMetrics(auditConfig)(sparkContext)
  }

}
