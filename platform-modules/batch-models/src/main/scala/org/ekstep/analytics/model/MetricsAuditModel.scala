package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, RestUtil}
import org.ekstep.analytics.framework.{Empty, IBatchModelTemplate, V3DerivedEvent, V3MetricEdata}
import org.ekstep.analytics.util.SecorMetricUtil
import org.joda.time.DateTime

object MetricsAuditModel extends IBatchModelTemplate[Empty, Empty, V3DerivedEvent, V3DerivedEvent] with Serializable {

  val className = "org.ekstep.analytics.model.MetricsAuditJob"

  override def name: String = "MetricsAuditJob"

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
    events;
  }

  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3DerivedEvent] = {

    val auditConfig = config.get("auditConfig")
    val models = auditConfig.get.asInstanceOf[Map[String, AnyRef]]
    val metrics = models.map{f =>
      val queryType = f._1
      queryType match {
        case "druid" =>
          computeDruidMetrics(models.get("druid").get.asInstanceOf[Map[String, AnyRef]])
        case "azure" =>
          SecorMetricUtil.getSecorMetrics(models.get("azure").get.asInstanceOf[Map[String,AnyRef]])
      }
    }
    println("final metrics", metrics)
    sc.parallelize(metrics.toList.flatten)
  }

  override def postProcess(events: RDD[V3DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3DerivedEvent] = {
    events;
  }

  def computeDruidMetrics(druidConfig: Map[String, AnyRef]) (implicit sc: SparkContext): List[V3DerivedEvent] = {
    val druidModel = druidConfig("models").asInstanceOf[Map[String, AnyRef]]

    val apiURL = AppConf.getConfig("druid.sql.host")
    val startDate = new DateTime().minusDays(1).toString("yyyy-MM-dd HH:mm:ss")
    val endDate = new DateTime().toString("yyyy-MM-dd HH:mm:ss")

    val metrics = druidModel.map(model => {
      model._1 match {
        case "telemetry-events" =>
          computeTelemetryCount(apiURL, model._2.asInstanceOf[Map[String, String]]("query"), startDate, endDate)
        case  "summary-events" =>
          computeSummaryCount(apiURL, model._2.asInstanceOf[Map[String, String]]("query"),startDate, endDate)
      }
    })
    (metrics.toList)
  }

    def computeTelemetryCount(apiURL: String, query: String, startDate: String, endDate: String): V3DerivedEvent = {
      val requestQuery = AppConf.getConfig(query).format(startDate, endDate)
      val response = RestUtil.post[List[Map[String, String]]](apiURL, requestQuery).headOption
      val responseMetrics = response.map(f => V3MetricEdata("telemetryCount", f.get("total").get.asInstanceOf[String])).toList
      val metricsEvent = CommonUtil.getMetricEvent(Map("system" -> "DruidCount", "subsystem" -> "druidTelemetryCount", "metrics" -> responseMetrics), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
      metricsEvent
    }

    def computeSummaryCount(apiURL: String, query: String, startDate: String, endDate: String): V3DerivedEvent = {
      val requestQuery = AppConf.getConfig(query).format(startDate, endDate)
      val response = RestUtil.post[List[Map[String, String]]](apiURL, requestQuery).headOption
      val responseMetrics = response.map(f => V3MetricEdata("summaryCount", f.get("total").get.asInstanceOf[String])).toList
      val metricsEvent = CommonUtil.getMetricEvent(Map("system" -> "DruidCount", "subsystem" -> "druidSummaryCount", "metrics" -> responseMetrics), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
      metricsEvent
    }
}
