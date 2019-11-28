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

    val metrics = models.flatMap{f =>
      val queryType = f._1
      queryType match {
        case "druid" =>
          computeDruidMetrics(models.get("druid").get.asInstanceOf[Map[String, AnyRef]])
        case "secor" =>
          SecorMetricUtil.getSecorMetrics(models.get("secor").get.asInstanceOf[Map[String,AnyRef]])
      }
    }
    sc.parallelize(metrics.toList)
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
      computeDruidSourceCount(apiURL, model._2.asInstanceOf[Map[String, String]]("query"), startDate, endDate, model._1)
    })
    (metrics.toList)
  }

  def computeDruidSourceCount(apiURL: String, query: String, startDate: String, endDate: String, systemName: String): V3DerivedEvent = {
    val requestQuery = AppConf.getConfig(query).format(startDate, endDate)
    val response = RestUtil.post[List[Map[String, String]]](apiURL, requestQuery).headOption
    val responseMetrics = response.map(f => V3MetricEdata("telemetryCount", f.get("total").get.asInstanceOf[String])).toList
    val metricsEvent = CommonUtil.getMetricEvent(Map("system" -> "DruidCount", "subsystem" -> systemName, "metrics" -> responseMetrics), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
    metricsEvent
  }
}
