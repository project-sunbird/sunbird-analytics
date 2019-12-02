package org.ekstep.analytics.model

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}

case class DruidMetrics(total_count: Long, date: String)

object MetricsAuditModel extends IBatchModelTemplate[Empty, Empty, V3DerivedEvent, V3DerivedEvent] with Serializable {

  val className = "org.ekstep.analytics.model.MetricsAuditJob"
  override def name: String = "MetricsAuditJob"

  implicit val fc = new FrameworkContext()

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
    events;
  }

  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3DerivedEvent] = {

    val auditConfig = config("auditConfig").asInstanceOf[List[Map[String, AnyRef]]]
    val metrics = auditConfig.map{f =>
      var metricsEvent: V3DerivedEvent = null
      val queryConfig = JSONUtils.deserialize[JobConfig](JSONUtils.serialize(f))
      val queryType = queryConfig.search.`type`
      val name = queryConfig.name.get

      queryType match {
        case "azure" =>
          val data = DataFetcher.fetchBatchData[V3Event](queryConfig.search)
          metricsEvent = getSecorMetrics(queryConfig, data, name)
        case "druid" =>
          val data = DataFetcher.fetchBatchData[DruidMetrics](queryConfig.search).first()
          val metrics = List(V3MetricEdata(name, Some(data.total_count)),V3MetricEdata("date", Some(data.date)))
          metricsEvent = CommonUtil.getMetricEvent(Map("system" -> "DruidCount", "subsystem" -> name, "metrics" -> metrics), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
      }
      metricsEvent
    }
    sc.parallelize(metrics)
  }

  override def postProcess(events: RDD[V3DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3DerivedEvent] = {
    events;
  }


  def getSecorMetrics(queryConfig: JobConfig,rdd: RDD[V3Event], name: String)(implicit sc: SparkContext): V3DerivedEvent = {
    var metricEvent: List[V3MetricEdata] = null
    name match {
      case "denorm" =>
        metricEvent = getDenormSecorAudit(rdd, queryConfig.filters.get)
      case "failed" =>
        metricEvent = getFailedSecorAudit(rdd)
      case _ =>
        metricEvent = getTotalSecorCountAudit(name, rdd)
    }
    val v3metric = CommonUtil.getMetricEvent(Map("system" -> "SecorAuditBackup", "subsystem" -> name,
      "metrics" -> metricEvent), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
    v3metric
  }

  def getTotalSecorCountAudit(secorName: String, rdd: RDD[V3Event]): List[V3MetricEdata] = {

    val totalCount = rdd.count()
    val metricEvent = List(V3MetricEdata("date", Option(new Date())), V3MetricEdata("inputEvents", Option(totalCount)))
    metricEvent
  }

  def getFailedSecorAudit(rdd: RDD[V3Event]): List[V3MetricEdata] = {
    val failedCountByPID = rdd.filter(f => null != f.context && null != f.context.pdata).groupBy(f => f.context.pdata.get.id)
    val metrics = failedCountByPID.map(f => V3MetricEdata(f._1, Some(f._2.size)))
    metrics.collect().toList
  }

  def getDenormSecorAudit(rdd: RDD[V3Event], filters: Array[Filter])(implicit sc: SparkContext): List[V3MetricEdata] = {
    val count = rdd.count()
    val metricData = filters.map{f =>
      V3MetricEdata(f.name, Option(DataFilter.filter(rdd, f).count()))
    }.toList
    metricData
  }
}
