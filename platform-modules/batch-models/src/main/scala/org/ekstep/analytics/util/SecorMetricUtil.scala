package org.ekstep.analytics.util

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.framework.{V3MetricEdata, _}

object SecorMetricUtil {
  def getSecorMetrics(secorConfig: Map[String, AnyRef])(implicit sc: SparkContext): List[V3DerivedEvent] = {
    val secorModels = secorConfig("models").asInstanceOf[Map[String, AnyRef]]
    val metrics = secorModels.map(model => {
      val secorType = model._1
      val fetcher = JSONUtils.deserialize[Fetcher](JSONUtils.serialize(model._2.asInstanceOf[Map[String, String]]("search")))
      val rdd = DataFetcher.fetchBatchData[V3Event](fetcher)
      secorType match {
        case "denorm" =>
          getDenormSecorAudit(rdd)
        case "failed" =>
          getFailedSecorAudit(rdd)
        case _ =>
          getTotalSecorCountAudit(secorType, rdd)
      }
    })
    (metrics.toList)
  }

  def getTotalSecorCountAudit(secorName: String, rdd: RDD[V3Event]): V3DerivedEvent = {

    val totalCount = rdd.count()
    val metrics = List(V3MetricEdata("date", Option(new Date())), V3MetricEdata("inputEvents", Option(totalCount)))
    val metricEvent = CommonUtil.getMetricEvent(Map("system" -> "SecorAuditBackup", "subsystem" -> (secorName + "  TotalCount"),
      "metrics" -> metrics), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
    metricEvent
  }


  def getDenormSecorAudit(rdd: RDD[V3Event]): V3DerivedEvent = {
    val count = rdd.count()
    val userDataRetrievedCount = rdd.filter(f => f.flags.get.user_data_retrieved.getOrElse(false)).count()
    val deviceDataRetrievedCount = rdd.filter(f => f.flags.get.device_data_retrieved.getOrElse(false)).count()
    val dialCodeRetrievedCount = rdd.filter(f => f.flags.get.dialcode_data_retrieved.getOrElse(false)).count()
    val derivedLocationRetrievedCount = rdd.filter(f => f.flags.get.derived_location_retrieved.getOrElse(false)).count()
    val contentDataRetrievedCount = rdd.filter(f => f.flags.get.content_data_retrieved.getOrElse(false)).count()
    val metrics = List(V3MetricEdata("date", Option(new Date())), V3MetricEdata("totalCount", Option(count)),
      V3MetricEdata("userDataRetreivedCount", Option(userDataRetrievedCount)), V3MetricEdata("deviceDataRetreivedCount", Option(deviceDataRetrievedCount))
      , V3MetricEdata("dialCodeDataRetreivedCount", Option(dialCodeRetrievedCount)), V3MetricEdata("derivedLocationRetreivedCount", Option(derivedLocationRetrievedCount))
      ,  V3MetricEdata("contentDataRetreivedCount", Option(contentDataRetrievedCount)))
    val metricEvent = CommonUtil.getMetricEvent(Map("system" -> "SecorAuditBackup", "subsystem" -> "DenormAudit",
      "metrics" -> metrics), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
    metricEvent
  }

  def getFailedSecorAudit(rdd: RDD[V3Event]): V3DerivedEvent = {
    val failedCountByPID = rdd.filter(f => null != f.context && null != f.context.pdata).groupBy(f => f.context.pdata.get.id)
    val metrics = failedCountByPID.map(f => V3MetricEdata(f._1, Some(f._2.size)))
    val metricEvent = CommonUtil.getMetricEvent(Map("system" -> "SecorAuditBackup", "subsystem" -> "FailedEventsByProducer",
      "metrics" -> metrics.collect().toList), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
    metricEvent
  }
}
