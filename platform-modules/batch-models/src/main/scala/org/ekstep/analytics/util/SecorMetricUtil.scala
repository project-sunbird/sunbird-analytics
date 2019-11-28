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
      var metricEvent: List[V3MetricEdata] = null
      val secorType = model._1
      val fetcher = JSONUtils.deserialize[Fetcher](JSONUtils.serialize(model._2.asInstanceOf[Map[String, String]]("search")))
      val rdd = DataFetcher.fetchBatchData[V3Event](fetcher)
      secorType match {
        case "denorm" =>
         metricEvent = getDenormSecorAudit(rdd)
        case "failed" =>
         metricEvent = getFailedSecorAudit(rdd)
        case _ =>
         metricEvent = getTotalSecorCountAudit(secorType, rdd)
      }
      CommonUtil.getMetricEvent(Map("system" -> "SecorAuditBackup", "subsystem" -> secorType,
            "metrics" -> metricEvent), AppConf.getConfig("metric.producer.id"), AppConf.getConfig("metric.producer.pid"))
    })
    (metrics.toList)
  }

  def getTotalSecorCountAudit(secorName: String, rdd: RDD[V3Event]): List[V3MetricEdata] = {

    val totalCount = rdd.count()
    val metricEvent = List(V3MetricEdata("date", Option(new Date())), V3MetricEdata("inputEvents", Option(totalCount)))
    metricEvent
  }


  def getDenormSecorAudit(rdd: RDD[V3Event]): List[V3MetricEdata] = {
    val count = rdd.count()
    val userDataRetrievedCount = rdd.filter(f => f.flags.get.user_data_retrieved.getOrElse(false)).count()
    val deviceDataRetrievedCount = rdd.filter(f => f.flags.get.device_data_retrieved.getOrElse(false)).count()
    val dialCodeRetrievedCount = rdd.filter(f => f.flags.get.dialcode_data_retrieved.getOrElse(false)).count()
    val derivedLocationRetrievedCount = rdd.filter(f => f.flags.get.derived_location_retrieved.getOrElse(false)).count()
    val contentDataRetrievedCount = rdd.filter(f => f.flags.get.content_data_retrieved.getOrElse(false)).count()
    val metricEvent = List(V3MetricEdata("date", Option(new Date())), V3MetricEdata("totalCount", Option(count)),
      V3MetricEdata("userDataRetreivedCount", Option(userDataRetrievedCount)), V3MetricEdata("deviceDataRetreivedCount", Option(deviceDataRetrievedCount))
      , V3MetricEdata("dialCodeDataRetreivedCount", Option(dialCodeRetrievedCount)), V3MetricEdata("derivedLocationRetreivedCount", Option(derivedLocationRetrievedCount))
      ,  V3MetricEdata("contentDataRetreivedCount", Option(contentDataRetrievedCount)))
    metricEvent
  }

  def getFailedSecorAudit(rdd: RDD[V3Event]): List[V3MetricEdata] = {
    val failedCountByPID = rdd.filter(f => null != f.context && null != f.context.pdata).groupBy(f => f.context.pdata.get.id)
    val metricEvent = failedCountByPID.map(f => V3MetricEdata(f._1, Some(f._2.size)))
    metricEvent.collect().toList
  }
}
