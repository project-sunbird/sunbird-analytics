package org.ekstep.analytics.updater

/**
  * @author Manjunath Davanam <manjunathd@ilimi.in>
  */

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.{Constants, WorkFlowUsageSummaryFact}
import org.ekstep.analytics.adapter.ContentAdapter
import org.joda.time.DateTime


case class workflowSummaryEvents(deviceId: String, mode: String, dType: String, totalSession: Long, totalTs: Double) extends AlgoInput with Input

case class DashBoardSummary(noOfUniqueDevices: Long, totalContentPlaySessions: Double, totalTimeSpent: Double, totalDigitalContentPublished:Long) extends AlgoOutput with Output


object UpdateDashboardModel extends IBatchModelTemplate[DerivedEvent, workflowSummaryEvents, DashBoardSummary, MeasuredEvent] with Serializable {

  val className = "org.ekstep.analytics.updater.UpdateDashboardModel"

  override def name: String = "UpdateDashboardModel"

  /**
    *
    * @param data       - RDD Event Data
    * @param config     - Configurations to run preprocess
    * @param sc         - SparkContext
    * @return           - workflowSummaryEvents
    */
  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[workflowSummaryEvents] = {
    val date = config.getOrElse("date", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String]
    val startTime = CommonUtil.dateFormat.parseDateTime(date).getMillis
    val endTime = CommonUtil.getEndTimestampOfDay(date)
    sc.cassandraTable[WorkFlowUsageSummaryFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT).where("m_updated_date>=?", startTime).where("m_updated_date<=?", endTime).filter { x => x.d_period == 0 }.map(event => {
      workflowSummaryEvents(event.d_device_id, event.d_mode, event.d_type, event.m_total_sessions, event.m_total_ts)
    })
  }

  /**
    *
    * @param data         - RDD Workflow summary event data
    * @param config       - Configurations to algorithm
    * @param sc           - Spark context
    * @return             - DashBoardSummary ->(uniqueDevices, totalContentPlaySession, totalTimeSpent,)
    */
  override def algorithm(data: RDD[workflowSummaryEvents], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DashBoardSummary] = {
    val uniqueDevices = data.filter(x => x.deviceId != "all").map(_.deviceId).distinct().count()
    val totalContentPlaySession = data.filter(x => x.mode.equals("play") && x.dType.equals("content")).map(_.totalSession).sum()
    val totalTimeSpent = data.filter(x => x.dType.equals("app") || x.dType.equals("session")).map(_.totalTs).sum()
    val publishedContents =  ContentAdapter.getPublishedContentList()
    println("publishedContentlist", publishedContents.count)
    val totalDigitalContentPublished = publishedContents.count
    println(uniqueDevices)
    println(totalContentPlaySession)
    println(totalTimeSpent)
    sc.parallelize(Array(DashBoardSummary(uniqueDevices, totalContentPlaySession, totalTimeSpent, totalDigitalContentPublished)))
  }

  /**
    *
    * @param data         - RDD DashboardSummary Event
    * @param config       - Configurations to run postprocess method
    * @param sc           - Spark context
    * @return             - ME_DASHBOARD_CUMULATIVE_SUMMARY MeasuredEvents
    */
  override def postProcess(data: RDD[DashBoardSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
    val version = AppConf.getConfig("telemetry.version")
    val record = data.first()
    val measures = Map(
      "totalTimeSpent" ->record.totalTimeSpent,
      "totalContentPlaySessions" -> record.totalContentPlaySessions,
      "noOfUniqueDevices" -> record.noOfUniqueDevices,
      "totalDigitalContentPublished"-> record.totalDigitalContentPublished,
      "telemetryVersion"-> version
    )
    val mid = CommonUtil.getMessageId("ME_DASHBOARD_CUMULATIVE_SUMMARY", null, "DAY", null
      , "", None, None, null)
    sc.parallelize(Array(MeasuredEvent("ME_DASHBOARD_CUMULATIVE_SUMMARY", System.currentTimeMillis(), 5345, version, mid, "", "", None, None,
      Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "WorkFlowUsageSummarizer").asInstanceOf[String])), None, "DAY", null),
      Dimensions(None, None, None, None, None, None, None), MEEdata(measures), None)))
  }

}