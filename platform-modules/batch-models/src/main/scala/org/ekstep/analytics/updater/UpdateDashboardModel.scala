package org.ekstep.analytics.updater

/**
  * Ref:Design wiki link: https://project-sunbird.atlassian.net/wiki/spaces/SBDES/pages/794198025/Design+Brainstorm+Data+structure+for+capturing+dashboard+portal+metrics
  * Ref:Implementation wiki link: https://project-sunbird.atlassian.net/wiki/spaces/SBDES/pages/794099772/Data+Product+Dashboard+summariser+-+Cumulative
  *
  * @author Manjunath Davanam <manjunathd@ilimi.in>
  */

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.{Constants, WorkFlowUsageSummaryFact}
import org.joda.time.DateTime


case class WorkflowSummaryEvents(deviceId: String, mode: String, dType: String, totalSession: Long, totalTs: Double, syncTs: Option[Long]) extends AlgoInput with Input

case class Metrics(noOfUniqueDevices: Long, totalContentPlaySessions: Double, totalTimeSpent: Double, totalDigitalContentPublished: Long, version: Option[String],  syncTs: Option[Long]) extends AlgoOutput with Output

case class DashBoardSummary(eid: String, ets: Long, syncts: Option[Long], metrics_summary: Option[Metrics]) extends AlgoOutput with Output

object UpdateDashboardModel extends IBatchModelTemplate[DerivedEvent, WorkflowSummaryEvents, Metrics, DashBoardSummary] with Serializable {

  val className = "org.ekstep.analytics.updater.UpdateDashboardModel"

  private val EVENT_ID: String = "ME_DASHBOARD_CUMULATIVE_SUMMARY"

  override def name: String = "UpdateDashboardModel"

  /**
    * preProcess which will fetch the `WorkFlowUsageSummaryFact` Event data from the Cassandra Database.
    *
    * @param data   - RDD Event Data(Empty RDD event)
    * @param config - Configurations to run preProcess
    * @param sc     - SparkContext
    * @return - workflowSummaryEvents
    */
  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkflowSummaryEvents] = {
    val date = config.getOrElse("date", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String]
    val startTime = CommonUtil.dateFormat.parseDateTime(date).getMillis
    val endTime = CommonUtil.getEndTimestampOfDay(date)
    sc.cassandraTable[WorkFlowUsageSummaryFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT).where("m_updated_date>=?", startTime).where("m_updated_date<=?", endTime).filter { x => x.d_period == 0 }.map(event => {
      WorkflowSummaryEvents(event.d_device_id, event.d_mode, event.d_type, event.m_total_sessions, event.m_total_ts, Some(event.m_last_sync_date.getMillis))
    })
  }

  /**
    *
    * @param data   - RDD Workflow summary event data
    * @param config - Configurations to algorithm
    * @param sc     - Spark context
    * @return - DashBoardSummary ->(uniqueDevices, totalContentPlaySession, totalTimeSpent,)
    */
  override def algorithm(data: RDD[WorkflowSummaryEvents], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Metrics] = {
    object _constant extends Enumeration {
      val APP = "app"
      val PLAY = "play"
      val CONTENT = "content"
      val SESSION = "session"
      val ALL = "all"
    }
    val uniqueDevices = data.filter(x => x.deviceId != _constant.ALL).map(_.deviceId).distinct().count()
    val totalContentPlaySession = data.filter(x => x.mode.equals(_constant.PLAY) && x.dType.equals(_constant.CONTENT)).map(_.totalSession).sum()
    val totalTimeSpent = data.filter(x => x.dType.equals(_constant.APP) || x.dType.equals(_constant.SESSION)).map(_.totalTs).sum()
    val totalDigitalContentPublished = ContentAdapter.getPublishedContentList().count
    var lastSyncTs: Option[Long] = Some(new DateTime().getMillis())
    if (!data.isEmpty()) {
      lastSyncTs = data.sortBy(_.syncTs, false).first().syncTs
    }
    sc.parallelize(Array(Metrics(uniqueDevices, totalContentPlaySession, totalTimeSpent, totalDigitalContentPublished, null, lastSyncTs)))
  }

  /**
    *
    * @param data   - RDD DashboardSummary Event
    * @param config - Configurations to run postprocess method
    * @param sc     - Spark context
    * @return - ME_DASHBOARD_CUMULATIVE_SUMMARY MeasuredEvents
    */
  override def postProcess(data: RDD[Metrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DashBoardSummary] = {
    val version = AppConf.getConfig("telemetry.version")
    val record = data.first()
    val measures = Metrics(record.noOfUniqueDevices, record.totalContentPlaySessions, record.totalTimeSpent, record.totalDigitalContentPublished, Some(version), None)
    sc.parallelize(Array(DashBoardSummary(EVENT_ID, System.currentTimeMillis(), record.syncTs , Some(measures))))
  }

}