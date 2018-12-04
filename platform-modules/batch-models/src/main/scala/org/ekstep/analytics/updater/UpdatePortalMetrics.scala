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
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.util.{Constants, WorkFlowUsageSummaryFact}

import scala.util.Try


case class WorkflowSummaryEvents(deviceId: String, mode: String, dType: String, totalSession: Long, totalTs: Double) extends AlgoInput with Input

case class Metrics(noOfUniqueDevices: Long, totalContentPlayTime: Double, totalTimeSpent: Double, totalContentPublished: Long) extends AlgoOutput with Output

case class PortalMetrics(eid: String, ets: Long, syncts: Long, metrics_summary: Option[Metrics]) extends AlgoOutput with Output

object UpdatePortalMetrics extends IBatchModelTemplate[DerivedEvent, WorkflowSummaryEvents, Metrics, PortalMetrics] with Serializable {

  val className = "org.ekstep.analytics.updater.UpdatePortalMetrics"

  private val EVENT_ID: String = "ME_PORTAL_CUMULATIVE_METRICS"

  override def name: String = "UpdatePortalMetrics"

  /**
    * preProcess which will fetch the `WorkFlowUsageSummaryFact` Event data from the Cassandra Database.
    *
    * @param data   - RDD Event Data(Empty RDD event)
    * @param config - Configurations to run preProcess
    * @param sc     - SparkContext
    * @return - workflowSummaryEvents
    */
  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkflowSummaryEvents] = {
    sc.cassandraTable[WorkFlowUsageSummaryFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT).filter { x => x.d_period == 0 }.map(event => {
      WorkflowSummaryEvents(event.d_device_id, event.d_mode, event.d_type, event.m_total_sessions, event.m_total_ts)
    })
  }

  /**
    *
    * @param data   - RDD Workflow summary event data
    * @param config - Configurations to algorithm
    * @param sc     - Spark context
    * @return - DashBoardSummary ->(uniqueDevices, totalContentPlayTime, totalTimeSpent,)
    */
  override def algorithm(data: RDD[WorkflowSummaryEvents], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Metrics] = {
    object _constant extends Enumeration {
      val APP = "app"
      val PLAY = "play"
      val CONTENT = "content"
      val SESSION = "session"
      val ALL = "all"
    }
    val uniqueDevicesCount = data.filter(x => x.deviceId != _constant.ALL).map(_.deviceId).distinct().count()
    val totalContentPlayTime = data.filter(x => x.mode.equals(_constant.PLAY) && x.dType.equals(_constant.CONTENT)).map(_.totalSession).sum()
    val totalTimeSpent = data.filter(x => x.dType.equals(_constant.APP) || x.dType.equals(_constant.SESSION)).map(_.totalTs).sum()
    val totalContentPublished: Int = Try(ContentAdapter.getPublishedContentList().count).getOrElse(0)
    sc.parallelize(Array(Metrics(uniqueDevicesCount, CommonUtil.roundDouble(totalContentPlayTime/3600, 2), CommonUtil.roundDouble(totalTimeSpent/3600, 2), totalContentPublished)))
  }

  /**
    *
    * @param data   - RDD DashboardSummary Event
    * @param config - Configurations to run postprocess method
    * @param sc     - Spark context
    * @return - ME_PORTAL_CUMULATIVE_METRICS MeasuredEvents
    */
  override def postProcess(data: RDD[Metrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PortalMetrics] = {
    val record = data.first()
    val measures = Metrics(record.noOfUniqueDevices, record.totalContentPlayTime, record.totalTimeSpent, record.totalContentPublished)
    val metrics = PortalMetrics(EVENT_ID, System.currentTimeMillis(), System.currentTimeMillis(), Some(measures))
    if (config.getOrElse("dispatch", false).asInstanceOf[Boolean]) {
      AzureDispatcher.dispatch(Array(JSONUtils.serialize(metrics)), config)
    }
   sc.parallelize(Array(metrics))
  }
}