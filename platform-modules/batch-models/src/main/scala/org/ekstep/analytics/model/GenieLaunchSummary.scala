package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.SessionBatchModel
import org.ekstep.analytics.framework.util.CommonUtil
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.util.JobLogger

case class GenieSummary(did: String, timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int, syncts: Long,
                        tags: Option[AnyRef], dateRange: DtRange) extends AlgoOutput
case class LaunchSessions(did: String, events: Buffer[Event]) extends AlgoInput

object GenieLaunchSummary extends SessionBatchModel[Event, MeasuredEvent] with IBatchModelTemplate[Event, LaunchSessions, GenieSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.GenieLaunchSummary"

    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LaunchSessions] = {
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val jobConfig = sc.broadcast(config);
        val genieLaunchSessions = getGenieLaunchSessions(data, idleTime);
        genieLaunchSessions.map { x => LaunchSessions(x._1, x._2) }
    }

    override def algorithm(data: RDD[LaunchSessions], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieSummary] = {
        data.map { x =>
            val geStart = x.events.head
            val geEnd = x.events.last
            val syncts = CommonUtil.getEventSyncTS(geEnd)
            val startTimestamp = CommonUtil.getEventTS(geStart)
            val endTimestamp = CommonUtil.getEventTS(geEnd)
            val dtRange = DtRange(startTimestamp, endTimestamp);
            val timeSpent = CommonUtil.getTimeDiff(startTimestamp, endTimestamp)
            val content = x.events.filter { x => "OE_START".equals(x.eid) }.map { x => x.gdata.id }.filter { x => x != null }.distinct
            GenieSummary(x.did, timeSpent.getOrElse(0d), endTimestamp, content, content.size, syncts, Option(geEnd.tags), dtRange);
        }.filter { x => (x.timeSpent >= 0) }
    }

    override def postProcess(data: RDD[GenieSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_GENIE_LAUNCH_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dateRange, summary.did);
            val measures = Map(
                "timeSpent" -> summary.timeSpent,
                "time_stamp" -> summary.time_stamp,
                "content" -> summary.content,
                "contentCount" -> summary.contentCount);
            MeasuredEvent("ME_GENIE_LAUNCH_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dateRange),
                Dimensions(None, Option(summary.did), None, None, None, None, None, None, None),
                MEEdata(measures), summary.tags);
        }
    }
}