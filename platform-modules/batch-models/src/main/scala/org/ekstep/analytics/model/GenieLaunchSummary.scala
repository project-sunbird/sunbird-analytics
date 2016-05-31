package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.Event
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

case class GenieSummary(timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int, syncts: Long, tags: Option[List[Map[String, AnyRef]]], dateRange: DtRange)

object GenieLaunchSummary extends SessionBatchModel[Event] with Serializable {

    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]())
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val jobConfig = sc.broadcast(config);

        val genieLaunchSessions = getGenieLaunchSessions(data, idleTime);

        val genieSummary = genieLaunchSessions.mapValues { x =>
            val geStart = x(0)
            val geEnd = x.last
            val syncts = CommonUtil.getEventSyncTS(geEnd)
            val startTimestamp = CommonUtil.getEventTS(geStart)
            val endTimestamp = CommonUtil.getEventTS(geEnd)
            val dtRange = DtRange(startTimestamp, endTimestamp);
            val timeSpent = CommonUtil.getTimeDiff(startTimestamp, endTimestamp)
            val content = x.filter { x => "OE_START".equals(x.eid) }.map { x => x.gdata.id }.distinct
            GenieSummary(timeSpent.getOrElse(0d), endTimestamp, content, content.size, syncts, Option(geStart.tags), dtRange);
        }.filter { x => (x._2.timeSpent >= 0) }

        genieSummary.map { x =>
            getMeasuredEventGenieSummary(x, jobConfig.value)
        }.map { x => JSONUtils.serialize(x) };
    }

    private def getMeasuredEventGenieSummary(gSumm: (String, GenieSummary), config: Map[String, AnyRef]): MeasuredEvent = {
        val summ = gSumm._2
        val mid = CommonUtil.getMessageId("ME_GENIE_LAUNCH_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], summ.dateRange, gSumm._1);
        val measures = Map(
            "timeSpent" -> summ.timeSpent,
            "time_stamp" -> summ.time_stamp,
            "content" -> summ.content,
            "contentCount" -> summ.contentCount);
        MeasuredEvent("ME_GENIE_LAUNCH_SUMMARY", System.currentTimeMillis(), summ.syncts, "1.0", mid, "", None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], summ.dateRange),
            Dimensions(None, Option(gSumm._1), None, None, None, None, None, None, None),
            MEEdata(measures), summ.tags);
    }
}