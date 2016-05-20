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

case class GenieSummary(timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int)

object GenieUsageSummary extends SessionBatchModel[Event] with Serializable {

    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val genieLaunchSessions = getGenieLaunchSessions(data);
        //val genieSessions = getGenieSessions(data);
        val summary = genieLaunchSessions.mapValues { x =>
            //            val geStart = x.filter { x => "GE_GENIE_START".equals(x.eid) }(0)
            //            val geEnd = x.filter { x => "GE_GENIE_END".equals(x.eid) }(0)

            println(x.size)
            val geStart = x(0)
            val geEnd = x.last
            //            println("=======================================================================================================================")
            //            println(geStart.eid)
            //            println(geEnd.eid)
            //            println("=======================================================================================================================")
            val timeSpent = CommonUtil.getTimeDiff(geStart, geEnd)
            val time_stamp = CommonUtil.getTimestamp(geEnd.ts)

            val content = x.filter { x => "OE_START".equals(x.eid) }.map { x => x.gdata.id }.distinct
            val contentCount = content.size
            GenieSummary(timeSpent.getOrElse(0d), time_stamp, content, contentCount);
        }
        summary.map { x => JSONUtils.serialize(x) };
    }

    private def getMeasuredEventGenieSummary(gSumm: (String, GenieSummary), config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {

        val summ = gSumm._2
        val mid = CommonUtil.getMessageId("ME_CONTENT_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange, gSumm._1);
        val measures = Map(
            "timeSpent" -> summ.timeSpent,
            "time_stamp" -> summ.time_stamp,
            "content" -> summ.content,
            "contentCount" -> summ.contentCount);
        MeasuredEvent("ME_GENIE_SUMMARY", System.currentTimeMillis(), dtRange.to, "1.0", mid, None, Option(contentSumm._1), None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentUsageSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange),
            Dimensions(None, None, None, None, None, None, None, Option(contentSumm._2), Option(contentSumm._3)),
            MEEdata(measures));
    }
}