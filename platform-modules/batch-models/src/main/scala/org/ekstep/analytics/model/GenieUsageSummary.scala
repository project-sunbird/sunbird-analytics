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
import org.ekstep.analytics.framework.LearnerId
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.updater.LearnerProfile
import com.datastax.spark.connector._

case class GenieSummary(timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int)
case class GenieSessionSummary(groupUser: Boolean, anonymousUser: Boolean, timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int)

object GenieUsageSummary extends SessionBatchModel[Event] with Serializable {

    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]())
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val jobConfig = sc.broadcast(config);

        val genieLaunchSessions = getGenieLaunchSessions(data, idleTime);
        val sessionEvents = data.filter { x => (!"".equals(x.sid)) }
        val genieSessions = getGenieSessions(sessionEvents, idleTime);

        val genieSummary = genieLaunchSessions.mapValues { x =>
            val geStart = x(0)
            val geEnd = x.last

            val startTimestamp = CommonUtil.getEventTS(geStart)
            val endTimestamp = CommonUtil.getEventTS(geEnd)
            val dtRange = DtRange(startTimestamp, endTimestamp);
            val time_stamp = endTimestamp
            val timeSpent = CommonUtil.getTimeDiff(startTimestamp, endTimestamp)
            val content = x.filter { x => "OE_START".equals(x.eid) }.map { x => x.gdata.id }.distinct
            val contentCount = content.size
            (GenieSummary(timeSpent.getOrElse(0d), time_stamp, content, contentCount), dtRange);
        }.filter { x => (x._2._1.timeSpent >= 0) }

        val gsSummary = genieSessions.mapValues { x =>
            val gsStart = x(0)
            val gsEnd = x.last
            val learner_id = gsStart.uid
            val startTimestamp = CommonUtil.getEventTS(gsStart)
            val endTimestamp = CommonUtil.getEventTS(gsEnd)
            val dtRange = DtRange(startTimestamp, endTimestamp);
            val timeSpent = CommonUtil.getTimeDiff(startTimestamp, endTimestamp)
            val time_stamp = endTimestamp
            val content = x.filter { x => "OE_START".equals(x.eid) }.map { x => x.gdata.id }.distinct
            val contentCount = content.size
            (learner_id, timeSpent.getOrElse(0d), time_stamp, content, contentCount, dtRange)
        }.map { x => (x._2._1, (x._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6)) }.filter { x => (x._2._2 >= 0) }

        val groupInfoSummary = gsSummary.map(f => LearnerId(f._1)).distinct().joinWithCassandraTable[LearnerProfile](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE).map { x => (x._1.learner_id, (x._2.group_user, x._2.anonymous_user)); }
        val genieSessionSummary = gsSummary.leftOuterJoin(groupInfoSummary).map { x =>
            ((x._2._1._1, GenieSessionSummary(x._2._2.getOrElse((false, false))._1, x._2._2.getOrElse((false, false))._2, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5)), x._2._1._6)
        }

        val gs = genieSummary.map { x =>
            getMeasuredEventGenieSummary((x._1, x._2._1), jobConfig.value, x._2._2)
        }.map { x => JSONUtils.serialize(x) };

        val gss = genieSessionSummary.map { x =>
            getMeasuredEventGenieSessionSummary(x._1, jobConfig.value, x._2)
        }.map { x => JSONUtils.serialize(x) };

        gs.union(gss);
    }

    private def getMeasuredEventGenieSummary(gSumm: (String, GenieSummary), config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {
        val summ = gSumm._2
        val mid = CommonUtil.getMessageId("ME_GENIE_LAUNCH_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange, gSumm._1);
        val measures = Map(
            "timeSpent" -> summ.timeSpent,
            "time_stamp" -> summ.time_stamp,
            "content" -> summ.content,
            "contentCount" -> summ.contentCount);
        MeasuredEvent("ME_GENIE_LAUNCH_SUMMARY", System.currentTimeMillis(), dtRange.to, "1.0", mid, None, None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange),
            Dimensions(None, Option(gSumm._1), None, None, None, None, None, None, None),
            MEEdata(measures));
    }

    private def getMeasuredEventGenieSessionSummary(gsSumm: (String, GenieSessionSummary), config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {
        val summ = gsSumm._2
        val mid = CommonUtil.getMessageId("ME_GENIE_SESSION_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange, gsSumm._1);
        val measures = Map(
            "timeSpent" -> summ.timeSpent,
            "time_stamp" -> summ.time_stamp,
            "content" -> summ.content,
            "contentCount" -> summ.contentCount);
        MeasuredEvent("ME_GENIE_SESSION_SUMMARY", System.currentTimeMillis(), dtRange.to, "1.0", mid, None, None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange),
            Dimensions(None, Option(gsSumm._1), None, None, None, None, None, Option(summ.groupUser), Option(summ.anonymousUser)),
            MEEdata(measures));
    }
}