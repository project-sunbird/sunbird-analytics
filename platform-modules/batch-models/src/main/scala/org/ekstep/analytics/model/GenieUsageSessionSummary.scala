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
import org.ekstep.analytics.framework.LearnerId
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.updater.LearnerProfile
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.LearnerId
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter

case class GenieSessionSummary(groupUser: Boolean, anonymousUser: Boolean, timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int, syncts: Long, tags: Option[AnyRef], dateRange: DtRange, learner_id: String, did: String)
case class Summary(sid: String, did: String, learner_id: String, timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int, syncts: Long, tags: Option[AnyRef], dateRange: DtRange)

object GenieUsageSessionSummary extends SessionBatchModel[Event, String] with Serializable {

    val className = "org.ekstep.analytics.model.GenieUsageSessionSummary"
    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        JobLogger.debug("### Execute method started ###", className);
        val config = jobParams.getOrElse(Map[String, AnyRef]())
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val jobConfig = sc.broadcast(config);

        //val sessionEvents = data.filter { x => (!"".equals(x.sid) && x.sid!=null) }
        val sessionEvents = DataFilter.filter(data, Array(Filter("sid", "ISNOTEMPTY"), Filter("uid", "ISNOTEMPTY")));
        val genieSessions = getGenieSessions(sessionEvents, idleTime);

        val gsSummary = genieSessions.mapValues { x =>
            val gsStart = x(0)
            val gsEnd = x.last
            val syncts = CommonUtil.getEventSyncTS(gsEnd);
            val startTimestamp = CommonUtil.getEventTS(gsStart)
            val endTimestamp = CommonUtil.getEventTS(gsEnd)
            val dtRange = DtRange(startTimestamp, endTimestamp);
            val timeSpent = CommonUtil.getTimeDiff(startTimestamp, endTimestamp)
            val content = x.filter { x => "OE_START".equals(x.eid) }.map { x => x.gdata.id }.filter { x => x != null }.distinct
            Summary(gsStart.sid, gsStart.did, gsStart.uid, timeSpent.getOrElse(0d), endTimestamp, content, content.size, syncts, Option(gsStart.tags), dtRange)
        }.filter { x => (x._2.timeSpent >= 0) }.map { x =>
            val summ = x._2
            (summ.learner_id, Summary(summ.sid, summ.did, summ.learner_id, summ.timeSpent, summ.time_stamp, summ.content, summ.contentCount, summ.syncts, summ.tags, summ.dateRange))
        }

        val groupInfoSummary = gsSummary.map(f => LearnerId(f._1)).distinct().joinWithCassandraTable[LearnerProfile](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE).map { x => (x._1.learner_id, (x._2.group_user, x._2.anonymous_user)); }
        val genieSessionSummary = gsSummary.leftOuterJoin(groupInfoSummary).map { x =>
            val summary = x._2._1
            (summary.sid, GenieSessionSummary(x._2._2.getOrElse((false, false))._1, x._2._2.getOrElse((false, false))._2, summary.timeSpent, summary.time_stamp, summary.content, summary.contentCount, summary.syncts, summary.tags, summary.dateRange, summary.learner_id, summary.did))
        }

        JobLogger.debug("### Execute method ended ###", className);
        genieSessionSummary.map { x =>
            getMeasuredEventGenieSessionSummary(x, jobConfig.value)
        }.map { x => JSONUtils.serialize(x) };
    }

    private def getMeasuredEventGenieSessionSummary(gsSumm: (String, GenieSessionSummary), config: Map[String, AnyRef]): MeasuredEvent = {
        val summ = gsSumm._2
        val mid = CommonUtil.getMessageId("ME_GENIE_SESSION_SUMMARY", summ.learner_id, config.getOrElse("granularity", "DAY").asInstanceOf[String], summ.dateRange, gsSumm._1);
        val measures = Map(
            "timeSpent" -> summ.timeSpent,
            "time_stamp" -> summ.time_stamp,
            "content" -> summ.content,
            "contentCount" -> summ.contentCount);
        MeasuredEvent("ME_GENIE_SESSION_SUMMARY", System.currentTimeMillis(), summ.syncts, "1.0", mid, "", None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], summ.dateRange),
            Dimensions(None, Option(summ.did), None, None, None, None, None, Option(summ.groupUser), Option(summ.anonymousUser)),
            MEEdata(measures), summ.tags);
    }
}