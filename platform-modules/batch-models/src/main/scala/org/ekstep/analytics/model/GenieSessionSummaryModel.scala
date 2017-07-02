package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.SessionBatchModel
import org.ekstep.analytics.framework.util.CommonUtil
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.updater.LearnerProfile
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.conf.AppConf

case class GenieSessionSummary(sid: String, groupUser: Boolean, anonymousUser: Boolean, timeSpent: Double, time_stamp: Long,
                               content: Buffer[String], contentCount: Int, syncts: Long, tags: Option[AnyRef], dateRange: DtRange,
                               learnerId: String, did: String, appId: String, channelId: String) extends AlgoOutput
case class Summary(sid: String, did: String, learnerId: String, timeSpent: Double, time_stamp: Long, content: Buffer[String],
                   contentCount: Int, syncts: Long, tags: Option[AnyRef], dateRange: DtRange, appId: String, channelId: String)
case class GenieSessions(channelId: String, sid: String, events: Buffer[Event]) extends AlgoInput

object GenieSessionSummaryModel extends SessionBatchModel[Event, MeasuredEvent] with IBatchModelTemplate[Event, GenieSessions, GenieSessionSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.GenieSessionSummaryModel"
    override def name: String = "GenieSessionSummaryModel"
    
    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieSessions] = {
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val jobConfig = sc.broadcast(config);
        val sessionEvents = DataFilter.filter(data, Array(Filter("sid", "ISNOTEMPTY"), Filter("uid", "ISNOTEMPTY")));
        val genieSessions = getGenieSessions(sessionEvents, idleTime);
        genieSessions.map { x => GenieSessions(x._1._1, x._1._2, x._2) }
    }

    override def algorithm(data: RDD[GenieSessions], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieSessionSummary] = {
        val gsSummary = data.map { x =>
            val gsStart = x.events.head
            val gsEnd = x.events.last
            
            val appId = gsStart.appid.getOrElse(AppConf.getConfig("default.app.id"))
            val channelId = gsEnd.channelid.getOrElse(AppConf.getConfig("default.channel.id"))
            
            val syncts = CommonUtil.getEventSyncTS(gsEnd);
            val startTimestamp = CommonUtil.getEventTS(gsStart)
            val endTimestamp = CommonUtil.getEventTS(gsEnd)
            val dtRange = DtRange(startTimestamp, endTimestamp);
            val timeSpent = CommonUtil.getTimeDiff(startTimestamp, endTimestamp)
            val content = x.events.filter { x => "OE_START".equals(x.eid) }.map { x => x.gdata.id }.filter { x => x != null }.distinct
            
            
            Summary(gsStart.sid, gsStart.did, gsStart.uid, timeSpent.getOrElse(0d), endTimestamp, content, content.size, syncts, Option(gsStart.tags), dtRange, appId, channelId)
        }.filter { x => (x.timeSpent >= 0) }.map { x =>
            (LearnerProfileIndex(x.learnerId, x.appId, x.channelId), Summary(x.sid, x.did, x.learnerId, x.timeSpent, x.time_stamp, x.content, x.contentCount, x.syncts, x.tags, x.dateRange, x.appId, x.channelId))
        }

        val groupInfoSummary = gsSummary.map(f => LearnerProfileIndex(f._1.learner_id, f._1.app_id, f._1.channel_id)).distinct().joinWithCassandraTable[LearnerProfile](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE).map { x => (LearnerProfileIndex(x._1.learner_id, x._1.app_id, x._1.channel_id), (x._2.group_user, x._2.anonymous_user)); }
        gsSummary.leftOuterJoin(groupInfoSummary).map { x =>
            val summary = x._2._1
            GenieSessionSummary(summary.sid, x._2._2.getOrElse((false, false))._1, x._2._2.getOrElse((false, false))._2, summary.timeSpent, summary.time_stamp, summary.content, summary.contentCount, summary.syncts, summary.tags, summary.dateRange, summary.learnerId, summary.did, summary.appId, summary.channelId)
        }
    }

    override def postProcess(data: RDD[GenieSessionSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_GENIE_SESSION_SUMMARY", summary.learnerId, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dateRange, summary.sid, Option(summary.appId), Option(summary.channelId));
            val measures = Map(
                "timeSpent" -> summary.timeSpent,
                "time_stamp" -> summary.time_stamp,
                "content" -> summary.content,
                "contentCount" -> summary.contentCount);
            MeasuredEvent("ME_GENIE_SESSION_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dateRange),
                Dimensions(None, Option(summary.did), None, None, None, None, None, Option(summary.groupUser), Option(summary.anonymousUser), None, None, None, None, None, None, None, None, None, None, None, None, None, None,Option(summary.appId), None, None, Option(summary.channelId)),
                MEEdata(measures), summary.tags);
        }
    }
}