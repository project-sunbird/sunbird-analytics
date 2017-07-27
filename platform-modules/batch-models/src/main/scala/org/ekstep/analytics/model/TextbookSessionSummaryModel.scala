package org.ekstep.analytics.model

import scala.collection.mutable.HashMap
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.SessionBatchModel
import org.ekstep.analytics.creation.model.CreationEvent
import org.apache.spark.HashPartitioner
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.CreationEventUtil
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.creation.model.CreationPData

/**
 * @author yuva
 */
case class UnitSummary(added_count: Long, deleted_count: Long, modified_count: Long)
case class LessonSummary(added_count: Long, deleted_count: Long, modified_count: Long)
case class TextbookSessionMetrics(uid: String, sid: String, pdata: CreationPData, channel: String, syncDate: Long, content_id: String, start_time: Long, end_time: Long, time_spent: Double, time_diff: Double, unit_summary: UnitSummary, lesson_summary: LessonSummary, date_range: DtRange) extends Output with AlgoOutput
case class TextbookSessions(sessionEvent: Buffer[CreationEvent]) extends AlgoInput
/**
 * @dataproduct
 * @Summarizer
 *
 * TextbookSessionSummaryModel
 *
 * Functionality
 * Compute session wise Textbook summary : Units and Lessons added/deleted/modified
 */
object TextbookSessionSummaryModel extends IBatchModelTemplate[CreationEvent, TextbookSessions, TextbookSessionMetrics, MeasuredEvent] with Serializable {
    implicit val className = "org.ekstep.analytics.model.TextbookSessionSummaryModel"
    override def name(): String = "TextbookSessionSummaryModel";
    override def preProcess(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookSessions] = {
        /*
         * Input raw telemetry
         * */
        val filteredEvents = data.filter { x => (x.edata.eks.env != null) }
        val sessions = getSessions(filteredEvents)
        sessions.map { x => TextbookSessions(x) }
    }

    override def algorithm(data: RDD[TextbookSessions], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookSessionMetrics] = {
        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];
        data.map { x =>
            val start_time = x.sessionEvent.head.ets
            val end_time = x.sessionEvent.last.ets
            val endEvent = x.sessionEvent.last
            val pdata = CreationEventUtil.getAppDetails(endEvent)
            val channel = CreationEventUtil.getChannelId(endEvent)
            val date_range = DtRange(start_time, end_time)
            var tmpLastEvent: CreationEvent = null;
            val eventsWithTs = x.sessionEvent.map { x =>
                if (tmpLastEvent == null) tmpLastEvent = x;
                val ts = CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get;
                tmpLastEvent = x;
                (x, if (ts > idleTime) 0 else ts)
            }
            val time_spent = CommonUtil.roundDouble(eventsWithTs.map(f => f._2).sum, 2);
            val time_diff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(start_time, end_time).get, 2);
            val uid = x.sessionEvent.head.uid
            val sid = x.sessionEvent.head.context.get.sid
            val content_id = x.sessionEvent.head.context.get.content_id
            val filtered_events = x.sessionEvent.filter { x => (x.edata.eks.`type`.equals("action") && (x.edata.eks.target.equals("") || x.edata.eks.target.equals("textbookunit")) && x.edata.eks.subtype.equals("save") && x.edata.eks.values.size > 0) }
            val buffered_map_values = filtered_events.map { x =>
                x.edata.eks.values.get
            }.flatMap { x => x }
            val total_units_added = buffered_map_values.map { x => x.getOrElse("unit_added", "0").toString().toLong }.sum
            val total_units_deleted = buffered_map_values.map { x => x.getOrElse("unit_deleted", "0").toString().toLong }.sum
            val total_units_modified = buffered_map_values.map { x => x.getOrElse("unit_modified", "0").toString().toLong }.sum
            val total_lessons_added = buffered_map_values.map { x => x.getOrElse("lesson_added", "0").toString().toLong }.sum
            val total_lessons_deleted = buffered_map_values.map { x => x.getOrElse("lesson_deleted", "0").toString().toLong }.sum
            val total_lessons_modified = buffered_map_values.map { x => x.getOrElse("lesson_modified", "0").toString().toLong }.sum
            TextbookSessionMetrics(uid, sid, pdata, channel, CreationEventUtil.getEventSyncTS(endEvent), content_id, start_time, end_time, time_spent, time_diff, UnitSummary(total_units_added, total_units_deleted, total_units_modified), LessonSummary(total_lessons_added, total_lessons_deleted, total_lessons_modified), date_range)
        }.filter { x => (x.time_spent > 0.0) }
    }

    override def postProcess(data: RDD[TextbookSessionMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_TEXTBOOK_SESSION_SUMMARY", summary.uid, config.getOrElse("granularity", "SESSION").asInstanceOf[String], summary.date_range, summary.content_id, Option(summary.pdata.id), Option(summary.channel));
            val measures = Map(
                "start_time" -> summary.start_time,
                "end_time" -> summary.end_time,
                "time_spent" -> summary.time_spent,
                "time_diff" -> summary.time_diff,
                "unit_summary" -> summary.unit_summary,
                "lesson_summary" -> summary.lesson_summary);
            val pdata = PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "TextbookSessionSummarizer").asInstanceOf[String]));
            MeasuredEvent("ME_TEXTBOOK_SESSION_SUMMARY", System.currentTimeMillis(), summary.syncDate, meEventVersion, mid, summary.uid, summary.channel, None, None,
                Context(pdata, None, "SESSION", summary.date_range),
                Dimensions(None, None, None, None, None, None, Option(PData(summary.pdata.id, summary.pdata.ver)), None, None, None, None, None, Option(summary.content_id), None, None, Option(summary.sid)), MEEdata(measures), None);
        };
    }

    /*
     * Sessionization based on Env
     * */
    private def getSessions(creationEvent: RDD[CreationEvent])(implicit sc: SparkContext): RDD[Buffer[CreationEvent]] = {
        var sessions = Buffer[Buffer[CreationEvent]]();
        var tmpArr = Buffer[CreationEvent]();
        var prevEnv = ""
        creationEvent.collect.sortBy { x => x.ets }.foreach { x =>
            if ((prevEnv.equals("textbook") && prevEnv.equals(x.edata.eks.env)) && (CommonUtil.getTimeDiff(tmpArr.last.ets, x.ets).get / 60 < 30)) {
                tmpArr += x
            } else {
                if (tmpArr.length > 0 && prevEnv.equals("textbook"))
                    sessions += tmpArr
                tmpArr = Buffer[CreationEvent]();
                tmpArr += x
            }
            prevEnv = x.edata.eks.env
        }
        if (sessions.isEmpty && prevEnv.equals("textbook"))
            sessions += tmpArr
        sc.parallelize(sessions)
    }
}