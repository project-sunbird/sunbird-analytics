package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Period._
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.joda.time.LocalDate
import org.ekstep.analytics.framework.util.JSONUtils

case class ContentMetrics(id: String, top_k_timespent: Map[String, Double], top_k_sessions: Map[String, Long])
case class ContentDetail(content_id: String, ts: Long, group_user: Boolean, content_type: String, mime_type: String, publish_date: DateTime, total_ts: Double, total_sessions: Int, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, end_date: Long)
case class ContentUsageSummaryFact(d_content_id: String, d_period: Int, d_group_user: Boolean, d_content_type: String, d_mime_type: String, m_publish_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_avg_sessions_week: Double, m_avg_ts_week: Double)
case class ContentUsageSummaryIndex(d_content_id: String, d_period: Int, d_group_user: Boolean)

object ContentUsageUpdater extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val contentSummary = events.map { x =>
            val ts = x.syncts
            val startDate = x.context.date_range.from
            val endDate = x.context.date_range.to

            val content_id = x.content_id.get
            val group_user = x.dimensions.group_user.get

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val content_type = eksMap.get("content_type").get.asInstanceOf[String]
            val mime_type = eksMap.get("mime_type").get.asInstanceOf[String]
            val publish_date = new DateTime(startDate)
            val total_ts = eksMap.get("total_ts").get.asInstanceOf[Double]
            val total_sessions = eksMap.get("total_sessions").get.asInstanceOf[Int]
            val avg_ts_session = eksMap.get("avg_ts_session").get.asInstanceOf[Double]
            val total_interactions = eksMap.get("total_interactions").get.asInstanceOf[Int]
            val avg_interactions_min = eksMap.get("avg_interactions_min").get.asInstanceOf[Double]
            ContentDetail(content_id, ts, group_user, content_type, mime_type, publish_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, endDate);
        }.cache();
        
        rollup(contentSummary, DAY)
        rollup(contentSummary, WEEK)
        rollup(contentSummary, MONTH)
        rollup(contentSummary, CUMULATIVE)

        // Top K content 
        val summaries = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).filter { x => !"Collection".equals(x.d_content_type) }.filter { x => x.d_period == 0 };
        val count = summaries.count().intValue();
        val defaultVal = if (5 > count) count else 5;
        val topContentByTime = summaries.sortBy(f => f.m_total_ts, false, 1).take(configMapping.value.getOrElse("topK", defaultVal).asInstanceOf[Int]);
        val topContentBySessions = summaries.sortBy(f => f.m_total_sessions, false, 1).take(configMapping.value.getOrElse("topK", defaultVal).asInstanceOf[Int]);
        val rdd = sc.parallelize(Array(ContentMetrics("top_content", topContentByTime.map { x => (x.d_content_id, x.m_total_ts) }.toMap, topContentBySessions.map { x => (x.d_content_id, x.m_total_sessions) }.toMap)), 1);
        rdd.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_METRICS_TABLE);
        //--------

        contentSummary.map { x => JSONUtils.serialize(x) };
    }
    
    private def rollup(data: RDD[ContentDetail], period: Period) {
        period match {
            case DAY =>
                data.map { x =>
                    ContentUsageSummaryFact(x.content_id, CommonUtil.getPeriod(x.ts, period), x.group_user, x.content_type, x.mime_type, x.publish_date, x.total_ts, x.total_sessions, x.avg_ts_session, x.total_interactions, x.avg_interactions_min, 0d, 0d)
                }.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)

            case WEEK | MONTH | CUMULATIVE =>
                update(data, period)
            case _ =>
        }
    }
    
    private def update(data: RDD[ContentDetail], period: Period) {

        val currentData = data.map { x =>
            val periodCode = CommonUtil.getPeriod(x.ts, period)
            (ContentUsageSummaryIndex(x.content_id, CommonUtil.getPeriod(x.ts, period), x.group_user), (ContentUsageSummaryFact(x.content_id, periodCode, x.group_user, x.content_type, x.mime_type, x.publish_date, x.total_ts, x.total_sessions, x.avg_ts_session, x.total_interactions, x.avg_interactions_min, 0d, 0d), x.end_date));
        }
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).on(SomeColumns("d_content_id","d_period","d_group_user"));
        val joinedData = currentData.leftOuterJoin(prvData)
        //joinedData.collect().foreach(println(_));
        val updatedSummary = joinedData.map { x =>
            val index = x._1
            val prvSumm = x._2._2.getOrElse(null)
            val newSumm = x._2._1._1
            if (null != prvSumm) {
                val total_ts = newSumm.m_total_ts + prvSumm.m_total_ts
                val total_sessions = newSumm.m_total_sessions + prvSumm.m_total_sessions
                val avg_ts_session = (total_ts) / (total_sessions)
                val total_interactions = newSumm.m_total_interactions + prvSumm.m_total_interactions
                val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
                val publis_date = if (newSumm.m_publish_date.isBefore(prvSumm.m_publish_date)) newSumm.m_publish_date else prvSumm.m_publish_date;
                var avg_sessions_week = 0d
                var avg_ts_week = 0d
                if (period == MONTH) {
                    avg_sessions_week = (total_sessions) / 5
                    avg_ts_week = (total_ts) / 5
                } else if (period == CUMULATIVE) {
                    val end_date = x._2._1._2
                    val numWeeks = CommonUtil.getWeeksBetween(publis_date.getMillis, end_date)

                    avg_sessions_week = if (numWeeks != 0) (total_sessions) / numWeeks else total_sessions
                    avg_ts_week = if (numWeeks != 0) (total_ts) / numWeeks else total_ts
                }
                ContentUsageSummaryFact(prvSumm.d_content_id, newSumm.d_period, prvSumm.d_group_user, prvSumm.d_content_type, prvSumm.d_mime_type, publis_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, avg_sessions_week, avg_ts_week);
            } else {
                newSumm;
            }
        }
        updatedSummary.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)
    }
}