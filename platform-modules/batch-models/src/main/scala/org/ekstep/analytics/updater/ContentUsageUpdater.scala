package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
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
import org.ekstep.analytics.framework.util.JobLogger

case class ContentMetrics(id: String, top_k_timespent: Map[String, Double], top_k_sessions: Map[String, Long])
case class ContentUsageSummaryFact(d_content_id: String, d_period: Int, d_group_user: Boolean, d_content_type: String, d_mime_type: String, m_publish_date: DateTime,
                                   m_last_sync_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long,
                                   m_avg_interactions_min: Double, m_avg_sessions_week: Option[Double], m_avg_ts_week: Option[Double])
case class ContentUsageSummaryIndex(d_content_id: String, d_period: Int, d_group_user: Boolean)

object ContentUsageUpdater extends IBatchModel[MeasuredEvent, String] with Serializable {

    val className = "org.ekstep.analytics.updater.ContentUsageUpdater"
  
    def execute(events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        JobLogger.debug("Execute method started", className)
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val contentSummary = events.map { x =>
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
            ContentUsageSummaryFact(content_id, CommonUtil.getPeriod(x.syncts, DAY), group_user, content_type, mime_type, publish_date, new DateTime(x.syncts),
                total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, None, None);
        }.cache();

        // Roll up summaries
        val contentSummaries = contentSummary.union(rollup(contentSummary, WEEK)).union(rollup(contentSummary, MONTH)).union(rollup(contentSummary, CUMULATIVE)).cache();

        // Update the database
        contentSummaries.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)

        // Compute top K content 
        val summaries = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).filter { x => !"Collection".equals(x.d_content_type) };
        val cumulativeSummaries = summaries.filter { x => x.d_period == 0 }.groupBy(x => (x.d_content_id, x.d_period)).map(f => f._2.reduce((a, b) => reduce(a, b, CUMULATIVE)));
        val count = cumulativeSummaries.count().intValue();
        val defaultVal = if (5 > count) count else 5;
        val topContentByTime = cumulativeSummaries.sortBy(f => f.m_total_ts, false, 1).take(configMapping.value.getOrElse("topK", defaultVal).asInstanceOf[Int]);
        val topContentBySessions = cumulativeSummaries.sortBy(f => f.m_total_sessions, false, 1).take(configMapping.value.getOrElse("topK", defaultVal).asInstanceOf[Int]);
        val topKContent = sc.parallelize(Array(ContentMetrics("top_content", topContentByTime.map { x => (x.d_content_id, x.m_total_ts) }.toMap, topContentBySessions.map { x => (x.d_content_id, x.m_total_sessions) }.toMap)), 1);
        topKContent.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_METRICS_TABLE);
        
        JobLogger.debug("Execute method ended", className)
        contentSummaries.map { x => JSONUtils.serialize(ContentUsageSummaryIndex(x.d_content_id, x.d_period, x.d_group_user)) };
    }

    /**
     * Rollup daily summaries by period. The period summaries are joined with the previous entries in the database and then reduced to produce new summaries.
     */
    private def rollup(data: RDD[ContentUsageSummaryFact], period: Period): RDD[ContentUsageSummaryFact] = {

        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.m_last_sync_date.getMillis, period);
            (ContentUsageSummaryIndex(x.d_content_id, d_period, x.d_group_user), x);
        }
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).on(SomeColumns("d_content_id", "d_period", "d_group_user"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(ContentUsageSummaryFact(index.d_content_id, index.d_period, index.d_group_user, newSumm.d_content_type, newSumm.d_mime_type,
                newSumm.m_publish_date, newSumm.m_last_sync_date, 0.0, 0, 0.0, 0, 0.0, None, None))
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    /**
     * Reducer to rollup two summaries
     */
    private def reduce(fact1: ContentUsageSummaryFact, fact2: ContentUsageSummaryFact, period: Period): ContentUsageSummaryFact = {
        val total_ts = fact2.m_total_ts + fact1.m_total_ts
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_ts_session = (total_ts) / (total_sessions)
        val total_interactions = fact2.m_total_interactions + fact1.m_total_interactions
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val publish_date = if (fact2.m_publish_date.isBefore(fact1.m_publish_date)) fact2.m_publish_date else fact1.m_publish_date;
        val sync_date = if (fact2.m_last_sync_date.isAfter(fact1.m_last_sync_date)) fact2.m_last_sync_date else fact1.m_last_sync_date;
        val numWeeks = CommonUtil.getWeeksBetween(publish_date.getMillis, sync_date.getMillis)
        val avg_sessions_week = period match {
            case MONTH      => Option(total_sessions.toDouble / 5)
            case CUMULATIVE => Option(if (numWeeks != 0) (total_sessions.toDouble) / numWeeks else total_sessions)
            case _          => None
        }
        val avg_ts_week = period match {
            case MONTH      => Option(total_ts / 5)
            case CUMULATIVE => Option(if (numWeeks != 0) (total_ts) / numWeeks else total_ts)
            case _          => None
        }
        ContentUsageSummaryFact(fact1.d_content_id, fact1.d_period, fact1.d_group_user, fact1.d_content_type, fact1.d_mime_type, publish_date, sync_date,
            total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, avg_sessions_week, avg_ts_week);
    }
}