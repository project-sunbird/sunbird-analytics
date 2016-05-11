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

case class ContentDetail(content_id: String, ts: Long, partner_id: String, group_user: Boolean, content_type: String, mime_type: String, publish_date: DateTime, total_ts: Double, total_sessions: Int, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double)
case class ContentUsageSummary(d_content_id: String, d_period: Int, d_partner_id: String, d_group_user: Boolean, d_content_type: String, d_mime_type: String, m_publish_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_avg_sessions_week: Double, m_avg_ts_week: Double)
case class PeriodCode(period: Int)

object ContentUsageUpdater extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val contentSummary = events.map { x =>
            val ts = x.syncts
            val startDate = x.context.date_range.from

            val content_id = x.content_id.get
            val partner_id = x.dimensions.partner_id.get
            val group_user = x.dimensions.group_user.get

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val content_type = eksMap.get("content_type").get.asInstanceOf[String]
            val mime_type = eksMap.get("mime_type").get.asInstanceOf[String]
            val publish_date = new DateTime(startDate)
            val total_ts = eksMap.get("total_ts").get.asInstanceOf[Double]
            val total_sessions = eksMap.get("total_sessions").get.asInstanceOf[Int]
            val avg_ts_session = eksMap.get("avg_ts_session").asInstanceOf[Double]
            val total_interactions = eksMap.get("total_interactions").get.asInstanceOf[Long]
            val avg_interactions_min = eksMap.get("avg_interactions_min").get.asInstanceOf[Double]
            ContentDetail(content_id, ts, partner_id, group_user, content_type, mime_type, publish_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min);
        }
        null;
    }

    private def updateDbWithPeriod(data: RDD[ContentDetail], period: Period) {

        period match {
            case DAY =>
                data.map { x =>
                    ContentUsageSummary(x.content_id, CommonUtil.getPeriod(x.ts, DAY), x.partner_id, x.group_user, x.content_type, x.mime_type, x.publish_date, x.total_ts, x.total_sessions, x.avg_ts_session, x.total_interactions, x.avg_interactions_min, 0d, 0d)
                }.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)

            case WEEK =>
                val currentData = data.map { x =>
                    val periodCode = CommonUtil.getPeriod(x.ts, WEEK)
                    (periodCode, ContentUsageSummary(x.content_id, periodCode, x.partner_id, x.group_user, x.content_type, x.mime_type, x.publish_date, x.total_ts, x.total_sessions, x.avg_ts_session, x.total_interactions, x.avg_interactions_min, 0d, 0d));
                }
                val prvData = data.map { x => PeriodCode(CommonUtil.getPeriod(x.ts, WEEK)) }.joinWithCassandraTable[ContentUsageSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).map(f => (f._1.period, f._2))
                val joinedData = currentData.leftOuterJoin(prvData)
                val updatedSummary = joinedData.map { x =>
                    val code = x._1
                    val prvSumm = x._2._2.getOrElse(null)
                    val newSumm = x._2._1
                    if (null != prvSumm) {
                        val total_ts = newSumm.m_total_ts + prvSumm.m_total_ts
                        val total_sessions = newSumm.m_total_sessions + prvSumm.m_total_sessions
                        val avg_ts_session = (total_ts) / (total_sessions)
                        val total_interactions = newSumm.m_total_interactions + prvSumm.m_total_interactions
                        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
                        ContentUsageSummary(prvSumm.d_content_id, code, prvSumm.d_partner_id, prvSumm.d_group_user, prvSumm.d_content_type, prvSumm.d_mime_type, prvSumm.m_publish_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, 0d, 0d);
                    } else {
                        newSumm;
                    }
                }
                updatedSummary.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)

            case MONTH =>
                val currentData = data.map { x =>
                    val periodCode = CommonUtil.getPeriod(x.ts, MONTH)
                    (periodCode, ContentUsageSummary(x.content_id, periodCode, x.partner_id, x.group_user, x.content_type, x.mime_type, x.publish_date, x.total_ts, x.total_sessions, x.avg_ts_session, x.total_interactions, x.avg_interactions_min, 0d, 0d));
                }
                val prvData = data.map { x => PeriodCode(CommonUtil.getPeriod(x.ts, MONTH)) }.joinWithCassandraTable[ContentUsageSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).map(f => (f._1.period, f._2))
                val joinedData = currentData.leftOuterJoin(prvData)
                val updatedSummary = joinedData.map { x =>
                    val code = x._1
                    val prvSumm = x._2._2.getOrElse(null)
                    val newSumm = x._2._1
                    if (null != prvSumm) {
                        val total_ts = newSumm.m_total_ts + prvSumm.m_total_ts
                        val total_sessions = newSumm.m_total_sessions + prvSumm.m_total_sessions
                        val avg_ts_session = (total_ts) / (total_sessions)
                        val total_interactions = newSumm.m_total_interactions + prvSumm.m_total_interactions
                        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
                        val avg_sessions_week = (total_sessions) / 5
                        val avg_ts_week = (total_ts) / 5
                        ContentUsageSummary(prvSumm.d_content_id, code, prvSumm.d_partner_id, prvSumm.d_group_user, prvSumm.d_content_type, prvSumm.d_mime_type, prvSumm.m_publish_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, avg_sessions_week, avg_ts_week);
                    } else {
                        newSumm;
                    }
                }
                updatedSummary.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)

            case CUMULATIVE =>
                
            case _          =>

        }
    }
}