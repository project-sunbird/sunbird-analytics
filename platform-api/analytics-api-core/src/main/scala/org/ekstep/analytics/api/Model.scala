package org.ekstep.analytics.api

import org.joda.time.DateTime

/**
 * @author Santhosh
 */
object Model {

}

case class Filter(partner_id: Option[String], group_user: Option[Boolean]);
case class Trend(day: Option[Int], week: Option[Int], month: Option[Int])
case class Request(filter: Option[Filter], summaries: Option[Array[String]], trend: Option[Trend], context: Option[Map[String, AnyRef]], query: Option[String], filters: Option[Map[String, AnyRef]], limit: Option[Int]);
case class RequestBody(id: String, ver: String, ts: String, request: Request, param: Option[Params]);

case class ContentSummary(period: Option[Int], total_ts: Double, total_sessions: Long, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, avg_sessions_week: Option[Double], avg_ts_week: Option[Double])

case class Params(resmsgid: String, msgid: String, err: String, status: String, errmsg: String);
case class Response(id: String, ver: String, ts: String, params: Params, result: Option[Map[String, AnyRef]]);

case class Range(start: Int, end: Int);
case class ContentId(d_content_id: String);
case class ContentUsageSummaryFact(d_content_id: String, d_period: Int, d_group_user: Boolean, d_content_type: String, d_mime_type: String, m_publish_date: DateTime,
                                   m_last_sync_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long,
                                   m_avg_interactions_min: Double, m_avg_sessions_week: Option[Double], m_avg_ts_week: Option[Double])

case class ContentToVector(content_id: String, vector: Map[Int, String]);

object Period extends Enumeration {
    type Period = Value
    val DAY, WEEK, MONTH, CUMULATIVE, LAST7, LAST30, LAST90 = Value
}

object Constants {
    val CONTENT_DB = "content_db";
    val DEVICE_DB = "device_db";
    val CONTENT_SUMMARY_FACT_TABLE = "content_usage_summary_fact";
    val DEVICE_RECOS_TABLE = "device_recos";
    val CONTENT_TO_VEC = "content_to_vector";
}