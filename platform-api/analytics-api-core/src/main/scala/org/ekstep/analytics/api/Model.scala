package org.ekstep.analytics.api

import org.joda.time.DateTime

/**
 * @author Santhosh
 */
object Model {

}

case class Filter(partner_id: Option[String] = None, group_user: Option[Boolean] = None, content_id: Option[String] = None, tag: Option[String] = None);
case class Trend(day: Option[Int], week: Option[Int], month: Option[Int])
case class Request(filter: Option[Filter], summaries: Option[Array[String]], trend: Option[Trend], context: Option[Map[String, AnyRef]], query: Option[String], filters: Option[Map[String, AnyRef]], config: Option[Map[String, AnyRef]], limit: Option[Int]);
case class RequestBody(id: String, ver: String, ts: String, request: Request, param: Option[Params]);
case class MetricsRequest(period: String, filter: Option[Filter]);
case class MetricsRequestBody(id: String, ver: String, ts: String, request: MetricsRequest, param: Option[Params]);

case class ContentSummary(period: Option[Int], total_ts: Double, total_sessions: Long, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double)
case class ItemMetrics(m_item_id: String, m_total_ts: Double, m_total_count: Integer, m_correct_res_count: Integer, m_inc_res_count: Integer, m_top5_incorrect_res: Array[String], m_avg_ts: Double)
case class ContentUsageMetrics(d_period: Option[Int], m_total_sessions: Long, m_total_ts: Double, m_total_interactions: Double, m_total_devices: Long, m_avg_sessions: Long, m_avg_ts: Double, m_avg_interactions_min: Double)
case class Comment(comment: String, date: Int);
case class ContentPopularityMetrics(d_period: Option[Int], m_downloads: Long, m_side_loads: Long, m_comments: Option[Array[Comment]], m_avg_rating: Double); 
case class GenieUsageMetrics(d_period: Option[Int], m_total_sessions: Long, m_total_ts: Double, m_total_devices: Long, m_avg_sessions: Long, m_avg_ts: Double)

case class Params(resmsgid: String, msgid: String, err: String, status: String, errmsg: String);
case class Result(metrics: Array[Map[String, AnyRef]], summary: Map[String, AnyRef], ttl: Int);
case class MetricsResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Result);
case class Response(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Option[Map[String, AnyRef]]);

case class Range(start: Int, end: Int);
case class ContentId(d_content_id: String);

case class ContentUsageSummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime,
                                      m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double)
case class ContentUsageSummary(d_period: Int, var label: Option[String], m_publish_date: Option[Long], m_last_sync_date: Option[Long], m_last_gen_date: Option[Long], m_total_ts: Option[Double], m_total_sessions: Option[Long], m_avg_ts_session: Option[Double], m_total_interactions: Option[Long], m_avg_interactions_min: Option[Double])
                                      
case class RecommendationContent(device_id: String, scores: List[(String, Double)])
case class ContentVectors(content_vectors: Array[ContentVector]);
class ContentVector(val contentId: String,val text_vec: List[Double], val tag_vec: List[Double]);

object ResponseCode extends Enumeration {
	type Code = Value
	val OK, CLIENT_ERROR, SERVER_ERROR, REQUEST_TIMEOUT, RESOURCE_NOT_FOUND = Value
}

object Period extends Enumeration {
    type Period = Value
    val DAY, WEEK, MONTH, CUMULATIVE, LAST7, LAST30, LAST90 = Value
}

object MetricsPeriod extends Enumeration {
    type MetricsPeriod = Value
    val LAST_7_DAYS, LAST_5_WEEKS, LAST_12_MONTHS, CUMULATIVE = Value
}

object Constants {
    val CONTENT_DB = "content_db";
    val DEVICE_DB = "device_db";
    val CONTENT_SUMMARY_FACT_TABLE = "content_usage_summary_fact";
    val DEVICE_RECOS_TABLE = "device_recos";
    val CONTENT_TO_VEC = "content_to_vector";
    val REGISTERED_TAGS = "registered_tags";
}