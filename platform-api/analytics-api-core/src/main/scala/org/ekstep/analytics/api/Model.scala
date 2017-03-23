package org.ekstep.analytics.api

import org.joda.time.DateTime
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.EData
import org.ekstep.analytics.framework.MEEdata

/**
 * @author Santhosh
 */
object Model {

}

case class Filter(partner_id: Option[String] = None, group_user: Option[Boolean] = None, content_id: Option[String] = None, tag: Option[String] = None, tags: Option[Array[String]] = None, start_date: Option[String] = None, end_date: Option[String] = None, events: Option[Array[String]] = None);
case class Trend(day: Option[Int], week: Option[Int], month: Option[Int])
case class Request(filter: Option[Filter], summaries: Option[Array[String]], trend: Option[Trend], context: Option[Map[String, AnyRef]], query: Option[String], filters: Option[Map[String, AnyRef]], config: Option[Map[String, AnyRef]], limit: Option[Int], output_format: Option[String] = Option("json"));
case class RequestBody(id: String, ver: String, ts: String, request: Request, params: Option[Params]);
case class MetricsRequest(period: String, filter: Option[Filter]);
case class MetricsRequestBody(id: String, ver: String, ts: String, request: MetricsRequest, param: Option[Params]);

case class ContentSummary(period: Option[Int], total_ts: Double, total_sessions: Long, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double)
case class ItemMetrics(m_item_id: String, m_total_ts: Double, m_total_count: Integer, m_correct_res_count: Integer, m_inc_res_count: Integer, m_top5_incorrect_res: Array[String], m_avg_ts: Double)
case class Comment(comment: String, date: Int);
case class GenieUsageMetrics(d_period: Option[Int], m_total_sessions: Long, m_total_ts: Double, m_total_devices: Long, m_avg_sessions: Long, m_avg_ts: Double)

case class Params(resmsgid: String, msgid: String, err: String, status: String, errmsg: String, client_key: Option[String] = None);
case class Result(metrics: Array[Map[String, AnyRef]], summary: Map[String, AnyRef]);
case class MetricsResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Result);
case class Response(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Option[Map[String, AnyRef]]);

case class Range(start: Int, end: Int);
case class ContentId(d_content_id: String);

case class ContentUsageSummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime,
	m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double)

case class ContentUsageMetrics(override val d_period: Option[Int] = None, label: Option[String] = None, m_total_ts: Option[Double] = Option(0.0), m_total_sessions: Option[Long] = Option(0), m_avg_ts_session: Option[Double] = Option(0.0), m_total_interactions: Option[Long] = Option(0), m_avg_interactions_min: Option[Double] = Option(0.0), m_total_devices: Option[Long] = Option(0), m_avg_sess_device: Option[Double] = Option(0.0)) extends Metrics;
case class ContentUsageListMetrics(override val d_period: Option[Int] = None, label: Option[String] = None, m_contents: Option[List[AnyRef]] = Option(List()), content: Option[List[AnyRef]] = Option(List())) extends Metrics;
case class ContentPopularityMetrics(override val d_period: Option[Int] = None, label: Option[String] = None, m_comments: Option[List[(String, Long)]] = None, m_downloads: Option[Long] = Option(0), m_side_loads: Option[Long] = Option(0), m_ratings: Option[List[Rating]] = Option(List()), m_avg_rating: Option[Double] = Option(0.0)) extends Metrics;
case class ContentPopularityViews(override val d_period: Option[Int] = None, label: Option[String] = None, m_comments: Option[List[(String, Long)]] = None, m_downloads: Option[Long] = Option(0), m_side_loads: Option[Long] = Option(0), m_ratings: Option[List[(Double, Long)]] = Option(List()), m_avg_rating: Option[Double] = Option(0.0)) extends Metrics;
case class GenieLaunchMetrics(override val d_period: Option[Int] = None, label: Option[String] = None, m_total_sessions: Option[Long] = Option(0), m_total_ts: Option[Double] = Option(0.0), m_total_devices: Option[Long] = Option(0), m_avg_sess_device: Option[Double] = Option(0.0), m_avg_ts_session: Option[Double] = Option(0)) extends Metrics;
case class ItemUsageSummaryView(override val d_period: Option[Int], d_content_id: String, d_tag: String, d_item_id: String, m_total_ts: Double, m_total_count: Int, m_correct_res_count: Int, m_inc_res_count: Int, m_correct_res: List[String], m_top5_incorrect_res: List[(String, List[String], Int)], m_avg_ts: Double, m_top5_mmc: List[(String, Int)]) extends Metrics;
case class InCorrectRes(resp: String, mmc: List[String], count: Int);
case class Misconception(concept: String, count: Int);
case class Rating(rating: Double, timestamp: Long)
case class ItemUsageSummary(d_item_id: String, d_content_id: Option[String] = None, m_total_ts: Option[Double] = Option(0.0), m_total_count: Option[Long] = Option(0), m_correct_res_count: Option[Long] = Option(0), m_inc_res_count: Option[Long] = Option(0), m_correct_res: Option[List[AnyRef]] = Option(List()), m_top5_incorrect_res: Option[List[InCorrectRes]] = Option(List()), m_avg_ts: Option[Double] = Option(0.0), m_top5_mmc: Option[List[Misconception]] = Option(List()))
case class ItemUsageMetrics(override val d_period: Option[Int] = None, label: Option[String] = None, items: Option[List[ItemUsageSummary]] = Option(List())) extends Metrics;

case class JobRequest(client_key: Option[String], request_id: Option[String], job_id: Option[String], status: Option[String], request_data: Option[String], iteration: Option[Int], dt_job_submitted: Option[DateTime] = None, location: Option[String] = None, dt_file_created: Option[DateTime] = None, dt_first_event: Option[DateTime] = None, dt_last_event: Option[DateTime] = None, dt_expiration: Option[DateTime] = None, dt_job_processing: Option[DateTime] = None, dt_job_completed: Option[DateTime] = None, input_events: Option[Int] = None, output_events: Option[Int] = None, file_size: Option[Long] = None, latency: Option[Int] = None, execution_time: Option[Long] = None, err_message: Option[String] = None, stage: Option[String] = None, stage_status: Option[String] = None)

case class RecommendationContent(device_id: String, scores: List[(String, Double)])
case class ContentVectors(content_vectors: Array[ContentVector]);
class ContentVector(val contentId: String, val text_vec: List[Double], val tag_vec: List[Double]);

object ResponseCode extends Enumeration {
	type Code = Value
	val OK, CLIENT_ERROR, SERVER_ERROR, REQUEST_TIMEOUT, RESOURCE_NOT_FOUND = Value
}

object Constants {
	val CONTENT_DB = "content_db";
	val DEVICE_DB = "device_db";
	val PLATFORML_DB = "platform_db";
	val JOB_REQUEST = "job_request";
	val CONTENT_SUMMARY_FACT_TABLE = "content_usage_summary_fact";
	val DEVICE_RECOS_TABLE = "device_recos";
	val CONTENT_RECOS_TABLE = "content_recos"
	val CONTENT_TO_VEC = "content_to_vector";
	val REGISTERED_TAGS = "registered_tags";
}

object OutputFormat {
  val CSV = "csv"
  val JSON = "json"
}
object APIIds {
	val RECOMMENDATIONS = "ekstep.analytics.recommendations"
	val DATA_REQUEST = "ekstep.analytics.dataset.request.submit";
	val GET_DATA_REQUEST = "ekstep.analytics.dataset.request.info";
	val GET_DATA_REQUEST_LIST = "ekstep.analytics.dataset.request.list";
	val CONTENT_USAGE = "ekstep.analytics.metrics.content-usage"
	val CONTENT_POPULARITY ="ekstep.analytics.metrics.content-popularity"
	val ITEM_USAGE = "ekstep.analytics.metrics.item-usage"
	val CONTENT_LIST = "ekstep.analytics.content-list"
	val GENIE_LUNCH = "ekstep.analytics.metrics.genie-launch"
	val CREATION_RECOMMENDATIONS = "ekstep.analytics.creation.recommendations"
}

case class JobOutput(location: Option[String] = None, file_size: Option[Long] = None, dt_file_created: Option[String] = None, dt_first_event: Option[Long] = None, dt_last_event: Option[Long] = None, dt_expiration: Option[Long] = None);
case class JobStats(dt_job_submitted: Long, dt_job_processing:  Option[Long] = None, dt_job_completed:  Option[Long] = None, input_events: Option[Int] = None, output_events: Option[Int] = None, latency: Option[Int] = None, execution_time: Option[Long] = None);
case class JobResponse(request_id: String, status: String, last_updated: Long, request_data: Request, attempts: Int, output: Option[JobOutput] = None, job_stats: Option[JobStats] = None);