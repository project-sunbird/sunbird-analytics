package org.ekstep.analytics.util

import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.DtRange
import org.joda.time.DateTime
import org.ekstep.analytics.framework.CassandraTable
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.InCorrectRes
import org.ekstep.analytics.framework.Misconception

class CaseClasses extends Serializable {}

/* Computed Event Without Optional Fields - Start */

@scala.beans.BeanInfo
class DerivedEvent(val eid: String, val ets: Long, val syncts: Long, val ver: String, val mid: String, val uid: String, val content_id: String,
                   val context: Context, val dimensions: Dimensions, val edata: MEEdata, val tags: AnyRef) extends Input with AlgoInput;

@scala.beans.BeanInfo
class Dimensions(val uid: String, val did: String, val gdata: GData, val domain: String, val loc: String, val group_user: Boolean, val anonymous_user: Boolean) extends Serializable;

@scala.beans.BeanInfo
class Context(val pdata: PData, val dspec: Map[String, String], val granularity: String, val date_range: DtRange) extends Serializable;

@scala.beans.BeanInfo
class Eks(val id: String, val ver: String, val levels: Array[Map[String, Any]], val noOfAttempts: Int, val timeSpent: Double,
          val interruptTime: Double, val timeDiff: Double, val start_time: Long, val end_time: Long, val currentLevel: Map[String, String],
          val noOfLevelTransitions: Int, val interactEventsPerMin: Double, val completionStatus: Boolean, val screenSummary: Array[AnyRef],
          val noOfInteractEvents: Int, val eventsSummary: Array[AnyRef], val syncDate: Long, val contentType: AnyRef, val mimeType: AnyRef,
          val did: String, val tags: AnyRef, val telemetryVer: String, val itemResponses: Array[AnyRef])

@scala.beans.BeanInfo
class MEEdata(val eks: Eks) extends Serializable;
/* Computed Event Without Optional Fields - End */

/* Cassandra Models */
case class ContentSummaryIndex(d_period: Int, d_content_id: String, d_tag: String) extends Output;
case class ItemUsageSummaryIndex(d_period: Int, d_tag: String, d_content_id: String, d_item_id: String) extends Output
case class ContentUsageSummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_devices: Long, m_avg_sess_device: Double, m_device_ids: Array[Byte], updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with CassandraTable;
case class ContentPopularitySummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, DateTime)], m_ratings: List[(Double, DateTime)], m_avg_rating: Double, updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with CassandraTable;
case class ContentPopularitySummaryFact2(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, Long)], m_ratings: List[(Double, Long)], m_avg_rating: Double, updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with CassandraTable;
case class ItemUsageSummaryFact(d_period: Int, d_tag: String, d_content_id: String, d_item_id: String, m_total_ts: Double, m_total_count: Int, m_correct_res_count: Int, m_inc_res_count: Int, m_correct_res: List[String], m_incorrect_res: List[(String, List[String], Int)], m_top5_incorrect_res: List[(String, List[String], Int)], m_avg_ts: Double, m_top5_mmc: List[(String, Int)], updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with CassandraTable;

/* Cassandra Readonly Models */
case class ContentUsageSummaryView(d_period: Int, d_content_id: String, d_tag: String, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_devices: Long, m_avg_sess_device: Double) extends CassandraTable
case class ContentPopularitySummaryView(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, Long)], m_ratings: List[(Double, Long)], m_avg_rating: Double) extends CassandraTable
case class GenieLaunchSummaryView(d_period: Int, d_tag: String, m_total_sessions: Long, m_total_ts: Double, m_total_devices: Long, m_avg_sess_device: Double, m_avg_ts_session: Long, m_contents: List[String]) extends CassandraTable
case class ItemUsageSummaryView(d_period: Int, d_content_id: String, d_tag: String, d_item_id: String, m_total_ts: Double, m_total_count: Int, m_correct_res_count: Int, m_inc_res_count: Int, m_correct_res: List[String], m_top5_incorrect_res: List[(String, List[String], Int)], m_avg_ts: Double, m_top5_mmc: List[(String, Int)]) extends CassandraTable

// Metric Model
case class ConfigDetails(keyspace: String, table: String, periodfrom: String, periodUpTo: String, filePrefix: String, fileSuffix: String, dispatchTo: String, dispatchParams: Map[String, AnyRef]);

// Content Store
case class ContentData(content_id: String, body: Option[Array[Byte]], last_updated_on: Option[DateTime], oldbody: Option[Array[Byte]]);

/* Job Request & Data Exhaust */
case class JobRequest(client_key: String, request_id: String, job_id: Option[String], status: String, request_data: String,
                      location: Option[String], dt_file_created: Option[DateTime], dt_first_event: Option[DateTime], dt_last_event: Option[DateTime],
                      dt_expiration: Option[DateTime], iteration: Option[Int], dt_job_submitted: DateTime, dt_job_processing: Option[DateTime],
                      dt_job_completed: Option[DateTime], input_events: Option[Long], output_events: Option[Long], file_size: Option[Long], latency: Option[Int],
                      execution_time: Option[Long], err_message: Option[String], stage: Option[String], stage_status: Option[String]) extends AlgoOutput

case class RequestFilter(start_date: String, end_date: String, tags: Option[List[String]], events: Option[List[String]], app_id: Option[String], channel: Option[String]);
case class RequestConfig(filter: RequestFilter, dataset_id: Option[String] = Option("eks-consumption-raw"), output_format: Option[String] = None);
case class RequestOutput(request_id: String, output_events: Int)
case class DataExhaustJobInput(eventDate: Long, event: String, eid: String) extends AlgoInput;
case class JobResponse(client_key: String, request_id: String, job_id: String, output_events: Long, bucket: String, prefix: String, first_event_date: Long, last_event_date: Long);
case class JobStage(request_id: String, client_key: String, stage: String, stage_status: String, status: String, err_message: String = "")
