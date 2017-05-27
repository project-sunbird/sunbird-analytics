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
case class ContentUsageSummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_devices: Long, m_avg_sess_device: Double, m_device_ids: Array[Byte], updated_date: DateTime = DateTime.now()) extends AlgoOutput with CassandraTable;
case class ContentPopularitySummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, DateTime)], m_ratings: List[(Double, DateTime)], m_avg_rating: Double, updated_date: DateTime = DateTime.now()) extends AlgoOutput with CassandraTable;
case class ContentPopularitySummaryFact2(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, Long)], m_ratings: List[(Double, Long)], m_avg_rating: Double, updated_date: DateTime = DateTime.now()) extends AlgoOutput with CassandraTable;
case class ItemUsageSummaryFact(d_period: Int, d_tag: String, d_content_id: String, d_item_id: String, m_total_ts: Double, m_total_count: Int, m_correct_res_count: Int, m_inc_res_count: Int, m_correct_res: List[String], m_incorrect_res: List[(String, List[String], Int)], m_top5_incorrect_res: List[(String, List[String], Int)], m_avg_ts: Double,  m_top5_mmc: List[(String, Int)], updated_date: DateTime = DateTime.now()) extends AlgoOutput with CassandraTable;

/* Cassandra Readonly Models */
case class ContentUsageSummaryView(d_period: Int, d_content_id: String, d_tag: String, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_devices: Long, m_avg_sess_device: Double) extends CassandraTable
case class ContentPopularitySummaryView(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, Long)], m_ratings: List[(Double, Long)], m_avg_rating: Double) extends CassandraTable
case class GenieLaunchSummaryView(d_period: Int, d_tag: String, m_total_sessions: Long, m_total_ts: Double, m_total_devices: Long, m_avg_sess_device: Double, m_avg_ts_session: Long, m_contents: List[String]) extends CassandraTable
case class ItemUsageSummaryView(d_period: Int, d_content_id: String, d_tag: String, d_item_id: String, m_total_ts: Double, m_total_count: Int, m_correct_res_count: Int, m_inc_res_count: Int, m_correct_res: List[String], m_top5_incorrect_res: List[(String,List[String], Int)], m_avg_ts: Double, m_top5_mmc: List[(String, Int)]) extends CassandraTable


// Content Store
case class ContentData(content_id: String, body: Option[Array[Byte]], last_updated_on: Option[DateTime], oldbody: Option[Array[Byte]]);
