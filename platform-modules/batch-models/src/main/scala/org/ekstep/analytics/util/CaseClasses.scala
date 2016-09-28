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

class CaseClasses extends Serializable {}

/* Computed Event Without Optional Fields - Start */

@scala.reflect.BeanInfo
class DerivedEvent(val eid: String, val ets: Long, val syncts: Long, val ver: String, val mid: String, val uid: String, val content_id: String,
                   val context: Context, val dimensions: Dimensions, val edata: MEEdata, val tags: AnyRef) extends Input with AlgoInput;

@scala.reflect.BeanInfo
class Dimensions(val uid: String, val did: String, val gdata: GData, val domain: String, val loc: String, val group_user: Boolean, val anonymous_user: Boolean) extends Serializable;

@scala.reflect.BeanInfo
class Context(val pdata: PData, val dspec: Map[String, String], val granularity: String, val date_range: DtRange) extends Serializable;

@scala.reflect.BeanInfo
class Eks(val id: String, val ver: String, val levels: Array[Map[String, Any]], val noOfAttempts: Int, val timeSpent: Double,
                     val interruptTime: Double, val timeDiff: Double, val start_time: Long, val end_time: Long, val currentLevel: Map[String, String],
                     val noOfLevelTransitions: Int, val interactEventsPerMin: Double, val completionStatus: Boolean, val screenSummary: Array[AnyRef], 
                     val noOfInteractEvents: Int, val eventsSummary: Array[AnyRef], val syncDate: Long, val contentType: AnyRef, val mimeType: AnyRef, 
                     val did: String, val tags: AnyRef, val telemetryVer: String, val itemResponses: Array[AnyRef])

@scala.reflect.BeanInfo
class MEEdata(val eks: Eks) extends Serializable;
/* Computed Event Without Optional Fields - End */

/* Cassandra Models */
case class ContentSummaryIndex(d_period: Int, d_content_id: String, d_tag: String) extends Output;
case class ContentUsageSummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_devices: Long, m_avg_sess_device: Double, m_device_ids: Array[Byte]) extends AlgoOutput with CassandraTable;
case class ContentPopularitySummaryFact(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, DateTime)], m_ratings: List[(Double, DateTime)], m_avg_rating: Double) extends AlgoOutput with CassandraTable;

/* Cassandra Readonly Models */
case class ContentUsageSummaryView(d_period: Int, d_content_id: String, d_tag: String, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_devices: Long, m_avg_sess_device: Double) extends CassandraTable
case class ContentPopularitySummaryView(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, DateTime)], m_ratings: List[(Double, DateTime)], m_avg_rating: Double) extends CassandraTable
case class GenieLaunchSummaryView(d_period: Int, d_tag: String, m_total_sessions: Long, m_total_ts: Double, m_total_devices: Long, m_avg_sess_device: Double, m_avg_ts_session: Long, m_contents: List[String]) extends CassandraTable