package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.Filter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.util.UUID
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.ContentId
import org.ekstep.analytics.util.ContentUsageSummaryView
import org.ekstep.analytics.util.ContentPopularitySummaryView
import org.ekstep.analytics.util.ContentSummaryIndex
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.Level._

case class PopularityUpdaterInput(contentId: String, contentSummary: Option[ContentUsageSummaryView], popularitySummary: Option[ContentPopularitySummaryView], creationSummary: Option[CEUsageSummaryFact], creationMetrics: Option[ContentCreationMetrics]) extends AlgoInput
case class GraphUpdateEvent(ets: Long, nodeUniqueId: String, transactionData: Map[String, Map[String, Map[String, Any]]], operationType: String = "UPDATE", nodeType: String = "DATA_NODE", graphId: String = "domain", objectType: String = "Content", nodeGraphId: Int = 0) extends AlgoOutput with Output

/**
 * @author Santhosh
 */
object UpdateContentModel extends IBatchModelTemplate[DerivedEvent, PopularityUpdaterInput, GraphUpdateEvent, GraphUpdateEvent] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateContentModel"
    override def name: String = "UpdateContentModel"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterInput] = {

        val date = config.getOrElse("date", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String]
        val start_time = CommonUtil.dateFormat.parseDateTime(date).getMillis
        val end_time = CommonUtil.getEndTimestampOfDay(date)
        val usageInfo = sc.cassandraTable[ContentUsageSummaryView](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("updated_date>=?", start_time).where("updated_date<=?", end_time).filter { x => (x.d_period == 0) & ("all".equals(x.d_tag)) }.map(f => (f.d_content_id, f));
        val popularityInfo = sc.cassandraTable[ContentPopularitySummaryView](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT).where("updated_date>=?", start_time).where("updated_date<=?", end_time).filter { x => (x.d_period == 0) & ("all".equals(x.d_tag)) }.map(f => (f.d_content_id, f));

        val creationUsageInfo = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("updated_date>=?", start_time).where("updated_date<=?", end_time).filter { x => (x.d_period == 0) }.map(f => (f.d_content_id, f));
        val creationMetricsInfo = sc.cassandraTable[ContentCreationMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_CREATION_TABLE).where("updated_date>=?", start_time).where("updated_date<=?", end_time).map(f => (f.d_content_id, f));
        val creationSummaries = creationUsageInfo.cogroup(creationMetricsInfo)
        
        val groupSummaries = usageInfo.cogroup(popularityInfo).cogroup(creationSummaries);
        groupSummaries.map{ f =>
            val consumptionSummaries = if(f._2._1.size > 0) f._2._1.head else (Iterable(), Iterable())
            val creationSummaries = if(f._2._2.size > 0) f._2._2.head else (Iterable(), Iterable())
            PopularityUpdaterInput(f._1, if (consumptionSummaries._1.size > 0) Option(consumptionSummaries._1.head) else None, if (consumptionSummaries._2.size > 0) Option(consumptionSummaries._2.head) else None, if (creationSummaries._1.size > 0) Option(creationSummaries._1.head) else None, if (creationSummaries._2.size > 0) Option(creationSummaries._2.head) else None)
        }
    }

    override def algorithm(data: RDD[PopularityUpdaterInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GraphUpdateEvent] = {
        data.map { x =>
            val url = Constants.getContentUpdateAPIUrl(x.contentId);
            val usageMap = if (x.contentSummary.isDefined) {
                Map("me_totalSessionsCount" -> x.contentSummary.get.m_total_sessions,
                    "me_totalTimespent" -> x.contentSummary.get.m_total_ts,
                    "me_totalInteractions" -> x.contentSummary.get.m_total_interactions,
                    "me_averageInteractionsPerMin" -> x.contentSummary.get.m_avg_interactions_min,
                    "me_averageSessionsPerDevice" -> x.contentSummary.get.m_avg_sess_device,
                    "me_totalDevices" -> x.contentSummary.get.m_total_devices,
                    "me_averageTimespentPerSession" -> x.contentSummary.get.m_avg_ts_session)
            } else {
                Map();
            }
            val popularityMap = if (x.popularitySummary.isDefined) {
                Map("me_averageRating" -> x.popularitySummary.get.m_avg_rating,
                    "me_totalDownloads" -> x.popularitySummary.get.m_downloads,
                    "me_totalSideloads" -> x.popularitySummary.get.m_side_loads,
                    "me_totalRatings" -> x.popularitySummary.get.m_ratings.size,
                    "me_totalComments" -> x.popularitySummary.get.m_comments.size)
            } else {
                Map();
            }
            val creationUsageMap = if (x.creationSummary.isDefined) {
                Map("me_creationTimespent" -> x.creationSummary.get.total_ts,
                    "me_creationSessions" -> x.creationSummary.get.total_sessions,
                    "me_avgCreationTsPerSession" -> x.creationSummary.get.avg_ts_session)
            } else {
                Map();
            }
            val creationMetricsMap = if (x.creationMetrics.isDefined) {
                Map("me_imagesCount" -> x.creationMetrics.get.images_count,
                    "me_audiosCount" -> x.creationMetrics.get.audios_count,
                    "me_videosCount" -> x.creationMetrics.get.videos_count,
                    "me_timespentDraft" -> x.creationMetrics.get.time_spent_draft.get,
                    "me_timespentReview" -> x.creationMetrics.get.time_spent_review.get);
            } else {
                Map();
            }
            val metrics = usageMap ++ popularityMap ++ creationUsageMap ++ creationMetricsMap
            val finalContentMap = metrics.map{ x => (x._1 -> Map("ov" -> null, "nv" -> x._2))}.toList.toMap
            GraphUpdateEvent(DateTime.now().getMillis, x.contentId, Map("properties" -> finalContentMap))            
        }.cache();
    }

    override def postProcess(data: RDD[GraphUpdateEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GraphUpdateEvent] = {
    	data
    }
}