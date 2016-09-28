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

case class PopularityUpdaterInput(contentId: String, contentSummary: Option[ContentUsageSummaryView], popularitySummary: Option[ContentPopularitySummaryView]) extends AlgoInput
case class PopularityUpdaterOutut(contentId: String, metrics: Map[String, AnyVal], reponseCode: String, errorMsg: Option[String]) extends AlgoOutput with Output

/**
 * @author Santhosh
 */
object UpdateContentModel extends IBatchModelTemplate[DerivedEvent, PopularityUpdaterInput, PopularityUpdaterOutut, PopularityUpdaterOutut] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateContentModel"
    override def name: String = "UpdateContentModel"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterInput] = {
        val usageContents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_CONTENT_USAGE_SUMMARY"))).map { x => ContentSummaryIndex(0, x.dimensions.content_id.get, "all") }.distinct();
        val usageSummaries = usageContents.joinWithCassandraTable[ContentUsageSummaryView](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).on(SomeColumns("d_period", "d_content_id", "d_tag")).map(f => (f._1.d_content_id, f._2));

        val popularitySummaries = usageContents.joinWithCassandraTable[ContentPopularitySummaryView](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT).on(SomeColumns("d_period", "d_content_id", "d_tag")).map(f => (f._1.d_content_id, f._2));

        val groupSummaries = usageSummaries.cogroup(popularitySummaries);
        groupSummaries.map(f =>
            PopularityUpdaterInput(f._1, if (f._2._1.size > 0) Option(f._2._1.head) else None, if (f._2._2.size > 0) Option(f._2._2.head) else None))
    }

    override def algorithm(data: RDD[PopularityUpdaterInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterOutut] = {
        data.map { x =>
            val url = Constants.getContentUpdateAPIUrl(x.contentId);
            val usageMap = if (x.contentSummary.isDefined) {
                Map("popularity" -> x.contentSummary.get.m_total_ts,
                    "me:totalSessionsCount" -> x.contentSummary.get.m_total_sessions,
                    "me:totalTimespent" -> x.contentSummary.get.m_total_ts,
                    "me:totalInteractions" -> x.contentSummary.get.m_total_interactions,
                    "me:averageInteractionsPerMin" -> x.contentSummary.get.m_avg_interactions_min,
                    "me:averageSessionsPerDevice" -> x.contentSummary.get.m_avg_sess_device,
                    "me:totalDevices" -> x.contentSummary.get.m_total_devices,
                    "me:averageTimespentPerSession" -> x.contentSummary.get.m_avg_ts_session)
            } else {
                Map();
            }
            val popularityMap = if (x.popularitySummary.isDefined) {
                Map("me:averageRating" -> x.popularitySummary.get.m_avg_rating,
                    "me:totalDownloads" -> x.popularitySummary.get.m_downloads,
                    "me:totalSideloads" -> x.popularitySummary.get.m_side_loads,
                    "me:totalRatings" -> x.popularitySummary.get.m_ratings.size,
                    "me:totalComments" -> x.popularitySummary.get.m_comments.size)
            } else {
                Map();
            }
            val contentMap = usageMap ++ popularityMap;
            val request = Map("request" -> Map("content" -> contentMap));
            val r = RestUtil.patch[Response](url, JSONUtils.serialize(request));
            PopularityUpdaterOutut(x.contentId, contentMap, r.responseCode, r.params.errmsg)
        };
    }

    override def postProcess(data: RDD[PopularityUpdaterOutut], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterOutut] = {
        data
    }
}