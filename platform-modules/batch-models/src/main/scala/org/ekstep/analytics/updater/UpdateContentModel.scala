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

case class PopularityUpdaterInput(contentId: String, contentSummary: Option[ContentUsageSummaryView], popularitySummary: Option[ContentPopularitySummaryView]) extends AlgoInput
case class PopularityUpdaterOutput(contentId: String, metrics: Map[String, AnyVal], reponseCode: String, errorMsg: Option[String]) extends AlgoOutput with Output

/**
 * @author Santhosh
 */
object UpdateContentModel extends IBatchModelTemplate[DerivedEvent, PopularityUpdaterInput, PopularityUpdaterOutput, PopularityUpdaterOutput] with Serializable {

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

    override def algorithm(data: RDD[PopularityUpdaterInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterOutput] = {
        data.map { x =>
            val url = Constants.getContentUpdateAPIUrl(x.contentId);
            val usageMap = if (x.contentSummary.isDefined) {
                Map("popularity" -> x.contentSummary.get.m_total_ts,
                    "me_totalSessionsCount" -> x.contentSummary.get.m_total_sessions,
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
            val versionKey = AppConf.getConfig("lp.contentmodel.versionkey").asInstanceOf[AnyVal];
            val metrics = usageMap ++ popularityMap
            val contentMap = metrics ++ Map("versionKey" -> versionKey);
            val request = Map("request" -> Map("content" -> contentMap));            
            val r = RestUtil.patch[Response](url, JSONUtils.serialize(request));
            JobLogger.log("org.ekstep.analytics.updater.UpdateContentModel", Option(Map("contentId" -> x.contentId, "metrics" -> metrics, "responseCode" -> r.responseCode, "errorMsg" ->  r.params.errmsg)), INFO)("org.ekstep.analytics.updater.UpdateContentModel");
            PopularityUpdaterOutput(x.contentId, metrics, r.responseCode, r.params.errmsg)
        };
    }

    override def postProcess(data: RDD[PopularityUpdaterOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterOutput] = {
    	data
    }
}