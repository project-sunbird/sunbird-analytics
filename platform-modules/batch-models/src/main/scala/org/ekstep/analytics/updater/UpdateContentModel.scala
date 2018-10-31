package org.ekstep.analytics.updater

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.{Constants, ContentPopularitySummaryView, WorkFlowUsageSummaryFact}
import org.joda.time.DateTime

case class PopularityUpdaterInput(contentId: String, workflowSummary: Option[WorkFlowUsageSummaryFact], popularitySummary: Option[ContentPopularitySummaryView]) extends AlgoInput
case class GraphUpdateEvent(ets: Long, nodeUniqueId: String, transactionData: Map[String, Map[String, Map[String, Any]]], objectType: String, operationType: String = "UPDATE", nodeType: String = "DATA_NODE", graphId: String = "domain", nodeGraphId: Int = 0) extends AlgoOutput with Output

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
        val popularityInfo = sc.cassandraTable[ContentPopularitySummaryView](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT).where("updated_date>=?", start_time).where("updated_date<=?", end_time).filter { x => (x.d_period == 0) & ("all".equals(x.d_tag)) }.map(f => (f.d_content_id, f));

        val workflowSummaryUsageInfo = sc.cassandraTable[WorkFlowUsageSummaryFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT).where("m_updated_date>=?", start_time).where("m_updated_date<=?", end_time).filter { x => (x.d_period == 0) & ("all".equals(x.d_tag)) & !(x.d_content_id.equals("all")) }.map(f => (f.d_content_id, f));

        val groupSummaries = workflowSummaryUsageInfo.cogroup(popularityInfo)
        groupSummaries.map{ x =>
          val workflowSummary = if(x._2._1.size > 0) x._2._1 else (Iterable())
          val popularityInfoSummary = if(x._2._2.size > 0) x._2._2 else (Iterable())
          PopularityUpdaterInput(x._1, if(workflowSummary.size > 0) Option(workflowSummary.head) else None, if (popularityInfoSummary.size > 0) Option(popularityInfoSummary.head) else None)

        }

    }

    override def algorithm(data: RDD[PopularityUpdaterInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GraphUpdateEvent] = {
        data.map { x =>
            val objectType = if(x.workflowSummary.isDefined){
                x.workflowSummary.get.m_content_type
            }else{
                "Content"
            }
            val workflowUsageMap = if (x.workflowSummary.isDefined) {
                Map("me_totalSessionsCount" -> x.workflowSummary.get.m_total_sessions,
                    "me_totalTimespent" -> x.workflowSummary.get.m_total_ts,
                    "me_totalInteractions" -> x.workflowSummary.get.m_total_interactions,
                    "me_averageInteractionsPerMin" -> x.workflowSummary.get.m_avg_interactions_min,
                    "me_averageTimespentPerSession" -> x.workflowSummary.get.m_avg_ts_session)
            } else {
                Map(""-> 0.0)
            }
            val popularityMap = if (x.popularitySummary.isDefined) {
                Map("me_averageRating" -> x.popularitySummary.get.m_avg_rating,
                    "me_totalDownloads" -> x.popularitySummary.get.m_downloads,
                    "me_totalSideloads" -> x.popularitySummary.get.m_side_loads,
                    "me_totalRatings" -> x.popularitySummary.get.m_ratings.size,
                    "me_totalComments" -> x.popularitySummary.get.m_comments.size)
            } else {
                Map(""-> 0.0)
            }

            val metrics = workflowUsageMap ++ popularityMap
            val finalContentMap = metrics.filter(x=> x._1.nonEmpty).map{ x => (x._1 -> Map("ov" -> null, "nv" -> x._2))}
            GraphUpdateEvent(DateTime.now().getMillis, x.contentId, Map("properties" -> finalContentMap),objectType )
        }.cache();
    }

    override def postProcess(data: RDD[GraphUpdateEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GraphUpdateEvent] = {

        data
    }
}