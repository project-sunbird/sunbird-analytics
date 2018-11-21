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

        val workflowSummaryUsageInfo = sc.cassandraTable[WorkFlowUsageSummaryFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT).where("m_updated_date>=?", start_time).where("m_updated_date<=?", end_time).filter { x => (x.d_period == 0) & ("all".equals(x.d_tag)) & ("all".equals(x.d_device_id)) & ("all".equals(x.d_user_id)) & !(x.d_content_id.equals("all")) }.map(f => (f.d_content_id, f));

        val groupSummaries = workflowSummaryUsageInfo.cogroup(popularityInfo)
        groupSummaries.map{ x =>
          val workflowSummary = if(x._2._1.size > 0) x._2._1 else (Iterable())
          val popularityInfoSummary = if(x._2._2.size > 0) x._2._2 else (Iterable())
          PopularityUpdaterInput(x._1, if(workflowSummary.size > 0) Option(workflowSummary.head) else None, if (popularityInfoSummary.size > 0) Option(popularityInfoSummary.head) else None)

        }

    }

    override def algorithm(data: RDD[PopularityUpdaterInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GraphUpdateEvent] = {
        data.map { x =>
            val contentType = x.workflowSummary.map(_.m_content_type).getOrElse("Content")
            val objectType = getObjectType(contentType, config)
            val workflowUsageMap = x.workflowSummary.map(f =>
                Map("me_totalSessionsCount" -> f.m_total_sessions,
                    "me_totalTimespent" -> f.m_total_ts,
                    "me_totalInteractions" -> f.m_total_interactions,
                    "me_averageInteractionsPerMin" -> f.m_avg_interactions_min,
                    "me_averageTimespentPerSession" -> f.m_avg_ts_session)
            ).getOrElse(Map("" -> 0.0))

            val popularityMap = x.popularitySummary.map(f =>
                Map("me_averageRating" -> f.m_avg_rating,
                    "me_totalDownloads" -> f.m_downloads,
                    "me_totalSideloads" -> f.m_side_loads,
                    "me_totalRatings" -> f.m_ratings.size,
                    "me_totalComments" -> f.m_comments.size)
            ).getOrElse(Map("" -> 0.0))
            val metrics = workflowUsageMap ++ popularityMap
            val finalContentMap = metrics.filter(x=> x._1.nonEmpty)
              .map{ x => x._1 -> Map("ov" -> null, "nv" -> x._2) }
            GraphUpdateEvent(DateTime.now().getMillis, x.contentId,
                Map("properties" -> finalContentMap), objectType.getOrElse("Content"))
        }.cache()
    }

    override def postProcess(data: RDD[GraphUpdateEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GraphUpdateEvent] = {
        data
    }

    def getObjectType(contentType : String, config : Map[String, AnyRef]) : String = {
        val contentTypeMap = config.getOrElse("contentTypes", Map("Content" -> List("Resource",
        "Collection",
        "TextBook",
        "LessonPlan",
        "Course",
        "Template",
        "Asset",
        "Plugin",
        "LessonPlanUnit",
        "CourseUnit",
        "TextBookUnit"))).asInstanceOf[Map[String, List[String]]]
        val objectType = contentTypeMap.filter{ x =>
            x._2.exists { _.equalsIgnoreCase(contentType)
          }
        }
        objectType.headOption.map(_._1).getOrElse(contentType)
    }
}