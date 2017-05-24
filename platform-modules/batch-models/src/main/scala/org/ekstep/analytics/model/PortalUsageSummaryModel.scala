/**
 * @author Sowmya Dixit
 */
package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Period
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework._

/**
 * Case Classes for the data product
 */
case class PortalUsageInput(period: Int, sessionEvents: Buffer[DerivedEvent]) extends AlgoInput
case class PortalUsageOutput(period: Int, author_id: String, app_id: String, dtRange: DtRange, anonymous_total_sessions: Long, anonymous_total_ts: Double,
                             total_sessions: Long, total_ts: Double, ce_total_sessions: Long, ce_percent_sessions: Double,
                             total_pageviews_count: Long, unique_users: List[String], unique_users_count: Long, avg_pageviews: Double,
                             avg_session_ts: Double, anonymous_avg_session_ts: Double, new_user_count: Long,
                             percent_new_users_count: Double) extends AlgoOutput

/**
 * @dataproduct
 * @Summarizer
 *
 * PortalUsageSummaryModel
 *
 * Functionality
 * 1. Generate portal usage summary events per day. This would be used to compute portal usage weekly, monthly & cumulative metrics.
 * Event used - ME_APP_SESSION_SUMMARY
 */
object PortalUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, PortalUsageInput, PortalUsageOutput, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.PortalUsageSummaryModel"
    override def name: String = "PortalUsageSummaryModel"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PortalUsageInput] = {
        val sessionEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_APP_SESSION_SUMMARY")));
        sessionEvents.map { f =>
            val period = CommonUtil.getPeriod(f.context.date_range.to, Period.DAY);
            (period, Buffer(f))
        }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => PortalUsageInput(x._1, x._2) };
    }

    override def algorithm(data: RDD[PortalUsageInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PortalUsageOutput] = {

        val authorSpecificUsage = data.map { event =>
            val filteredSessions = event.sessionEvents.filter { x => false == x.dimensions.anonymous_user.get }
            filteredSessions.groupBy { x => x.uid }.map { f =>
                val firstEvent = f._2.sortBy { x => x.context.date_range.from }.head
                val lastEvent = f._2.sortBy { x => x.context.date_range.to }.last
                val date_range = DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to);
                val appId = firstEvent.dimensions.app_id.getOrElse("EkstepPortal")
                val eksMapList = f._2.map { x =>
                    x.edata.eks.asInstanceOf[Map[String, AnyRef]]
                }
                val totalSessions = f._2.length.toLong
                val totalTS = eksMapList.map { x =>
                    x.get("time_spent").get.asInstanceOf[Double]
                }.sum
                val ceTotalSessions = eksMapList.map { x =>
                    x.get("ce_visits").get.asInstanceOf[Number].longValue()
                }.filter { x => x > 0 }.length.toLong
                val cePercentSessions = if (ceTotalSessions == 0 || totalSessions == 0) 0d else BigDecimal((ceTotalSessions / (totalSessions * 1d)) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
                val avgSessionTS = if (totalTS == 0 || totalSessions == 0) 0d else BigDecimal(totalTS / totalSessions).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;

                PortalUsageOutput(event.period, f._1, appId, date_range, 0L, 0.0, totalSessions, totalTS, ceTotalSessions, cePercentSessions, 0L, List(), 0L, 0.0, avgSessionTS, 0.0, 0, 0.0)
            }
        }.flatMap { x => x }

        data.map { f =>
            val firstEvent = f.sessionEvents.sortBy { x => x.context.date_range.from }.head
            val lastEvent = f.sessionEvents.sortBy { x => x.context.date_range.to }.last
            val date_range = DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to);
            val appId = firstEvent.dimensions.app_id.getOrElse("EkstepPortal")
            val eksMapList = f.sessionEvents.map { x =>
                x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            }
            val anonymousSessions = f.sessionEvents.filter { x => true == x.dimensions.anonymous_user.get }
            val anonymousTotalSessions = if (anonymousSessions.length > 0) anonymousSessions.length.toLong else 0L
            val anonymousTotalTS = if (anonymousSessions.length > 0) anonymousSessions.map { x =>
                val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
                eksMap.get("time_spent").get.asInstanceOf[Double]
            }.sum
            else 0.0
            val totalSessions = f.sessionEvents.length.toLong
            val totalTS = eksMapList.map { x =>
                x.get("time_spent").get.asInstanceOf[Double]
            }.sum
            val ceTotalSessions = eksMapList.map { x =>
                x.get("ce_visits").get.asInstanceOf[Number].longValue()
            }.filter { x => x > 0 }.length.toLong
            val cePercentSessions = if (ceTotalSessions == 0 || totalSessions == 0) 0d else BigDecimal((ceTotalSessions / (totalSessions * 1d)) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            val totalPageviewsCount = eksMapList.map { x =>
                x.get("page_views_count").get.asInstanceOf[Number].longValue()
            }.sum
            val uniqueUsers = f.sessionEvents.map(x => x.uid).distinct.filterNot { x => x.isEmpty() }.toList
            val uniqueUsersCount = uniqueUsers.length.toLong
            val avgPageviews = if (totalPageviewsCount == 0 || totalSessions == 0) 0d else BigDecimal(totalPageviewsCount / (totalSessions * 1d)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            val avgSessionTS = if (totalTS == 0 || totalSessions == 0) 0d else BigDecimal(totalTS / totalSessions).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            val anonymousAvgSessionTS = if (anonymousTotalTS == 0 || anonymousTotalSessions == 0) 0d else BigDecimal(anonymousTotalTS / anonymousTotalSessions).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            val newUserCount = f.sessionEvents.map { x =>
                val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
                (eksMap.get("first_visit").get.asInstanceOf[Boolean], x.uid)
            }.filter(f => f._1 == (true)).length.toLong
            val percentNewUsersCount = if (newUserCount == 0 || uniqueUsersCount == 0) 0d else BigDecimal((newUserCount / (uniqueUsersCount * 1d)) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;

            PortalUsageOutput(f.period, "all", appId, date_range, anonymousTotalSessions, anonymousTotalTS, totalSessions, totalTS, ceTotalSessions, cePercentSessions, totalPageviewsCount, uniqueUsers, uniqueUsersCount, avgPageviews, avgSessionTS, anonymousAvgSessionTS, newUserCount, percentNewUsersCount)
        } ++ authorSpecificUsage;
    }

    override def postProcess(data: RDD[PortalUsageOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { usageSumm =>
            val mid = CommonUtil.getMessageId("ME_APP_USAGE_SUMMARY", usageSumm.author_id, "DAY", usageSumm.dtRange);
            val measures = Map(
                "anonymous_total_sessions" -> usageSumm.anonymous_total_sessions,
                "anonymous_total_ts" -> usageSumm.anonymous_total_ts,
                "total_sessions" -> usageSumm.total_sessions,
                "total_ts" -> usageSumm.total_ts,
                "ce_total_sessions" -> usageSumm.ce_total_sessions,
                "ce_percent_sessions" -> usageSumm.ce_percent_sessions,
                "total_pageviews_count" -> usageSumm.total_pageviews_count,
                "unique_users" -> usageSumm.unique_users,
                "unique_users_count" -> usageSumm.unique_users_count,
                "avg_pageviews" -> usageSumm.avg_pageviews,
                "avg_session_ts" -> usageSumm.avg_session_ts,
                "anonymous_avg_session_ts" -> usageSumm.anonymous_avg_session_ts,
                "new_user_count" -> usageSumm.new_user_count,
                "percent_new_users_count" -> usageSumm.percent_new_users_count);
            MeasuredEvent("ME_APP_USAGE_SUMMARY", System.currentTimeMillis(), usageSumm.dtRange.to, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AppUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", usageSumm.dtRange),
                Dimensions(None, None, None, None, None, None, None, None, None, None, Option(usageSumm.period), None, None, None, None, None, None, None, None, None, Option(usageSumm.author_id), None, None, Option(usageSumm.app_id)),
                MEEdata(measures), None);
        }
    }
}