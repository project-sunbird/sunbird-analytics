package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.Period._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.SessionBatchModel
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.apache.commons.lang3.StringUtils

/**
 * @author yuva/Amit
 */
case class AuthorMetrics(period: Int, uid: String, total_session: Long, total_time: Double, total_ce_time: Double, total_ce_visit: Long, percent_ce_sessions: Double, avg_session_ts: Double, percent_ce_ts: Double, dt_range: DtRange, syncts: Long) extends AlgoOutput
case class AuthorEvents(uid: String, events: Buffer[DerivedEvent]) extends AlgoInput
case class PortalSessionMetrics(time_spent: Double, ce_visits: Long, env_summary: List[Map[String, AnyRef]])

/**
 * @dataproduct
 * @Summarizer
 *
 * AuthorUsageSummaryModel
 *
 * Functionality
 * 1. Generate Author usage summary events per day. This would be used to compute total sessions,total time spent, avg session time stamp etc.. per author based on day.
 * Event used - ME_AUTHOR_USAGE_SUMMARY
 */

object AuthorUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, AuthorEvents, AuthorMetrics, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.AuthorUsageSummaryModel"
    override def name(): String = "AuthorUsageSummaryModel";

    /**
     *  Input Portal session summary
     */
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorEvents] = {

        data.filter { x =>
            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val ce_visits = eksMap.getOrElse("ce_visits", 0L).asInstanceOf[Number].longValue()
            ce_visits > 0
        }.map { x => (x.uid, Buffer(x)) }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => AuthorEvents(x._1, x._2) }
    }

    override def algorithm(data: RDD[AuthorEvents], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorMetrics] = {

        data.map { x =>
            val author = x.uid
            val periodEvents = x.events.map { x => (CommonUtil.getPeriod(x.syncts, DAY), x) }.groupBy { x => x._1 }
            periodEvents.map { x =>
                val period = x._1
                val events = x._2.map(f => f._2)
                val totalSessions = events.length

                val metrics = events.map { x =>
                    val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
                    val timeSpent = eksMap.getOrElse("time_spent", 0.0).asInstanceOf[Number].doubleValue()
                    val ce_visits = eksMap.getOrElse("ce_visits", 0L).asInstanceOf[Number].longValue()
                    val envSumm = eksMap.get("env_summary").get.asInstanceOf[List[Map[String, AnyRef]]].filter { x => (x.getOrElse("env", "").equals("content-editor")) }
                    PortalSessionMetrics(timeSpent, ce_visits, envSumm)
                }

                val totalTS = metrics.map { x => x.time_spent }.sum
                val ceTotalTS = metrics.map { x => x.env_summary.map { x => x.getOrElse("time_spent", 0.0).asInstanceOf[Number].doubleValue() } }.flatMap { x => x }.sum
                val ceTotalVisits = metrics.map { x => x.ce_visits }.sum
                val cePercentSession = (ceTotalVisits * 1.0 / totalSessions) * 100
                val avgSessionTS = totalTS / totalSessions
                val cePercentTS = (ceTotalTS / totalTS) * 100

                val sortedEvents = events.sortBy { x => x.syncts }
                val date_range = DtRange(sortedEvents.head.syncts, sortedEvents.last.syncts)
                val syncts = sortedEvents.last.syncts

                AuthorMetrics(period, author, totalSessions, totalTS, ceTotalTS, ceTotalVisits, cePercentSession, avgSessionTS, cePercentTS, date_range, syncts)
            }
        }.flatMap { x => x }
    }

    override def postProcess(data: RDD[AuthorMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_AUTHOR_USAGE_SUMMARY", summary.period.toString() + summary.uid, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dt_range.to);
            val measures = Map(
                "total_session" -> summary.total_session,
                "total_ts" -> summary.total_time,
                "ce_total_ts" -> summary.total_ce_time,
                "ce_total_visits" -> summary.total_ce_visit,
                "ce_percent_sessions" -> summary.percent_ce_sessions,
                "avg_session_ts" -> summary.avg_session_ts,
                "ce_percent_ts" -> summary.percent_ce_ts);
            val pdata = PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AuthorUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]);
            MeasuredEvent("ME_AUTHOR_USAGE_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, summary.uid, None, None,
                Context(pdata, None, "DAY", summary.dt_range),
                Dimensions(None, None, None, None, None, None, None, None, None, None, Option(summary.period), None, None, None, None, None), MEEdata(measures), None);
        };
    }
}