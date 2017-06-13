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
import scala.collection.mutable.ListBuffer

/**
 * @author yuva/Amit
 */

case class AuthorUsageSummary(ak: AuthorKey, total_session: Long, total_ts: Double, ce_total_ts: Double, total_ce_visit: Long, ce_visits_count: Long, percent_ce_sessions: Double, avg_session_ts: Double, percent_ce_ts: Double, dt_range: DtRange, syncts: Long) extends AlgoOutput
case class AuthorKey(period: Int, author: String)
case class AuthorUsageInput(ck: AuthorKey, events: Buffer[AuthorUsageSummary]) extends AlgoInput

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

object AuthorUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, AuthorUsageInput, AuthorUsageSummary, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.AuthorUsageSummaryModel"
    override def name(): String = "AuthorUsageSummaryModel";

    private def _computeMetrics(events: Buffer[AuthorUsageSummary], ck: AuthorKey): AuthorUsageSummary = {
        val firstEvent = events.sortBy { x => x.dt_range.from }.head;
        val lastEvent = events.sortBy { x => x.dt_range.to }.last;
        val ak = firstEvent.ak;
        
        val dateRange = DtRange(firstEvent.dt_range.from, lastEvent.dt_range.to);
        val totalSessions = events.size
        val totalTS = CommonUtil.roundDouble(events.map { x => x.total_ts }.sum, 2)
        val avgSessionTS = CommonUtil.roundDouble(totalTS / totalSessions, 2)
        
        val totalCEVisits = events.map { x => x.total_ce_visit }.sum
        val ceTotalTS = CommonUtil.roundDouble(events.map { x => x.ce_total_ts }.sum, 2)
        
        val ceVisitsOccurrence = events.map { x => if(x.total_ce_visit >0 ) 1 else 0 }.sum
        
        val cePercentSessions = CommonUtil.roundDouble((ceVisitsOccurrence * 1.0 / totalSessions) * 100, 2)
        val cePercentTS = CommonUtil.roundDouble(if (totalTS > 0.0) ((ceTotalTS / totalTS) * 100) else (0d), 2)
        AuthorUsageSummary(ak, totalSessions, totalTS, ceTotalTS, totalCEVisits, ceVisitsOccurrence, cePercentSessions, avgSessionTS, cePercentTS, dateRange, lastEvent.syncts)
    }
    
    def getAuthorUsageSummary(event: DerivedEvent, period: Int, authorId: String): AuthorUsageSummary = {
        val ak = AuthorKey(period, authorId)
        val totalSessions = 1
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val totalTS = eksMap.getOrElse("time_spent", 0.0).asInstanceOf[Double]
        val avgSessionTS = totalTS
        
        val ceVisits = eksMap.getOrElse("ce_visits", 0L).asInstanceOf[Number].longValue()
        val ceVisitOccurrence = if(ceVisits > 0) 1 else 0;
        val ceTotalTS = eksMap.get("env_summary").get.asInstanceOf[List[Map[String, AnyRef]]]
            .filter { x => (x.getOrElse("env", "").equals("content-editor")) }
            .map { x => x.getOrElse("time_spent", 0.0).asInstanceOf[Double] }.sum
        
        val cePercentSessions = (ceVisits * 1.0 / totalSessions) * 100
        val cePercentTS = if (totalTS > 0.0) ((ceTotalTS / totalTS) * 100) else (0d)
        
        AuthorUsageSummary(ak, totalSessions, totalTS, ceTotalTS, ceVisits, ceVisitOccurrence, cePercentSessions, avgSessionTS, cePercentTS, event.context.date_range, event.syncts)
    }
    /**
     *  Input Portal session summary
     */
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorUsageInput] = {

        val filteredEvents = data.filter { x => (false == x.dimensions.anonymous_user.get) }
        val normalizeEvents = filteredEvents.map { event =>

            var list: ListBuffer[AuthorUsageSummary] = ListBuffer[AuthorUsageSummary]();
            val period = CommonUtil.getPeriod(event.context.date_range.to, Period.DAY);
            // For all
            list += getAuthorUsageSummary(event, period, "all");
            list += getAuthorUsageSummary(event, period, event.uid);
            list.toArray;
        }.flatMap { x => x.map { x => x } };

        normalizeEvents.map { x => (x.ak, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => AuthorUsageInput(x._1, x._2) };
    }

    override def algorithm(data: RDD[AuthorUsageInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorUsageSummary] = {

        data.map { x =>
            _computeMetrics(x.events, x.ck);
        }.filter { x => x.total_ce_visit > 0 }
    }

    override def postProcess(data: RDD[AuthorUsageSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_AUTHOR_USAGE_SUMMARY",summary.ak.author, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dt_range);
            val measures = Map(
                "total_sessions" -> summary.total_session,
                "total_ts" -> summary.total_ts,
                "ce_total_ts" -> summary.ce_total_ts,
                "ce_total_visits" -> summary.total_ce_visit,
                "ce_visits_count" -> summary.ce_visits_count,
                "ce_percent_sessions" -> summary.percent_ce_sessions,
                "avg_ts_session" -> summary.avg_session_ts,
                "ce_percent_ts" -> summary.percent_ce_ts);
            val pdata = PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AuthorUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]);
            MeasuredEvent("ME_AUTHOR_USAGE_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, summary.ak.author, None, None,
                Context(pdata, None, "DAY", summary.dt_range),
                Dimensions(None, None, None, None, None, None, None, None, None, None, Option(summary.ak.period), None, None, None, None, None), MEEdata(measures), None);
        };
    }
}