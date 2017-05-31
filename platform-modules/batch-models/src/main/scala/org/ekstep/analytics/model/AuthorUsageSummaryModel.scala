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

case class AuthorUsageSummary(ak: AuthorKey, total_session: Long, total_ts: Double, ce_total_ts: Double, total_ce_visit: Long, percent_ce_sessions: Double, avg_session_ts: Double, percent_ce_ts: Double, dt_range: DtRange, syncts: Long) extends AlgoOutput
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
        
        val date_range = DtRange(firstEvent.dt_range.from, lastEvent.dt_range.to);
        val total_session = events.size
        val total_ts = CommonUtil.roundDouble(events.map { x => x.total_ts }.sum, 2)
        val avg_session_ts = CommonUtil.roundDouble(total_ts / total_session, 2)
        
        val total_ce_visit = events.map { x => x.total_ce_visit }.sum
        val ce_total_ts = CommonUtil.roundDouble(events.map { x => x.ce_total_ts }.sum, 2)
        
        val ce_percent_sessions = CommonUtil.roundDouble((total_ce_visit * 1.0 / total_session) * 100, 2)
        val ce_percent_ts = CommonUtil.roundDouble(if (total_ts > 0.0) ((ce_total_ts / total_ts) * 100) else (0d), 2)
        AuthorUsageSummary(ak, total_session, total_ts, ce_total_ts, total_ce_visit, ce_percent_sessions, avg_session_ts, ce_percent_ts, date_range, lastEvent.syncts)
    }
    
    def getAuthorUsageSummary(event: DerivedEvent, period: Int, authorId: String): AuthorUsageSummary = {
        val ak = AuthorKey(period, authorId)
        val total_session = 1
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val total_ts = eksMap.getOrElse("time_spent", 0.0).asInstanceOf[Double]
        val avg_session_ts = total_ts
        
        val ce_visits = eksMap.getOrElse("ce_visits", 0L).asInstanceOf[Number].longValue()
        val ce_total_ts = eksMap.get("env_summary").get.asInstanceOf[List[Map[String, AnyRef]]]
            .filter { x => (x.getOrElse("env", "").equals("content-editor")) }
            .map { x => x.getOrElse("time_spent", 0.0).asInstanceOf[Double] }.sum
        
        val ce_percent_sessions = (ce_visits * 1.0 / total_session) * 100
        val ce_percent_ts = if (total_ts > 0.0) ((ce_total_ts / total_ts) * 100) else (0d)
        
        AuthorUsageSummary(ak, total_session, total_ts, ce_total_ts, ce_visits, ce_percent_sessions, avg_session_ts, ce_percent_ts, event.context.date_range, event.syncts)
    }
    /**
     *  Input Portal session summary
     */
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorUsageInput] = {

        val filteredEvents = data.filter { x => (!x.uid.equals("")) }
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
                "total_session" -> summary.total_session,
                "total_ts" -> summary.total_ts,
                "ce_total_ts" -> summary.ce_total_ts,
                "ce_total_visits" -> summary.total_ce_visit,
                "ce_percent_sessions" -> summary.percent_ce_sessions,
                "avg_session_ts" -> summary.avg_session_ts,
                "ce_percent_ts" -> summary.percent_ce_ts);
            val pdata = PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AuthorUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]);
            MeasuredEvent("ME_AUTHOR_USAGE_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, summary.ak.author, None, None,
                Context(pdata, None, "DAY", summary.dt_range),
                Dimensions(None, None, None, None, None, None, None, None, None, None, Option(summary.ak.period), None, None, None, None, None), MEEdata(measures), None);
        };
    }
}