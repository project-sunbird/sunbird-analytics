package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.Params
import org.ekstep.analytics.api.ContentSummary
import org.ekstep.analytics.api.Trend
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.api.ContentUsageSummary
import org.joda.time.Weeks
import org.joda.time.DateTime
import org.ekstep.analytics.api.ContentUsageSummary
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.api.Period._

/**
 * @author Santhosh
 */

object ContentAPIService {

    @transient val dayPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd");
    @transient val monthPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM");
    @transient val df: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withZoneUTC();
    private val CONTENT_DB = "content_db";
    private val CONTENT_SUMMARY_FACT_TABLE = "content_usage_summary_fact";

    def getContentUsageMetrics(contentId: String, requestBody: String)(implicit sc: SparkContext): String = {
        val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
        JSONUtils.serialize(contentUsageMetrics(contentId, reqBody));
    }

    private def contentUsageMetrics(contentId: String, reqBody: RequestBody)(implicit sc: SparkContext): Response = {
        // Initialize to default values if not found from the request.
        val reqTrend: Trend = reqBody.request.trend.getOrElse(Trend(Option(7), Option(5), Option(12)));
        val trend = Map[String, (Period, Int)]("day" -> (DAY, reqTrend.day.getOrElse(0)), "week" -> (WEEK, reqTrend.week.getOrElse(0)), "month" -> (MONTH, reqTrend.month.getOrElse(0)));
        val reqSummaries = reqBody.request.summaries.getOrElse(Array[String]("day", "week", "month", "cumulative"));
        val summaryMap = reqSummaries.map { x =>
            x match {
                case "day"        => (x, DAY)
                case "week"       => (x, WEEK)
                case "month"      => (x, MONTH)
                case "cumulative" => (x, CUMULATIVE)
            }
        }.toMap

        val contentRDD = sc.cassandraTable[ContentUsageSummary](CONTENT_DB, CONTENT_SUMMARY_FACT_TABLE).where("d_content_id = ?", contentId).cache();
        val trends = trend.mapValues(x =>
            x._1 match {
                case DAY   => filterTrends(contentRDD, getDayRange(x._2), reqBody.request.filter);
                case WEEK  => filterTrends(contentRDD, getWeekRange(x._2), reqBody.request.filter);
                case MONTH => filterTrends(contentRDD, getMonthRange(x._2), reqBody.request.filter);
            });

        val summaries = summaryMap.mapValues { x =>
            x match {
                case DAY   => reduceTrends(trends.get("day").getOrElse(Array[ContentSummary]()));
                case WEEK  => reduceTrends(trends.get("week").getOrElse(Array[ContentSummary]()));
                case MONTH => reduceTrends(trends.get("month").getOrElse(Array[ContentSummary]()));
                case CUMULATIVE => filterTrends(contentRDD, Range(-1, 0), reqBody.request.filter)
            }
        }
        contentRDD.unpersist(false);

        val result = Map[String, AnyRef](
            "ttl" -> 0.0.asInstanceOf[AnyRef],
            "summaries" -> summaries,
            "trend" -> trends);
        Response("ekstep.analytics.contentusagesummary", "1.0", df.print(System.currentTimeMillis()), Params("054f3b10-309f-4552-ae11-02c66640967b", null, null, "successful", null), result);
    }

    private def filterTrends(contentRDD: RDD[ContentUsageSummary], periodRange: Range, filter: Option[Filter]): Array[ContentSummary] = {
        val trends = contentRDD.filter { x => x.d_period > periodRange.start && x.d_period <= periodRange.end }
            .map { x =>
                ContentSummary(Option(x.d_period), x.m_total_ts, x.m_total_sessions, x.m_avg_ts_session, x.m_total_interactions, x.m_avg_interactions_min, Option(x.m_avg_sessions_week), Option(x.m_avg_ts_week))
            }
        val groupByPeriod = trends.groupBy { x => x.period.get }.map(f => reduceTrends(f._2.toArray).get);
        groupByPeriod.collect();
    }

    private def reduceTrends(summaries: Array[ContentSummary]): Option[ContentSummary] = {
        if (summaries.size > 0)
            Option(summaries.reduce((a, b) => {
                val total_ts = a.total_ts + b.total_ts;
                val total_sessions = a.total_sessions + b.total_sessions;
                val total_interactions = a.total_interactions + b.total_interactions;
                val avg_ts_session = total_ts / total_sessions;
                val avg_interactions_min = total_interactions / (total_ts / 60);
                ContentSummary(None, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, None, None)
            }))
        else None;
    }

    private def getDayRange(count: Int): Range = {
        val endDate = DateTime.now();
        val startDate = endDate.minusDays(count);
        Range(dayPeriod.print(startDate).toInt, dayPeriod.print(endDate).toInt)
    }

    private def getMonthRange(count: Int): Range = {
        val endDate = DateTime.now();
        val startDate = endDate.minusDays(count * 30);
        Range(monthPeriod.print(startDate).toInt, monthPeriod.print(endDate).toInt)
    }

    private def getWeekRange(count: Int): Range = {
        val endDate = DateTime.now();
        val startDate = endDate.minusDays(count * 7);
        Range((startDate.getWeekyear + "77" + startDate.getWeekOfWeekyear).toInt, (endDate.getWeekyear + "77" + endDate.getWeekOfWeekyear).toInt)
    }

}