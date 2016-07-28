package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.Params
import org.ekstep.analytics.api.ContentSummary
import org.ekstep.analytics.api.Trend
import org.ekstep.analytics.api.Range
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.joda.time.Weeks
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.api.Period._
import org.ekstep.analytics.api.ContentUsageSummaryFact
import org.ekstep.analytics.api.util.CommonUtil
import org.joda.time.DateTimeZone
import org.ekstep.analytics.api.ContentSummary
import java.util.UUID
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.ContentToVector
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.DtRange
import sys.process._
import org.ekstep.analytics.framework.util.S3Util
import java.io.File
import org.ekstep.analytics.streaming.KafkaEventProducer
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher

/**
 * @author Santhosh
 */

object ContentAPIService {

    def contentToVec(contentId: String)(implicit sc: SparkContext, config: Map[String, String]): String = {

        try {
            val baseUrl = config.get("base.url").get;
            val scriptLoc = config.get("python.scripts.loc").get;
            val contentArr = Array(s"$baseUrl/learning/v2/content/$contentId")
            val enrichedJson = sc.makeRDD(contentArr).pipe(s"python $scriptLoc/content/enrich_content.py").cache

            val corpus = enrichedJson.pipe(s"python $scriptLoc/object2vec/update_content_corpus.py");

            val bucket = config.get("s3.bucket").get
            val modelPath = config.get("model.file.path").get
            val prefix = config.get("prefix").get

            if ("true".equals(config.get("train.model").get)) {
                //Training ....  
                ScriptDispatcher.dispatch(Array(), Map("script" -> s"python $scriptLoc/object2vec/corpus_to_vec.py", "corpus_loc" -> config.get("corpus.loc").get, "model" -> modelPath))
                //upload model file
                uploadModel(bucket, prefix, modelPath)
            }
            //download model
            S3Util.download(bucket, prefix, modelPath)
            val vectorRDD = corpus.map { x =>
                JSONUtils.serialize(Map("contentId" -> contentId, "document" -> x, "infer_all" -> config.get("infer.all").get, "corpus_loc" -> config.get("corpus.loc").get, "model" -> modelPath));
            }//.pipe(s"python $scriptLoc/object2vec/infer_query.py")
            vectorRDD.collect.last;
            //vectorRDD.map { x => JSONUtils.deserialize[ContentToVector](x);}.saveToCassandra(Constants.CONTENT_DB, Constants.CONTENT_TO_VEC);

            //val enrichedJsonMap = enrichedJson.map { x => JSONUtils.deserialize[Map[String, AnyRef]](x) }.collect.last
            //val me = JSONUtils.serialize(getME(enrichedJsonMap, contentId))
            //KafkaEventProducer.sendEvents(Array(me), config.get("topic").get, config.get("broker.list").get)
            //me;
        } catch {
            case ex: Exception =>
                JSONUtils.serialize(Map("status" -> "FAILED", "Message" -> ex.getMessage, "stackTrace" -> ex.printStackTrace()));
        }
    }

    def getContentUsageMetrics(contentId: String, requestBody: String)(implicit sc: SparkContext): String = {
        val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
        JSONUtils.serialize(contentUsageMetrics(contentId, reqBody));
    }

    private def contentUsageMetrics(contentId: String, reqBody: RequestBody)(implicit sc: SparkContext): Response = {
        // Initialize to default values if not found from the request.
        if (reqBody.request == null) {
            throw new Exception("Request cannot be blank");
        }
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

        val contentRDD = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_DB, Constants.CONTENT_SUMMARY_FACT_TABLE).where("d_content_id = ?", contentId).cache();
        val trends = trend.mapValues(x =>
            x._1 match {
                case DAY   => filterTrends(contentRDD, CommonUtil.getDayRange(x._2), DAY, reqBody.request.filter);
                case WEEK  => filterTrends(contentRDD, CommonUtil.getWeekRange(x._2), WEEK, reqBody.request.filter);
                case MONTH => filterTrends(contentRDD, CommonUtil.getMonthRange(x._2), MONTH, reqBody.request.filter);
            });

        val summaries = summaryMap.mapValues[Option[ContentUsageSummaryFact]] { x =>
            x match {
                case DAY        => reduceTrends(trends.get("day").get, DAY);
                case WEEK       => reduceTrends(trends.get("week").get, WEEK);
                case MONTH      => reduceTrends(trends.get("month").get, MONTH);
                case CUMULATIVE => reduceTrends(filterTrends(contentRDD, Range(-1, 0), CUMULATIVE, reqBody.request.filter), CUMULATIVE)
            }
        }
        contentRDD.unpersist(false);

        val result = Map[String, AnyRef](
            "ttl" -> CommonUtil.getRemainingHours.asInstanceOf[AnyRef],
            "trend" -> trends.mapValues(_.map(transform)),
            "summaries" -> summaries.mapValues(transform));
        CommonUtil.OK("ekstep.analytics.contentusagesummary", result);
    }

    private def transform(fact: ContentUsageSummaryFact): ContentSummary = {
        ContentSummary(Option(fact.d_period), fact.m_total_ts, fact.m_total_sessions, fact.m_avg_ts_session, fact.m_total_interactions, fact.m_avg_interactions_min,
            fact.m_avg_sessions_week, fact.m_avg_ts_week)
    }

    private def transform(fact: Option[ContentUsageSummaryFact]): Option[ContentSummary] = {
        if (fact.isDefined)
            Option(ContentSummary(None, fact.get.m_total_ts, fact.get.m_total_sessions, fact.get.m_avg_ts_session, fact.get.m_total_interactions,
                fact.get.m_avg_interactions_min, fact.get.m_avg_sessions_week, fact.get.m_avg_ts_week))
        else None;
    }

    private def filterTrends(contentRDD: RDD[ContentUsageSummaryFact], periodRange: Range, period: Period, filter: Option[Filter]): Array[ContentUsageSummaryFact] = {
        val trends = contentRDD.filter { x => x.d_period > periodRange.start && x.d_period <= periodRange.end };
        val filteredByDimensions = if (filter.isDefined) trends.filter { x =>
            if (filter.get.group_user.isDefined) {
                x.d_group_user == filter.get.group_user.get
            } else {
                true
            }
        }
        else trends;
        filteredByDimensions.groupBy { x => x.d_period }
            .map(f => reduceTrends(f._2.toArray, period).get)
            .sortBy(f => f.d_period, true, 10)
            .collect();
    }

    private def reduceTrends(summaries: Array[ContentUsageSummaryFact], period: Period): Option[ContentUsageSummaryFact] = {
        if (summaries.size > 0)
            Option(summaries.reduceRight((a, b) => reduce(a, b, period)))
        else None;
    }

    /**
     * Reducer to rollup two summaries
     */
    private def reduce(fact1: ContentUsageSummaryFact, fact2: ContentUsageSummaryFact, period: Period): ContentUsageSummaryFact = {

        val total_ts = fact2.m_total_ts + fact1.m_total_ts
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_ts_session = (total_ts) / (total_sessions)
        val total_interactions = fact2.m_total_interactions + fact1.m_total_interactions
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val publish_date = if (fact2.m_publish_date.isBefore(fact1.m_publish_date)) fact2.m_publish_date else fact1.m_publish_date;
        val sync_date = if (fact2.m_last_sync_date.isAfter(fact1.m_last_sync_date)) fact2.m_last_sync_date else fact1.m_last_sync_date;
        val numWeeks = CommonUtil.getWeeksBetween(publish_date.getMillis, sync_date.getMillis)
        val avg_sessions_week = period match {
            case MONTH | CUMULATIVE => Option(if (numWeeks != 0) (total_sessions.toDouble) / numWeeks else total_sessions)
            case _                  => None
        }
        val avg_ts_week = period match {
            case MONTH | CUMULATIVE => Option(if (numWeeks != 0) (total_ts) / numWeeks else total_ts)
            case _                  => None
        }
        ContentUsageSummaryFact(fact1.d_content_id, fact1.d_period, fact1.d_group_user, fact1.d_content_type, fact1.d_mime_type, publish_date, sync_date,
            total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, avg_sessions_week, avg_ts_week);
    }

    private def getME(data: Map[String, AnyRef], contentId: String): MeasuredEvent = {
        val ts = System.currentTimeMillis()
        val dateRange = DtRange(ts, ts)
        val mid = org.ekstep.analytics.framework.util.CommonUtil.getMessageId("AN_ENRICHED_CONTENT", null, null, dateRange, contentId);
        MeasuredEvent("AN_ENRICHED_CONTENT", ts, ts, "1.0", mid, null, Option(contentId), None, Context(PData("AnalyticsDataPipeline", "ContentToVec", "1.0"), None, null, dateRange), null, MEEdata(Map("enrichedJson" -> data)));
    }

    private def uploadModel(bucket: String, prefix: String, modelPath: String) {

        val d = new File(modelPath)
        val files = if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList;
        } else {
            List[File]();
        }
        for (f <- files) {
            val key = prefix + f.getName.split("/").last
            S3Util.upload(bucket, f.getAbsolutePath, key)
        }
    }
}