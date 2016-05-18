package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import scala.collection.mutable.HashMap
import java.text.SimpleDateFormat
import java.util.Date
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.LocalDate
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.apache.spark.HashPartitioner

case class ContentSummary(content_id: String, start_date: DateTime, total_num_sessions: Long, total_ts: Double, average_ts_session: Double,
                          total_interactions: Long, average_interactions_min: Double, num_sessions_week: Double, ts_week: Double, content_type: String, mime_type: String)

object ContentActivitySummary extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        println("### Running the model ContentSummary ###");
        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val newEvents = filteredEvents.map(event => (event.dimensions.gdata.get.id, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b);
        val prevContentState = newEvents.map(f => ContentId(f._1)).joinWithCassandraTable[ContentSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE).map(f => (f._1.content_id, f._2))
        val contentData = newEvents.leftOuterJoin(prevContentState);

        val contentSummary = contentData.mapValues { events =>

            val sortedEvents = events._1.sortBy { x => x.syncts };
            val firstEvent = sortedEvents.head;
            val lastEvent = sortedEvents.last;
            val eventSyncts = lastEvent.syncts;
            val gameId = firstEvent.dimensions.gdata.get.id;
            val gameVersion = firstEvent.dimensions.gdata.get.ver;

            val firstGE = events._1.sortBy { x => x.context.date_range.from }.head;
            val lastGE = events._1.sortBy { x => x.context.date_range.to }.last;
            val eventStartTimestamp = firstGE.context.date_range.from;
            val eventEndTimestamp = lastGE.context.date_range.to;
            val eventStartDate = new DateTime(eventStartTimestamp);
            val date_range = DtRange(firstEvent.syncts, lastEvent.syncts);

            val prevContentSummary = events._2.getOrElse(ContentSummary(gameId, DateTime.now(), 0L, 0.0, 0.0, 0L, 0.0, 0L, 0.0, "", ""));
            val startDate = if (eventStartDate.isBefore(prevContentSummary.start_date)) eventStartDate else prevContentSummary.start_date;
            val numSessions = sortedEvents.size + prevContentSummary.total_num_sessions;
            val timeSpent = sortedEvents.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
            }.sum + (prevContentSummary.total_ts * 3600);
            val totalInteractions = sortedEvents.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Int])
            }.sum + prevContentSummary.total_interactions;
            val numWeeks = getWeeksBetween(startDate.getMillis, eventEndTimestamp)

            val averageTsSession = (timeSpent / numSessions);
            val averageInteractionsMin = if (totalInteractions == 0 || timeSpent == 0) 0d else CommonUtil.roundDouble(BigDecimal(totalInteractions / (timeSpent / 60)).toDouble, 2);
            val numSessionsWeek: Double = if (numWeeks == 0) numSessions else numSessions / numWeeks
            val tsWeek = if (numWeeks == 0) timeSpent else timeSpent / numWeeks
            val contentType = if (prevContentSummary.content_type.isEmpty()) firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("contentType", "").asInstanceOf[String] else prevContentSummary.content_type
            val mimeType = if (prevContentSummary.mime_type.isEmpty()) firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("mimeType", "").asInstanceOf[String] else prevContentSummary.mime_type

            (ContentSummary(gameId, startDate, numSessions, CommonUtil.roundDouble(timeSpent / 3600, 2), CommonUtil.roundDouble(averageTsSession, 2), totalInteractions, CommonUtil.roundDouble(averageInteractionsMin, 2), numSessionsWeek, tsWeek, contentType, mimeType), date_range, gameVersion)
        }.cache();

        contentSummary.map(f => f._2._1).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE);

        val summaries = sc.cassandraTable[ContentSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE).filter { x => !"Collection".equals(x.content_type) };
        val count = summaries.count().intValue();
        val defaultVal = if (5 > count) count else 5;
        val topContentByTime = summaries.sortBy(f => f.total_ts, false, 1).take(config.getOrElse("topK", defaultVal).asInstanceOf[Int]);
        val topContentBySessions = summaries.sortBy(f => f.total_num_sessions, false, 1).take(config.getOrElse("topK", defaultVal).asInstanceOf[Int]);

        val rdd = sc.parallelize(Array(ContentMetrics("content", topContentByTime.map { x => (x.content_id, x.total_ts) }.toMap, topContentBySessions.map { x => (x.content_id, x.total_num_sessions) }.toMap)), 1);
        rdd.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_METRICS_TABLE);
        contentSummary.map(f => {
            getMeasuredEvent(f._2._1, configMapping.value, f._2._2, f._2._3);
        }).map { x => JSONUtils.serialize(x) };
    }

    private def getMeasuredEvent(contentSumm: ContentSummary, config: Map[String, AnyRef], dtRange: DtRange, game_version: String): MeasuredEvent = {

        val mid = CommonUtil.getMessageId("ME_CONTENT_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange, contentSumm.content_id);
        val measures = Map(
            "timeSpent" -> contentSumm.total_ts,
            "totalSessions" -> contentSumm.total_num_sessions,
            "averageTimeSpent" -> contentSumm.average_ts_session,
            "totalInteractionEvents" -> contentSumm.total_interactions,
            "averageInteractionsPerMin" -> contentSumm.average_interactions_min,
            "sessionsPerWeek" -> contentSumm.num_sessions_week,
            "tsPerWeek" -> contentSumm.ts_week,
            "contentType" -> contentSumm.content_type,
            "mimeType" -> contentSumm.mime_type);
        MeasuredEvent("ME_CONTENT_SUMMARY", System.currentTimeMillis(), dtRange.to, "1.0", mid, None, None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange),
            Dimensions(None, None, Option(new GData(contentSumm.content_id, game_version)), None, None, None, None),
            MEEdata(measures));
    }

    private def getWeeksBetween(fromDate: Long, toDate: Long): Int = {
        val from = new LocalDate(fromDate)
        val to = new LocalDate(toDate)
        val dates = CommonUtil.datesBetween(from, to)
        dates.size / 7
    }
}