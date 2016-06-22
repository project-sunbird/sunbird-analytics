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
import org.ekstep.analytics.framework.util.JobLogger

case class ContentSummary(content_id: String, start_date: DateTime, total_num_sessions: Long, total_ts: Double, average_ts_session: Double,
                          total_interactions: Long, average_interactions_min: Double, num_sessions_week: Double, ts_week: Double, content_type: String, mime_type: String)
case class ContentSummaryInput(contentId: String, events: Buffer[DerivedEvent], prevSummary: Option[ContentSummary]) extends AlgoInput
case class ContentSummaryOutput(contentId: String, cs: ContentSummary, dtRange: DtRange, gameVersion: String) extends AlgoOutput

object ContentActivitySummary extends IBatchModelTemplate[DerivedEvent, ContentSummaryInput, ContentSummaryOutput, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.ContentActivitySummary"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSummaryInput] = {
        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));

        val newEvents = filteredEvents.map(event => (event.dimensions.gdata.get.id, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b);
        val prevContentState = newEvents.map(f => ContentId(f._1)).joinWithCassandraTable[ContentSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE).map(f => (f._1.content_id, f._2))
        val contentData = newEvents.leftOuterJoin(prevContentState);
        contentData.map { x => ContentSummaryInput(x._1, x._2._1, x._2._2) }
    }

    override def algorithm(data: RDD[ContentSummaryInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSummaryOutput] = {
        data.map { f =>

            val events = f.events
            val sortedEvents = events.sortBy { x => x.syncts };
            val firstEvent = sortedEvents.head;
            val lastEvent = sortedEvents.last;
            val eventSyncts = lastEvent.syncts;
            val gameId = firstEvent.dimensions.gdata.get.id;
            val gameVersion = firstEvent.dimensions.gdata.get.ver;

            val firstGE = events.sortBy { x => x.context.date_range.from }.head;
            val lastGE = events.sortBy { x => x.context.date_range.to }.last;
            val eventStartTimestamp = firstGE.context.date_range.from;
            val eventEndTimestamp = lastGE.context.date_range.to;
            val eventStartDate = new DateTime(eventStartTimestamp);
            val date_range = DtRange(firstEvent.syncts, lastEvent.syncts);

            val prevContentSummary = f.prevSummary.getOrElse(ContentSummary(gameId, DateTime.now(), 0L, 0.0, 0.0, 0L, 0.0, 0L, 0.0, "", ""));
            val startDate = if (eventStartDate.isBefore(prevContentSummary.start_date)) eventStartDate else prevContentSummary.start_date;
            val numSessions = sortedEvents.size + prevContentSummary.total_num_sessions;
            val timeSpent = sortedEvents.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
            }.sum + (prevContentSummary.total_ts * 3600);
            val totalInteractions = sortedEvents.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Int])
            }.sum + prevContentSummary.total_interactions;
            val numWeeks = CommonUtil.getWeeksBetween(startDate.getMillis, eventEndTimestamp)

            val averageTsSession = (timeSpent / numSessions);
            val averageInteractionsMin = if (totalInteractions == 0 || timeSpent == 0) 0d else CommonUtil.roundDouble(BigDecimal(totalInteractions / (timeSpent / 60)).toDouble, 2);
            val numSessionsWeek: Double = if (numWeeks == 0) numSessions else numSessions / numWeeks
            val tsWeek = if (numWeeks == 0) timeSpent else timeSpent / numWeeks
            val contentType = if (prevContentSummary.content_type.isEmpty()) firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("contentType", "").asInstanceOf[String] else prevContentSummary.content_type
            val mimeType = if (prevContentSummary.mime_type.isEmpty()) firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("mimeType", "").asInstanceOf[String] else prevContentSummary.mime_type

            ContentSummaryOutput(f.contentId, ContentSummary(gameId, startDate, numSessions, CommonUtil.roundDouble(timeSpent / 3600, 2), CommonUtil.roundDouble(averageTsSession, 2), totalInteractions, CommonUtil.roundDouble(averageInteractionsMin, 2), numSessionsWeek, tsWeek, contentType, mimeType), date_range, gameVersion)
        }.cache();
    }

    override def postProcess(data: RDD[ContentSummaryOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map(f => f.cs).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE);

        val summaries = sc.cassandraTable[ContentSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE).filter { x => !"Collection".equals(x.content_type) };
        val count = summaries.count().intValue();
        val defaultVal = if (5 > count) count else 5;
        val topContentByTime = summaries.sortBy(f => f.total_ts, false, 1).take(config.getOrElse("topK", defaultVal).asInstanceOf[Int]);
        val topContentBySessions = summaries.sortBy(f => f.total_num_sessions, false, 1).take(config.getOrElse("topK", defaultVal).asInstanceOf[Int]);

        val rdd = sc.parallelize(Array(ContentMetrics("content", topContentByTime.map { x => (x.content_id, x.total_ts) }.toMap, topContentBySessions.map { x => (x.content_id, x.total_num_sessions) }.toMap)), 1);
        rdd.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_METRICS_TABLE);
        data.map { input =>
            val contentSumm = input.cs
            val mid = CommonUtil.getMessageId("ME_CONTENT_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], input.dtRange, contentSumm.content_id);
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
            MeasuredEvent("ME_CONTENT_SUMMARY", System.currentTimeMillis(), input.dtRange.to, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], input.dtRange),
                Dimensions(None, None, Option(new GData(contentSumm.content_id, input.gameVersion)), None, None, None, None),
                MEEdata(measures));
        }

    }
}