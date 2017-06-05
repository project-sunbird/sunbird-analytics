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
import org.ekstep.analytics.util.Constants
/**
 * @author yuva
 */
case class TextbookUsageInput(period: Int, sessionEvents: Buffer[DerivedEvent]) extends AlgoInput
case class TextbookUsageOutput(period: Int, dtRange: DtRange, userCount: Long, time_spent: Double, time_diff: Double, totalSessions: Long, avgSessionTS: Double, unit_summary: UnitSummary, lesson_summary: LessonSummary) extends AlgoOutput with Output
/**
 * @dataproduct
 * @Summarizer
 *
 * TextbookUsageSummaryModel
 *
 * Functionality
 * Compute period wise Textbook summary : Units and Lessons added/deleted/modified
 * input - ME_TEXTBOOK_SESSION_SUMMARY
 */
object TextbookUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, TextbookUsageInput, TextbookUsageOutput, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.TextbookUsageSummaryModel"
    override def name: String = "TextbookUsageSummaryModel"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookUsageInput] = {
        val sessionEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_TEXTBOOK_SESSION_SUMMARY")));
        sessionEvents.map { f =>
            val period = CommonUtil.getPeriod(f.context.date_range.to, Period.DAY);
            (period, Buffer(f))
        }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => TextbookUsageInput(x._1, x._2) };
    }

    override def algorithm(data: RDD[TextbookUsageInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookUsageOutput] = {

        data.map { event =>
            val firstEvent = event.sessionEvents.sortBy { x => x.context.date_range.from }.head
            val lastEvent = event.sessionEvents.sortBy { x => x.context.date_range.to }.last
            val userCount = event.sessionEvents.map(x => x.uid).distinct.filterNot { x => x.isEmpty() }.toList.length.toLong
            val totalSessions = event.sessionEvents.length.toLong
            val date_range = DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to);
            val time_spent = event.sessionEvents.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_spent").get.asInstanceOf[Number].longValue() }.sum
            val time_diff = event.sessionEvents.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_diff").get.asInstanceOf[Number].longValue() }.sum
            val total_units_added = event.sessionEvents.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("unit_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("total_units_added", 0l).asInstanceOf[Number].longValue() }
            val total_units_deleted = event.sessionEvents.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("unit_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("total_units_deleted", 0l).asInstanceOf[Number].longValue() }
            val total_units_modified = event.sessionEvents.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("unit_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("total_units_modified", 0l).asInstanceOf[Number].longValue() }
            val total_lessons_added = event.sessionEvents.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("lesson_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("total_lessons_added", 0l).asInstanceOf[Number].longValue() }
            val total_lessons_deleted = event.sessionEvents.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("lesson_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("total_lessons_deleted", 0l).asInstanceOf[Number].longValue() }
            val total_lessons_modified = event.sessionEvents.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("lesson_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("total_lessons_modified", 0l).asInstanceOf[Number].longValue() }
            val avgSessionTS = if (time_spent == 0 || totalSessions == 0) 0d else BigDecimal(time_spent / totalSessions).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            TextbookUsageOutput(event.period, date_range, userCount, time_spent, time_diff, totalSessions, avgSessionTS, UnitSummary(total_units_added.sum, total_units_deleted.sum, total_units_modified.sum), LessonSummary(total_lessons_added.sum, total_lessons_deleted.sum, total_lessons_modified.sum))
        }
    }

    override def postProcess(data: RDD[TextbookUsageOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { usageSumm =>
            val mid = CommonUtil.getMessageId("ME_TEXTBOOK_USAGE_SUMMARY", "", "DAY", usageSumm.dtRange);
            val measures = Map(
                "users_count" -> usageSumm.userCount,
                "total_sessions" -> usageSumm.totalSessions,
                "total_ts" -> usageSumm.time_spent,
                "avg_ts_session" -> usageSumm.avgSessionTS,
                "time_diff" -> usageSumm.time_diff,
                "unit_summary" -> usageSumm.unit_summary,
                "lesson_summary" -> usageSumm.lesson_summary);
            MeasuredEvent("ME_TEXTBOOK_USAGE_SUMMARY", System.currentTimeMillis(), usageSumm.dtRange.to, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "TextbookUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", usageSumm.dtRange),
                Dimensions(None, None, None, None, None, None, None, None, None, None, Option(usageSumm.period), None, None, None, None, None, None, None, None, None, None, None, None, None),
                MEEdata(measures), None);
        }
    }
}