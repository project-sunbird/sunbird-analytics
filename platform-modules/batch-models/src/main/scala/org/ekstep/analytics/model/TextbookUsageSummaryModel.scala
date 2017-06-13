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
case class TextbookUsageInput(period: Int, session_events: Buffer[DerivedEvent]) extends AlgoInput
case class TextbookUsageOutput(period: Int, dtRange: DtRange, unique_users_count: Long, time_spent: Double, total_sessions: Long, avg_ts_session: Double, textbooks_count: Long, unit_summary: UnitSummary, lesson_summary: LessonSummary, syncts: Long) extends AlgoOutput with Output
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
        val session_events = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_TEXTBOOK_SESSION_SUMMARY")));
        session_events.map { f =>
            val period = CommonUtil.getPeriod(f.context.date_range.to, Period.DAY);
            (period, Buffer(f))
        }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => TextbookUsageInput(x._1, x._2) };
    }

    override def algorithm(data: RDD[TextbookUsageInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookUsageOutput] = {

        data.map { event =>
            val first_event = event.session_events.sortBy { x => x.context.date_range.from }.head
            val last_event = event.session_events.sortBy { x => x.context.date_range.to }.last
            val unique_users_count = event.session_events.map(x => x.uid).distinct.filterNot { x => x.isEmpty() }.toList.length.toLong
            val textbooks_count = event.session_events.map(x => x.dimensions.content_id.get).distinct.filterNot { x => x.isEmpty() }.toList.length.toLong
            val total_sessions = event.session_events.length.toLong
            val date_range = DtRange(first_event.context.date_range.from, last_event.context.date_range.to);
            val time_spent = event.session_events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_spent").get.asInstanceOf[Number].longValue() }.sum
            val total_units_added = event.session_events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("unit_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("added_count", 0l).asInstanceOf[Number].longValue() }
            val total_units_deleted = event.session_events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("unit_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue() }
            val total_units_modified = event.session_events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("unit_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("modified_count", 0l).asInstanceOf[Number].longValue() }
            val total_lessons_added = event.session_events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("lesson_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("added_count", 0l).asInstanceOf[Number].longValue() }
            val total_lessons_deleted = event.session_events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("lesson_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue() }
            val total_lessons_modified = event.session_events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("lesson_summary").get.asInstanceOf[Map[String, AnyRef]].getOrElse("modified_count", 0l).asInstanceOf[Number].longValue() }
            val avg_ts_session = if (time_spent == 0 || total_sessions == 0) 0d else BigDecimal(time_spent / total_sessions).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            TextbookUsageOutput(event.period, date_range, unique_users_count, time_spent, total_sessions, avg_ts_session, textbooks_count, UnitSummary(total_units_added.sum, total_units_deleted.sum, total_units_modified.sum), LessonSummary(total_lessons_added.sum, total_lessons_deleted.sum, total_lessons_modified.sum), last_event.syncts)
        }
    }

    override def postProcess(data: RDD[TextbookUsageOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { usageSumm =>
            val mid = CommonUtil.getMessageId("ME_TEXTBOOK_USAGE_SUMMARY", "", "DAY", usageSumm.dtRange);
            val measures = Map(
                "unique_users_count" -> usageSumm.unique_users_count,
                "total_sessions" -> usageSumm.total_sessions,
                "total_ts" -> usageSumm.time_spent,
                "textbooks_count" -> usageSumm.textbooks_count,
                "avg_ts_session" -> usageSumm.avg_ts_session,
                "unit_summary" -> usageSumm.unit_summary,
                "lesson_summary" -> usageSumm.lesson_summary);
            MeasuredEvent("ME_TEXTBOOK_USAGE_SUMMARY", System.currentTimeMillis(), usageSumm.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "TextbookUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", usageSumm.dtRange),
                Dimensions(None, None, None, None, None, None, None, None, None, None, Option(usageSumm.period), None, None, None, None, None, None, None, None, None, None, None, None, None),
                MEEdata(measures), None);
        }
    }
}