package org.ekstep.analytics.updater

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.model.UnitSummary
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.model.LessonSummary
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher
import org.joda.time.DateTime
/**
 * @author yuva
 */
case class TextbookSessionMetricsFact(d_period: Int, time_spent: Double, time_diff: Double, unit_summary: Map[String, Long], lesson_summary: Map[String, Long], updated_date: Long) extends AlgoOutput with Output
case class TextbookSessionMetricsFact_T(d_period: Int, time_spent: Double, time_diff: Double, unit_summary: Map[String, Long], lesson_summary: Map[String, Long], updated_date: Long, last_gen_date: Long)
case class TextbookIndex(d_period: Int)
/**
 * @dataproduct
 * @Updater
 *
 * UpdateTextbookUsageDB
 *
 * Functionality
 * Update Textbook summary : Units and Lessons added/deleted/modified into cassandra table and influxdb
 * input - ME_TEXTBOOK_USAGE_SUMMARY
 */
object UpdateTextbookUsageDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, TextbookSessionMetricsFact, TextbookSessionMetricsFact] with IInfluxDBUpdater with Serializable {

    override def name(): String = "UpdateTextbookUsageDB";
    implicit val className = "org.ekstep.analytics.updater.UpdateTextbookUsageDB";
    val TEXTBOOK_SESSION_METRICS = "textbook_session_metrics";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_TEXTBOOK_USAGE_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookSessionMetricsFact] = {
        val textbookSessions = data.map { x =>
            val d_period = x.dimensions.period.get
            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val unit_summary = eksMap.get("unit_summary").get.asInstanceOf[Map[String, Long]]
            val lesson_summary = eksMap.get("lesson_summary").get.asInstanceOf[Map[String, Long]]
            val start_time = eksMap.getOrElse("start_time", 0L).asInstanceOf[Number].longValue()
            val end_time = eksMap.getOrElse("end_time", 0L).asInstanceOf[Number].longValue()
            val time_spent = CommonUtil.roundDouble(eksMap.getOrElse("time_spent", 0.0).asInstanceOf[Double], 2)
            val time_diff = CommonUtil.roundDouble(eksMap.getOrElse("time_diff", 0.0).asInstanceOf[Double], 2)
            TextbookSessionMetricsFact_T(d_period, time_spent, time_diff, unit_summary, lesson_summary, System.currentTimeMillis(), x.context.date_range.to)
        }.cache
        rollup(textbookSessions, DAY).union(rollup(textbookSessions, WEEK)).union(rollup(textbookSessions, MONTH)).union(rollup(textbookSessions, CUMULATIVE)).cache();
    }
    override def postProcess(data: RDD[TextbookSessionMetricsFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookSessionMetricsFact] = {
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SESSION_METRICS_FACT);
        saveToInfluxDB(data);
        data;
    }

    private def rollup(data: RDD[TextbookSessionMetricsFact_T], period: Period): RDD[TextbookSessionMetricsFact] = {

        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.last_gen_date, period);
            (TextbookIndex(d_period), x)
        }.reduceByKey(reduceTUS);
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[TextbookSessionMetricsFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SESSION_METRICS_FACT).on(SomeColumns("d_period"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(TextbookSessionMetricsFact(index.d_period, 0.0, 0.0, Map("total_units_added" -> 0l, "total_units_deleted" -> 0l, "total_units_modified" -> 0l), Map("total_lessons_added" -> 0l, "total_lessons_deleted" -> 0l, "total_lessons_modified" -> 0l), System.currentTimeMillis()))
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    private def reduce(fact1: TextbookSessionMetricsFact, fact2: TextbookSessionMetricsFact_T, period: Period): TextbookSessionMetricsFact = {

        val totalTime_spent = CommonUtil.roundDouble(fact1.time_spent + fact2.time_spent, 2)
        val totalTime_diff = CommonUtil.roundDouble(fact1.time_diff + fact2.time_diff, 2)
        val totalUnits_added = fact1.unit_summary.getOrElse("total_units_added", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("total_units_added", 0l).asInstanceOf[Number].longValue()
        val totalUnits_deleted = fact1.unit_summary.getOrElse("total_units_deleted", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("total_units_deleted", 0l).asInstanceOf[Number].longValue()
        val totalUnits_modified = fact1.unit_summary.getOrElse("total_units_modified", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("total_units_modified", 0l).asInstanceOf[Number].longValue()
        val total_lessons_added = fact1.lesson_summary.getOrElse("total_lessons_added", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("total_lessons_added", 0l).asInstanceOf[Number].longValue()
        val total_lessons_deleted = fact1.lesson_summary.getOrElse("total_lessons_deleted", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("total_lessons_deleted", 0l).asInstanceOf[Number].longValue()
        val total_lessons_modified = fact1.lesson_summary.getOrElse("total_lessons_modified", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("total_lessons_modified", 0l).asInstanceOf[Number].longValue()
        val unit_summary = Map("total_units_added" -> totalUnits_added, "total_units_deleted" -> totalUnits_deleted, "total_units_modified" -> totalUnits_modified)
        val lesson_summary = Map("total_lessons_added" -> total_lessons_added, "total_lessons_deleted" -> total_lessons_deleted, "total_lessons_modified" -> total_lessons_modified)
        TextbookSessionMetricsFact(fact1.d_period, totalTime_spent, totalTime_diff, unit_summary, lesson_summary, System.currentTimeMillis())
    }

    private def reduceTUS(fact1: TextbookSessionMetricsFact_T, fact2: TextbookSessionMetricsFact_T): TextbookSessionMetricsFact_T = {
        val totalTime_spent = CommonUtil.roundDouble(fact1.time_spent + fact2.time_spent, 2)
        val totalTime_diff = CommonUtil.roundDouble(fact1.time_diff + fact2.time_diff, 2)
        val totalUnits_added = fact1.unit_summary.getOrElse("total_units_added", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("total_units_added", 0l).asInstanceOf[Number].longValue()
        val totalUnits_deleted = fact1.unit_summary.getOrElse("total_units_deleted", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("total_units_deleted", 0l).asInstanceOf[Number].longValue()
        val totalUnits_modified = fact1.unit_summary.getOrElse("total_units_modified", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("total_units_modified", 0l).asInstanceOf[Number].longValue()
        val total_lessons_added = fact1.lesson_summary.getOrElse("total_lessons_added", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("total_lessons_added", 0l).asInstanceOf[Number].longValue()
        val total_lessons_deleted = fact1.lesson_summary.getOrElse("total_lessons_deleted", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("total_lessons_deleted", 0l).asInstanceOf[Number].longValue()
        val total_lessons_modified = fact1.lesson_summary.getOrElse("total_lessons_modified", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("total_lessons_modified", 0l).asInstanceOf[Number].longValue()
        val unit_summary = Map("total_units_added" -> totalUnits_added, "total_units_deleted" -> totalUnits_deleted, "total_units_modified" -> totalUnits_modified)
        val lesson_summary = Map("total_lessons_added" -> total_lessons_added, "total_lessons_deleted" -> total_lessons_deleted, "total_lessons_modified" -> total_lessons_modified)
        TextbookSessionMetricsFact_T(fact1.d_period, totalTime_spent, totalTime_diff, unit_summary, lesson_summary, System.currentTimeMillis(), fact2.last_gen_date)

    }

    private def saveToInfluxDB(data: RDD[TextbookSessionMetricsFact]) {
        val metrics = data.filter { x => x.d_period != 0 }.map { x =>
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("d_period" -> time._2, "updated_date" -> x.updated_date.toString()), Map("time_spent" -> x.time_spent.asInstanceOf[AnyRef], "time_diff" -> x.time_diff.asInstanceOf[AnyRef],

                "unit_summary.total_units_added" -> x.unit_summary.getOrElse("total_units_added", 0l).asInstanceOf[AnyRef],
                "unit_summary.total_units_deleted" -> x.unit_summary.getOrElse("total_units_deleted", 0l).asInstanceOf[AnyRef],
                "unit_summary.total_units_modified" -> x.unit_summary.getOrElse("total_units_modified", 0l).asInstanceOf[AnyRef],
                "lesson_summary.total_lessons_added" -> x.lesson_summary.getOrElse("total_lessons_added", 0l).asInstanceOf[Number],
                "lesson_summary.total_lessons_deleted" -> x.lesson_summary.getOrElse("total_lessons_deleted", 0l).asInstanceOf[Number],
                "lesson_summary.total_lessons_modified" -> x.lesson_summary.getOrElse("total_lessons_modified", 0l).asInstanceOf[Number]), time._1);
        };
        InfluxDBDispatcher.dispatch(TEXTBOOK_SESSION_METRICS, metrics);
    }

}