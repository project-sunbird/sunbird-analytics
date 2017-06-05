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
case class TextbookSessionMetricsFact(d_period: Int, total_ts: Double, total_sessions: Long, avg_ts_session: Double, unique_users_count: Long, textbooks_count: Long, unit_summary: Map[String, Long], lesson_summary: Map[String, Long], updated_date: Long) extends AlgoOutput with Output
case class TextbookSessionMetricsFact_T(d_period: Int, total_ts: Double, total_sessions: Long, avg_ts_session: Double, unique_users_count: Long, textbooks_count: Long, unit_summary: Map[String, Long], lesson_summary: Map[String, Long], updated_date: Long, last_gen_date: Long)
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
        val textbook_sessions = data.map { x =>
            val d_period = x.dimensions.period.get
            val eks_map = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val unique_users_count = eks_map.getOrElse("unique_users_count", 0L).asInstanceOf[Number].longValue()
            val textbooks_count = eks_map.getOrElse("textbooks_count", 0L).asInstanceOf[Number].longValue()
            val total_sessions = eks_map.getOrElse("total_sessions", 0L).asInstanceOf[Number].longValue()
            val unit_summary = eks_map.get("unit_summary").get.asInstanceOf[Map[String, Long]]
            val lesson_summary = eks_map.get("lesson_summary").get.asInstanceOf[Map[String, Long]]
            val start_time = eks_map.getOrElse("start_time", 0L).asInstanceOf[Number].longValue()
            val end_time = eks_map.getOrElse("end_time", 0L).asInstanceOf[Number].longValue()
            val total_ts = CommonUtil.roundDouble(eks_map.getOrElse("total_ts", 0.0).asInstanceOf[Double], 2)
            val time_diff = CommonUtil.roundDouble(eks_map.getOrElse("time_diff", 0.0).asInstanceOf[Double], 2)
            val avg_ts_session = CommonUtil.roundDouble(eks_map.getOrElse("avg_ts_session", 0.0).asInstanceOf[Double], 2)
            TextbookSessionMetricsFact_T(d_period, total_ts, total_sessions, avg_ts_session, unique_users_count, textbooks_count, unit_summary, lesson_summary, System.currentTimeMillis(), x.context.date_range.to)
        }.cache
        rollup(textbook_sessions, DAY).union(rollup(textbook_sessions, WEEK)).union(rollup(textbook_sessions, MONTH)).union(rollup(textbook_sessions, CUMULATIVE)).cache();
    }
    override def postProcess(data: RDD[TextbookSessionMetricsFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookSessionMetricsFact] = {
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SESSION_METRICS_FACT);
        saveToInfluxDB(data);
        data;
    }

    private def rollup(data: RDD[TextbookSessionMetricsFact_T], period: Period): RDD[TextbookSessionMetricsFact] = {

        val current_data = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.last_gen_date, period);
            (TextbookIndex(d_period), x)
        }.reduceByKey(reduceTUS);
        val prv_data = current_data.map { x => x._1 }.joinWithCassandraTable[TextbookSessionMetricsFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEXTBOOK_SESSION_METRICS_FACT).on(SomeColumns("d_period"));
        val joined_data = current_data.leftOuterJoin(prv_data)
        val rollup_summaries = joined_data.map { x =>
            val index = x._1
            val new_summ = x._2._1
            val prv_summ = x._2._2.getOrElse(TextbookSessionMetricsFact(index.d_period, 0.0, 0l, 0.0, 0l, 0l, Map("added_count" -> 0l, "deleted_count" -> 0l, "modified_count" -> 0l), Map("added_count" -> 0l, "deleted_count" -> 0l, "modified_count" -> 0l), System.currentTimeMillis()))
            reduce(prv_summ, new_summ, period);
        }
        rollup_summaries;
    }

    private def reduce(fact1: TextbookSessionMetricsFact, fact2: TextbookSessionMetricsFact_T, period: Period): TextbookSessionMetricsFact = {

        val total_ts = CommonUtil.roundDouble(fact1.total_ts + fact2.total_ts, 2)
        val total_units_added = fact1.unit_summary.getOrElse("added_count", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("added_count", 0l).asInstanceOf[Number].longValue()
        val total_units_deleted = fact1.unit_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue()
        val total_units_modified = fact1.unit_summary.getOrElse("modified_count", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("modified_count", 0l).asInstanceOf[Number].longValue()
        val total_lessons_added = fact1.lesson_summary.getOrElse("added_count", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("added_count", 0l).asInstanceOf[Number].longValue()
        val total_lessons_deleted = fact1.lesson_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue()
        val total_lessons_modified = fact1.lesson_summary.getOrElse("modified_count", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("modified_count", 0l).asInstanceOf[Number].longValue()
        val unique_users_count = fact1.unique_users_count + fact2.unique_users_count
        val total_sessions = fact1.total_sessions + fact2.total_sessions
        val avg_ts_session = fact1.avg_ts_session + fact2.avg_ts_session
        val textbooks_count = fact1.textbooks_count + fact2.textbooks_count
        val unit_summary = Map("added_count" -> total_units_added, "deleted_count" -> total_units_deleted, "modified_count" -> total_units_modified)
        val lesson_summary = Map("added_count" -> total_lessons_added, "deleted_count" -> total_lessons_deleted, "modified_count" -> total_lessons_modified)
        TextbookSessionMetricsFact(fact1.d_period, total_ts, total_sessions, avg_ts_session, unique_users_count, textbooks_count, unit_summary, lesson_summary, System.currentTimeMillis())
    }

    private def reduceTUS(fact1: TextbookSessionMetricsFact_T, fact2: TextbookSessionMetricsFact_T): TextbookSessionMetricsFact_T = {
        val totalTime_spent = CommonUtil.roundDouble(fact1.total_ts + fact2.total_ts, 2)
        val total_units_added = fact1.unit_summary.getOrElse("added_count", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("added_count", 0l).asInstanceOf[Number].longValue()
        val total_units_deleted = fact1.unit_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue()
        val total_units_modified = fact1.unit_summary.getOrElse("modified_count", 0l).asInstanceOf[Number].longValue() + fact2.unit_summary.getOrElse("modified_count", 0l).asInstanceOf[Number].longValue()
        val total_lessons_added = fact1.lesson_summary.getOrElse("added_count", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("added_count", 0l).asInstanceOf[Number].longValue()
        val total_lessons_deleted = fact1.lesson_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number].longValue()
        val total_lessons_modified = fact1.lesson_summary.getOrElse("modified_count", 0l).asInstanceOf[Number].longValue() + fact2.lesson_summary.getOrElse("modified_count", 0l).asInstanceOf[Number].longValue()
        val unique_users_count = fact1.unique_users_count + fact2.unique_users_count
        val textbooks_count = fact1.textbooks_count + fact2.textbooks_count
        val total_sessions = fact1.total_sessions + fact2.total_sessions
        val avg_ts_session = fact1.avg_ts_session + fact2.avg_ts_session
        val unit_summary = Map("added_count" -> total_units_added, "deleted_count" -> total_units_deleted, "modified_count" -> total_units_modified)
        val lesson_summary = Map("added_count" -> total_lessons_added, "deleted_count" -> total_lessons_deleted, "modified_count" -> total_lessons_modified)
        TextbookSessionMetricsFact_T(fact1.d_period, totalTime_spent, total_sessions, avg_ts_session, unique_users_count, textbooks_count, unit_summary, lesson_summary, System.currentTimeMillis(), fact2.last_gen_date)

    }

    private def saveToInfluxDB(data: RDD[TextbookSessionMetricsFact]) {
        val metrics = data.filter { x => x.d_period != 0 }.map { x =>
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("d_period" -> time._2, "updated_date" -> x.updated_date.toString()),
                Map(
                    "total_ts" -> x.total_ts.asInstanceOf[AnyRef],
                    "unique_users_count" -> x.unique_users_count.asInstanceOf[AnyRef],
                    "total_sessions" -> x.total_sessions.asInstanceOf[AnyRef],
                    "avg_ts_session" -> x.avg_ts_session.asInstanceOf[AnyRef],
                    "textbooks_count" -> x.textbooks_count.asInstanceOf[AnyRef],
                    "unit_summary.added_count" -> x.unit_summary.getOrElse("added_count", 0l).asInstanceOf[AnyRef],
                    "unit_summary.deleted_count" -> x.unit_summary.getOrElse("deleted_count", 0l).asInstanceOf[AnyRef],
                    "unit_summary.modified_count" -> x.unit_summary.getOrElse("modified_count", 0l).asInstanceOf[AnyRef],
                    "lesson_summary.added_count" -> x.lesson_summary.getOrElse("added_count", 0l).asInstanceOf[Number],
                    "lesson_summary.deleted_count" -> x.lesson_summary.getOrElse("deleted_count", 0l).asInstanceOf[Number],
                    "lesson_summary.modified_count" -> x.lesson_summary.getOrElse("modified_count", 0l).asInstanceOf[Number]), time._1);
        };
        InfluxDBDispatcher.dispatch(TEXTBOOK_SESSION_METRICS, metrics);
    }

}