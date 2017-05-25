/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._

/**
 * Case class for Cassandra Models
 */
case class CEUsageSummaryFact(d_period: Int, d_content_id: String, m_users_count: Long, m_total_sessions: Long, m_total_ts: Double, m_avg_time_spent: Double, m_last_updated_on: Long) extends AlgoOutput
case class CEUsageSummaryIndex(d_period: Int, d_content_id: String) extends Output

case class CEUsageSummaryFact_T(d_period: Int, d_content_id: String, m_users_count: Long, m_total_sessions: Long, m_total_ts: Double, m_avg_time_spent: Double, m_last_gen_date: Long) extends AlgoOutput

/**
 * @dataproduct
 * @Updater
 *
 * UpdateContentEditorMetricsDB
 *
 * Functionality
 * 1. Update content editor usage summary per day, week, month & cumulative metrics in Cassandra DB.
 * Event used - ME_CE_USAGE_SUMMARY
 */
object UpdateContentEditorUsageDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, CEUsageSummaryFact, CEUsageSummaryIndex] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateContentEditorMetricsDB"
    override def name: String = "UpdateContentEditorMetricsDB"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_CE_USAGE_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CEUsageSummaryFact] = {

        val CESummary = data.map { x =>

            val period = x.dimensions.period.get;

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val users_count = eksMap.get("users_count").get.asInstanceOf[Number].longValue()
            val total_sessions = eksMap.get("total_sessions").get.asInstanceOf[Number].longValue()
            val total_ts = eksMap.get("total_ts").get.asInstanceOf[Double]
            val avg_time_spent = eksMap.get("avg_time_spent").get.asInstanceOf[Double]
            
            CEUsageSummaryFact_T(period, x.dimensions.content_id.get, users_count, total_sessions, total_ts, avg_time_spent, x.context.date_range.to);
        }.cache();

        // Roll up summaries
        rollup(CESummary, DAY).union(rollup(CESummary, WEEK)).union(rollup(CESummary, MONTH)).union(rollup(CESummary, CUMULATIVE)).cache();
    }

    override def postProcess(data: RDD[CEUsageSummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CEUsageSummaryIndex] = {
        // Update the database
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY)
        data.map { x => CEUsageSummaryIndex(x.d_period, x.d_content_id) };
    }

    private def rollup(data: RDD[CEUsageSummaryFact_T], period: Period): RDD[CEUsageSummaryFact] = {

        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.m_last_gen_date, period);
            (CEUsageSummaryIndex(d_period, x.d_content_id), CEUsageSummaryFact_T(d_period, x.d_content_id, x.m_users_count, x.m_total_sessions, x.m_total_ts, x.m_avg_time_spent, x.m_last_gen_date));
        }.reduceByKey(reduceCEUS);
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).on(SomeColumns("d_period", "d_content_id"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(CEUsageSummaryFact(index.d_period, index.d_content_id, 0L, 0L, 0.0, 0.0, 0L));
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    private def reduceCEUS(fact1: CEUsageSummaryFact_T, fact2: CEUsageSummaryFact_T): CEUsageSummaryFact_T = {
        val users_count = fact2.m_users_count + fact1.m_users_count;
        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts + fact1.m_total_ts, 2);
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_time_spent = CommonUtil.roundDouble((total_ts / total_sessions), 2);

        CEUsageSummaryFact_T(fact1.d_period, fact1.d_content_id, users_count, total_sessions, total_ts, avg_time_spent, fact2.m_last_gen_date);
    }

    private def reduce(fact1: CEUsageSummaryFact, fact2: CEUsageSummaryFact_T, period: Period): CEUsageSummaryFact = {
        val users_count = fact2.m_users_count + fact1.m_users_count
        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts + fact1.m_total_ts, 2);
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_time_spent = if(!"all".equals(fact1.d_content_id)) 0.0 else CommonUtil.roundDouble((total_ts / total_sessions), 2);

        CEUsageSummaryFact(fact1.d_period, fact1.d_content_id, users_count, total_sessions, total_ts, avg_time_spent, System.currentTimeMillis());
    }
}