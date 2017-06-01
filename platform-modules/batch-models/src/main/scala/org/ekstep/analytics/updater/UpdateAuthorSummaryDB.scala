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

case class AuthorMetricsFact_T(d_period: Int, d_author_id: String, total_session: Long, total_ts: Double, total_ce_ts: Double, total_ce_visit: Long, percent_ce_sessions: Double, avg_session_ts: Double, percent_ce_ts: Double, updated_date: Long, last_gen_date: Long)
case class AuthorMetricsFact(d_period: Int, d_author_id: String, total_session: Long, total_ts: Double, total_ce_ts: Double, total_ce_visit: Long, percent_ce_sessions: Double, avg_session_ts: Double, percent_ce_ts: Double, updated_date: Long) extends AlgoOutput with Output
case class AuthorMetricsIndex(d_period: Int, d_author_id: String)

object UpdateAuthorSummaryDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, AuthorMetricsFact, AuthorMetricsFact] with Serializable {

    override def name(): String = "UpdateAuthorSummaryDB";
    implicit val className = "org.ekstep.analytics.updater.UpdateAuthorSummaryDB";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }
    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorMetricsFact] = {
        val authorMetrics = data.map { x =>
            val period = x.dimensions.period.get
            val author = x.uid
            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val totalSessions = eksMap.getOrElse("total_session", 0L).asInstanceOf[Number].longValue()
            val totalTS = CommonUtil.roundDouble(eksMap.getOrElse("total_ts", 0.0).asInstanceOf[Double], 2)
            val totalCETS = CommonUtil.roundDouble(eksMap.getOrElse("ce_total_ts", 0.0).asInstanceOf[Double], 2)
            val totalCEVisits = eksMap.getOrElse("ce_total_visits", 0l).asInstanceOf[Number].longValue()
            val ce_visits_occ = eksMap.getOrElse("ce_visits_occ", 0l).asInstanceOf[Number].longValue()
            val percentCEsessions = (if (0 != totalSessions) (ce_visits_occ * 1.0 / totalSessions) else 0.0) * 100
            val avgSessionTS = CommonUtil.roundDouble(if (0 != totalSessions) (totalTS / totalSessions) else 0.0, 2)
            val percentCEts = CommonUtil.roundDouble((if (0 != totalTS) (totalCETS / totalTS) else 0.0) * 100, 2)
            AuthorMetricsFact_T(period, author, totalSessions, totalTS, totalCETS, totalCEVisits, percentCEsessions, avgSessionTS, percentCEts, System.currentTimeMillis(), x.syncts)
        }.cache
        rollup(authorMetrics, DAY).union(rollup(authorMetrics, WEEK)).union(rollup(authorMetrics, MONTH)).union(rollup(authorMetrics, CUMULATIVE)).cache();
    }

    override def postProcess(data: RDD[AuthorMetricsFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorMetricsFact] = {
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT);
        data;
    }

    private def rollup(data: RDD[AuthorMetricsFact_T], period: Period): RDD[AuthorMetricsFact] = {

        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.last_gen_date, period);
            (AuthorMetricsIndex(d_period, x.d_author_id), x)
        }
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[AuthorMetricsFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT).on(SomeColumns("d_period", "d_author_id"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(AuthorMetricsFact(index.d_period, index.d_author_id, 0l, 0.0, 0.0, 0l, 0.0, 0.0, 0.0, System.currentTimeMillis()))
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    private def reduce(fact1: AuthorMetricsFact, fact2: AuthorMetricsFact_T, period: Period): AuthorMetricsFact = {
        val totalSessions = fact1.total_session + fact2.total_session
        val totalTS = CommonUtil.roundDouble(fact1.total_ts + fact2.total_ts, 2)
        val totalCETS = CommonUtil.roundDouble(fact1.total_ce_ts + fact2.total_ce_ts, 2)
        val totalCEVisits = fact1.total_ce_visit + fact2.total_ce_visit
        val percentCEsessions = (if (0 != totalSessions) (totalCEVisits * 1.0 / totalSessions) else 0.0) * 100
        val avgSessionTS = CommonUtil.roundDouble(if (0 != totalSessions) (totalTS / totalSessions) else 0.0, 2)
        val percentCEts = CommonUtil.roundDouble((if (0 != totalTS) (totalCETS / totalTS) else 0.0) * 100, 2)
        AuthorMetricsFact(fact1.d_period, fact1.d_author_id, totalSessions, totalTS, totalCETS, totalCEVisits, percentCEsessions, avgSessionTS, percentCEts, System.currentTimeMillis())
    }
}