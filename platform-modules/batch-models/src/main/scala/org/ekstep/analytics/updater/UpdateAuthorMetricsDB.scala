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

case class AuthorMetricsFact(d_period: Int, d_author: String, last_updated_on: Long, total_sessions: Long, total_timespent: Double, total_ce_timespent: Double, total_ce_visit: Long, percent_ce_sessions: Double, avg_session_ts: Double, percent_ce_ts: Double) extends AlgoOutput with Output
case class AuthorMetricsIndex(d_period: Int, d_author: String)

object UpdateAuthorMetricsDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, AuthorMetricsFact, AuthorMetricsFact] with Serializable {

    override def name(): String = "UpdateAuthorMetricsDB";
    implicit val className = "org.ekstep.analytics.updater.UpdateAuthorMetricsDB";
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }
    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorMetricsFact] = {
        val authorMetrics = data.map { x =>
            val period = x.dimensions.period.get
            val author = x.uid
            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val totalSessions = eksMap.getOrElse("total_sessions", 0L).asInstanceOf[Number].longValue()
            val totalTS = eksMap.getOrElse("total_ts", 0.0).asInstanceOf[Double]
            val totalCETS = eksMap.getOrElse("ce_total_ts", 0.0).asInstanceOf[Double]
            val totalCEVisits = eksMap.getOrElse("ce_total_visits", 0.0).asInstanceOf[Number].longValue()
            val percentCEsessions = (totalCEVisits * 1.0 / totalSessions) * 100
            val avgSessionTS = totalTS / totalSessions
            val percentCEts = (totalCETS / totalTS) * 100
            AuthorMetricsFact(period, author, System.currentTimeMillis(), totalSessions, totalTS, totalCETS, totalCEVisits, percentCEsessions, avgSessionTS, percentCEts)
        }.cache
        rollup(authorMetrics, DAY).union(rollup(authorMetrics, WEEK)).union(rollup(authorMetrics, MONTH)).union(rollup(authorMetrics, CUMULATIVE)).cache();
    }

    override def postProcess(data: RDD[AuthorMetricsFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuthorMetricsFact] = {
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT);
        data;
    }

    private def rollup(data: RDD[AuthorMetricsFact], period: Period): RDD[AuthorMetricsFact] = {

        val currentData = data.map { x =>
            (AuthorMetricsIndex(x.d_period, x.d_author), x)
        }
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[AuthorMetricsFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT).on(SomeColumns("d_period", "d_author"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(AuthorMetricsFact(index.d_period, index.d_author, System.currentTimeMillis(), 0l, 0.0, 0.0, 0l, 0.0, 0.0, 0.0))
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    private def reduce(fact1: AuthorMetricsFact, fact2: AuthorMetricsFact, period: Period): AuthorMetricsFact = {
        val totalSessions = fact1.total_sessions + fact2.total_sessions
        val totalTS = fact1.total_timespent + fact2.total_timespent
        val totalCETS = fact1.total_ce_timespent + fact2.total_ce_timespent
        val totalCEVisits = fact1.total_ce_visit + fact2.total_ce_visit
        val percentCEsessions = (totalCEVisits * 1.0 / totalSessions) * 100
        val avgSessionTS = totalTS / totalSessions
        val percentCEts = (totalCETS / totalTS) * 100
        AuthorMetricsFact(fact1.d_period, fact1.d_author, System.currentTimeMillis(), totalSessions, totalTS, totalCETS, totalCEVisits, percentCEsessions, avgSessionTS, percentCEts)
    }
}