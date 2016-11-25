package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.ContentUsageMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.framework.util.JSONUtils

object CotentUsageMetricsModel extends IMetricsModel[ContentUsageMetrics, ContentUsageMetrics] with Serializable {

    override def metric: String = "cus";

    override def getMetrics(records: RDD[ContentUsageMetrics], period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config): RDD[ContentUsageMetrics] = {

        val periodEnum = periodMap.get(period).get._1;
        val periods = _getPeriods(period);
        val recordsRDD = records.map { x => (x.d_period.get, x) };
        val periodsRDD = sc.parallelize(periods.map { period => (period, ContentUsageMetrics(Option(period), Option(CommonUtil.getPeriodLabel(periodEnum, period)))) });
        periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
            if (f._2._2.isDefined) _merge(f._2._2.get, f._2._1) else f._2._1
        };
    }

    private def _merge(obj: ContentUsageMetrics, dummy: ContentUsageMetrics): ContentUsageMetrics = {
        ContentUsageMetrics(dummy.d_period, dummy.label, obj.m_total_ts, obj.m_total_sessions, obj.m_avg_ts_session, obj.m_total_interactions,
            obj.m_avg_interactions_min, obj.m_total_devices, obj.m_avg_sess_device)
    }

    override def reduce(fact1: ContentUsageMetrics, fact2: ContentUsageMetrics, fields: Array[String] = Array()): ContentUsageMetrics = {
        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts.get + fact1.m_total_ts.get, 2);
        val total_sessions = fact2.m_total_sessions.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_sessions.getOrElse(0l).asInstanceOf[Number].longValue()
        val avg_ts_session = if (total_sessions > 0) CommonUtil.roundDouble((total_ts / total_sessions), 2) else 0.0;
        val total_interactions = fact2.m_total_interactions.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_interactions.getOrElse(0l).asInstanceOf[Number].longValue()
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val total_devices = fact2.m_total_devices.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_devices.getOrElse(0l).asInstanceOf[Number].longValue()
        val avg_sess_device = if (total_devices > 0) CommonUtil.roundDouble(total_sessions.toDouble / total_devices, 2) else 0.0;
        ContentUsageMetrics(fact1.d_period, None, Option(total_ts), Option(total_sessions), Option(avg_ts_session), Option(total_interactions), Option(avg_interactions_min), Option(total_devices), Option(avg_sess_device));
    }
    
    override def getSummary(summary: ContentUsageMetrics) : ContentUsageMetrics = {
    	ContentUsageMetrics(None, None, summary.m_total_ts, summary.m_total_sessions, summary.m_avg_ts_session, summary.m_total_interactions, summary.m_avg_interactions_min, summary.m_total_devices, summary.m_avg_sess_device);
    }
}