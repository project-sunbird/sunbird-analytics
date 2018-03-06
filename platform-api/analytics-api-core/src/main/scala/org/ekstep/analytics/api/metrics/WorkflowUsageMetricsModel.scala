package org.ekstep.analytics.api.metrics

import com.typesafe.config.Config
import org.ekstep.analytics.api.metrics.UsageMetricsModel.{_getPeriods, periodMap}
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.{IMetricsModel, WorkflowUsageMetrics}

object WorkflowUsageMetricsModel extends IMetricsModel[WorkflowUsageMetrics, WorkflowUsageMetrics] with Serializable {

    override def metric: String = "wfus";

    override def getMetrics(records: Array[WorkflowUsageMetrics], period: String, fields: Array[String] = Array())(implicit config: Config): Array[WorkflowUsageMetrics] = {

        val periodEnum = periodMap.get(period).get._1;
        val periods = _getPeriods(period);
        val recordsArray = records.map { x => (x.d_period.get, x) };
        val periodsArray = periods.map { period => (period, WorkflowUsageMetrics(Option(period), Option(CommonUtil.getPeriodLabel(periodEnum, period)))) };
        periodsArray.map { tup1 =>
          val tmp = recordsArray.filter(tup2 => tup1._1 == tup2._1)
          if (tmp.isEmpty) (tup1._1, (tup1._2, None)) else (tup1._1, (tup1._2, tmp.apply(0)._2))
        }.sortBy(-_._1).map { f => if (None != f._2._2 ) _merge(f._2._2.asInstanceOf[WorkflowUsageMetrics], f._2._1) else f._2._1 }
    }

    private def _merge(obj: WorkflowUsageMetrics, dummy: WorkflowUsageMetrics): WorkflowUsageMetrics = {
        WorkflowUsageMetrics(dummy.d_period, dummy.label, obj.m_total_ts, obj.m_total_sessions, obj.m_avg_ts_session, obj.m_total_interactions,
          obj.m_avg_interactions_min, obj.m_total_pageviews_count, obj.m_avg_pageviews, obj.m_total_users_count, obj.m_total_content_count, obj.m_total_devices_count)
    }

    override def reduce(fact1: WorkflowUsageMetrics, fact2: WorkflowUsageMetrics, fields: Array[String] = Array()): WorkflowUsageMetrics = {
        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts.get + fact1.m_total_ts.get, 2);
        val total_sessions = fact2.m_total_sessions.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_sessions.getOrElse(0l).asInstanceOf[Number].longValue()
        val avg_ts_session = if (total_sessions > 0) CommonUtil.roundDouble((total_ts / total_sessions), 2) else 0.0;
        val total_interactions = fact2.m_total_interactions.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_interactions.getOrElse(0l).asInstanceOf[Number].longValue()
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val total_pageviews_count = fact2.m_total_pageviews_count.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_pageviews_count.getOrElse(0l).asInstanceOf[Number].longValue()
        val avg_pageviews = if (total_sessions > 0) CommonUtil.roundDouble((total_pageviews_count / total_sessions), 2) else 0.0;
        val total_users_count = fact2.m_total_users_count.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_users_count.getOrElse(0l).asInstanceOf[Number].longValue()
        val total_content_count = fact2.m_total_content_count.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_content_count.getOrElse(0l).asInstanceOf[Number].longValue()
        val total_devices_count = fact2.m_total_devices_count.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_total_devices_count.getOrElse(0l).asInstanceOf[Number].longValue()
        WorkflowUsageMetrics(fact1.d_period, None, Option(total_ts), Option(total_sessions), Option(avg_ts_session), Option(total_interactions), Option(avg_interactions_min), Option(total_pageviews_count), Option(avg_pageviews), Option(total_users_count), Option(total_content_count), Option(total_devices_count));
    }

    override def getSummary(summary: WorkflowUsageMetrics) : WorkflowUsageMetrics = {
        WorkflowUsageMetrics(None, None, summary.m_total_ts, summary.m_total_sessions, summary.m_avg_ts_session, summary.m_total_interactions, summary.m_avg_interactions_min, summary.m_total_pageviews_count, summary.m_avg_pageviews, summary.m_total_users_count, summary.m_total_content_count, summary.m_total_devices_count);
    }
}
