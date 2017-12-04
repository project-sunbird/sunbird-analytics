package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.GenieLaunchMetrics
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CommonUtil

object GenieLaunchMetricsModel extends IMetricsModel[GenieLaunchMetrics, GenieLaunchMetrics]  with Serializable {
  	override def metric : String = "gls";
	
	override def getMetrics(records: Array[GenieLaunchMetrics], period: String, fields: Array[String] = Array())(implicit config: Config): Array[GenieLaunchMetrics] = {
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsArray = records.map { x => (x.d_period.get, x) };
		val periodsArray = periods.map { period => (period, GenieLaunchMetrics(Option(period), Option(CommonUtil.getPeriodLabel(periodEnum, period)))) };
		periodsArray.map { tup1 =>
            val tmp = recordsArray.filter(tup2 => tup1._1 == tup2._1)
            if (tmp.isEmpty) (tup1._1, (tup1._2, None)) else (tup1._1, (tup1._2, tmp.apply(0)._2))
        }.sortBy(-_._1).map { f => if (None != f._2._2) _merge(f._2._2.asInstanceOf[GenieLaunchMetrics], f._2._1) else f._2._1 }
	}
	
	private def _merge(obj: GenieLaunchMetrics, dummy: GenieLaunchMetrics): GenieLaunchMetrics = {
        GenieLaunchMetrics(dummy.d_period, dummy.label, obj.m_total_sessions, obj.m_total_ts, obj.m_total_devices, 
        		obj.m_avg_sess_device, obj.m_avg_ts_session);
    }
	
	override def reduce(fact1: GenieLaunchMetrics, fact2: GenieLaunchMetrics, fields: Array[String] = Array()): GenieLaunchMetrics = {
		val total_sessions = fact2.m_total_sessions.getOrElse(0l).asInstanceOf[Number].longValue + fact1.m_total_sessions.getOrElse(0l).asInstanceOf[Number].longValue;
		val total_ts = CommonUtil.roundDouble((fact2.m_total_ts.getOrElse(0.0).asInstanceOf[Number].doubleValue + fact1.m_total_ts.getOrElse(0.0).asInstanceOf[Number].doubleValue), 2);
		val total_devices = fact2.m_total_devices.getOrElse(0l).asInstanceOf[Number].longValue + fact1.m_total_devices.getOrElse(0l).asInstanceOf[Number].longValue;
		val avg_sess_device = if (total_devices > 0) CommonUtil.roundDouble(total_sessions.toDouble / total_devices, 2) else 0.0;
		val avg_ts_session = if (total_sessions > 0) CommonUtil.roundDouble((total_ts / total_sessions), 2) else 0.0;
		GenieLaunchMetrics(fact1.d_period, None, Option(total_sessions), Option(total_ts), Option(total_devices), Option(avg_sess_device), Option(avg_ts_session));
	}
	
	override def getSummary(summary: GenieLaunchMetrics) : GenieLaunchMetrics = {
		GenieLaunchMetrics(None, None, summary.m_total_sessions, summary.m_total_ts, summary.m_total_devices, summary.m_avg_sess_device, summary.m_avg_ts_session);		
	}
}