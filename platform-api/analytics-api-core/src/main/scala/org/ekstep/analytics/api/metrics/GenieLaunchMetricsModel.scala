package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.GenieLaunchMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CommonUtil

object GenieLaunchMetricsModel extends IMetricsModel[GenieLaunchMetrics, GenieLaunchMetrics]  with Serializable {
  	override def metric : String = "gls";
	
	override def getMetrics(records: RDD[GenieLaunchMetrics], period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config): RDD[GenieLaunchMetrics] = {
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsRDD = records.map { x => (x.d_period.get, x) };
		var periodsRDD = sc.parallelize(periods.map { period => (period, GenieLaunchMetrics(Option(period), Option(CommonUtil.getPeriodLabel(periodEnum, period)))) });
		periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
			if(f._2._2.isDefined) _merge(f._2._2.get, f._2._1) else f._2._1
		};
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