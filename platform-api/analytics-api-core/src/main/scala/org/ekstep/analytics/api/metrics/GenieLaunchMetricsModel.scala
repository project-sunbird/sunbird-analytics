package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.GenieLaunchMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CommonUtil

object GenieLaunchMetricsModel extends IMetricsModel[GenieLaunchMetrics]  with Serializable {
  	override def metric : String = "gls";
	
	override def getMetrics(records: RDD[GenieLaunchMetrics], period: String)(implicit sc: SparkContext, config: Config): Array[GenieLaunchMetrics] = {
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsRDD = records.map { x => (x.d_period, x) };
		var periodsRDD = sc.parallelize(periods.map { period => (period, GenieLaunchMetrics(period)) });
		periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
			if(f._2._2.isDefined) f._2._2.get else f._2._1 
		}.map { x => x.label = Option(CommonUtil.getPeriodLabel(periodEnum, x.d_period)); x }.collect();
	}
	
	override def getSummary(metrics: Array[GenieLaunchMetrics]): Map[String, AnyRef] = {
		Map();
	}
}