package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.ContentUsageMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.IMetricsModel

object CotentUsageMetricsModel extends IMetricsModel[ContentUsageMetrics]  with Serializable {
	
	override def metric : String = "cus";
	
	override def getMetrics(records: RDD[ContentUsageMetrics], period: String)(implicit sc: SparkContext, config: Config): Array[ContentUsageMetrics] = {
	    
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsRDD = records.map { x => (x.d_period, x) };
		var periodsRDD = sc.parallelize(periods.map { period => (period, ContentUsageMetrics(period)) });
		periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
			if(f._2._2.isDefined) f._2._2.get else f._2._1 
		}.map { x => x.label = Option(CommonUtil.getPeriodLabel(periodEnum, x.d_period)); x }.collect();
	}
	
	override def getSummary(metrics: Array[ContentUsageMetrics]): Map[String, AnyRef] = {
		Map();
	}
	
}