package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.ContentPopularityMetrics
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.util.CommonUtil

object ContentPopularityMetricsModel extends IMetricsModel[ContentPopularityMetrics]  with Serializable {
	override def metric : String = "cps";
	
	override def getMetrics(records: RDD[ContentPopularityMetrics], period: String)(implicit sc: SparkContext, config: Config): Array[ContentPopularityMetrics] = {
	    
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsRDD = records.map { x => (x.d_period, x) };
		var periodsRDD = sc.parallelize(periods.map { period => (period, ContentPopularityMetrics(period)) });
		periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
			if(f._2._2.isDefined) f._2._2.get else f._2._1 
		}.map { x => x.label = Option(CommonUtil.getPeriodLabel(periodEnum, x.d_period)); x }.collect();
	}
	
	override def getSummary(metrics: Array[ContentPopularityMetrics]): Map[String, AnyRef] = {
		Map();
	}
}