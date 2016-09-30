package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.ContentPopularityMetrics
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.util.CommonUtil

object ContentPopularityMetricsModel extends IMetricsModel[ContentPopularityMetrics]  with Serializable {
	override def metric : String = "cps";
	
	override def getMetrics(records: RDD[ContentPopularityMetrics], period: String)(implicit sc: SparkContext, config: Config): RDD[ContentPopularityMetrics] = {
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsRDD = records.map { x => (x.d_period.get, x) };
		var periodsRDD = sc.parallelize(periods.map { period => (period, ContentPopularityMetrics(Option(period))) });
		periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
			if(f._2._2.isDefined) f._2._2.get else f._2._1 
		}.map { x => x.label = Option(CommonUtil.getPeriodLabel(periodEnum, x.d_period.get)); x };
	}
	
	override def reduce(fact1: ContentPopularityMetrics, fact2: ContentPopularityMetrics): ContentPopularityMetrics = {
		val m_downloads = fact2.m_downloads.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_downloads.getOrElse(0l).asInstanceOf[Number].longValue();
		val m_side_loads = fact2.m_side_loads.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_side_loads.getOrElse(0l).asInstanceOf[Number].longValue();
		val m_ratings = (fact2.m_ratings.getOrElse(List()) ++ fact1.m_ratings.getOrElse(List())).distinct;
		val m_avg_rating = if (m_ratings.length > 0) {
			val total_rating = m_ratings.map(_._1).sum;
			if (total_rating > 0) CommonUtil.roundDouble(total_rating/m_ratings.length, 2) else 0.0;
		} else 0.0;
		ContentPopularityMetrics(None, None, Option(m_downloads), Option(m_side_loads), Option(m_ratings), Option(m_avg_rating));
	}
}