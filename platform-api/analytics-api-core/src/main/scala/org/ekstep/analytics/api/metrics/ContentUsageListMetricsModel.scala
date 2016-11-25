package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.ContentUsageListMetrics
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.service.RecommendationAPIService
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.util.ContentCacheUtil

object ContentUsageListMetricsModel  extends IMetricsModel[ContentUsageListMetrics, ContentUsageListMetrics]  with Serializable {
	
	override def metric : String = "gls"; // Because content list is part of GLS.
	
	override def preProcess()(implicit sc: SparkContext, config: Config) = {
		ContentCacheUtil.validateCache()(sc, config);
	}
	
	override def getMetrics(records: RDD[ContentUsageListMetrics], period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config): RDD[ContentUsageListMetrics] = {
		val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsRDD = records.map { x => (x.d_period.get, x) };
		val periodsRDD = sc.parallelize(periods.map { period => (period, ContentUsageListMetrics(Option(period))) });
		periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
			if(f._2._2.isDefined) f._2._2.get else f._2._1 
		}.map { x => 
			val label = Option(CommonUtil.getPeriodLabel(periodEnum, x.d_period.get));
			val contents = for(id <- x.m_contents.getOrElse(List())) yield {
				ContentCacheUtil.get.getOrElse(id.toString, Map())
			}
			ContentUsageListMetrics(x.d_period, label, x.m_contents, Option(contents.filter(f => !f.isEmpty)));
		};
	}
	
	override def reduce(fact1: ContentUsageListMetrics, fact2: ContentUsageListMetrics, fields: Array[String] = Array()): ContentUsageListMetrics = {
	    val m_contents = (fact2.m_contents.getOrElse(List()) ++ fact1.m_contents.getOrElse(List())).distinct;
		val contents = (fact2.content.getOrElse(List()) ++ fact1.content.getOrElse(List())).distinct;
		ContentUsageListMetrics(fact1.d_period, None, Option(m_contents), Option(contents))
	}
	
	override def getSummary(summary: ContentUsageListMetrics) : ContentUsageListMetrics = {
		ContentUsageListMetrics(None, None, summary.m_contents, summary.content)
	}
}