package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.ItemUsageMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.ItemUsageSummary
import scala.collection.JavaConversions._
import org.ekstep.analytics.api.ItemUsageSummaryView
import org.ekstep.analytics.api.InCorrectRes

object ItemUsageMetricsModel extends IMetricsModel[ItemUsageSummaryView, ItemUsageMetrics]  with Serializable {
  	override def metric : String = "ius";
  	
  	override def getMetrics(records: RDD[ItemUsageSummaryView], period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config): RDD[ItemUsageMetrics] = {
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsRDD = records.groupBy { x => x.d_period + "-" + x.d_content_id }.map{ f => 
			val items = f._2.map { x => 
				val top5_incorrect_res = if (null == x.m_top5_incorrect_res || x.m_top5_incorrect_res.isEmpty) List() else  x.m_top5_incorrect_res.map(f => InCorrectRes(f._1, f._2));
				ItemUsageSummary(x.d_item_id, Option(x.d_content_id), Option(x.m_total_ts), Option(x.m_total_count), Option(x.m_correct_res_count), Option(x.m_inc_res_count), Option(x.m_correct_res), Option(top5_incorrect_res), Option(x.m_avg_ts)) 
			}.toList;
			val first = f._2.head;
			ItemUsageMetrics(Option(first.d_period.get), None, Option(items));
		}.map { x => (x.d_period.get, x) };
		val periodsRDD = sc.parallelize(periods.map { period => (period, ItemUsageMetrics(Option(period),  Option(CommonUtil.getPeriodLabel(periodEnum, period)))) });
		periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
			if(f._2._2.isDefined) _merge(f._2._2.get, f._2._1) else f._2._1 
		};
	}
  	
  	private def _merge(obj: ItemUsageMetrics, dummy: ItemUsageMetrics): ItemUsageMetrics = {
        ItemUsageMetrics(dummy.d_period, dummy.label, obj.items)
    }
  	
  	override def reduce(fact1: ItemUsageMetrics, fact2: ItemUsageMetrics, fields: Array[String] = Array()): ItemUsageMetrics = {
		val items = (fact1.items ++ fact2.items).flatMap { x => x }
  		val summaryItems = items.groupBy { x => x.d_item_id }.map { f => f._2.reduce(reduceItemSummary) }.toList;
  		ItemUsageMetrics(fact1.d_period, None, Option(summaryItems));
	}
  	
  	private def reduceItemSummary(fact1: ItemUsageSummary, fact2: ItemUsageSummary) : ItemUsageSummary = {
  		val m_total_ts = fact2.m_total_ts.getOrElse(0.0) + fact1.m_total_ts.getOrElse(0.0);
		val m_total_count = fact2.m_total_count.getOrElse(0l).asInstanceOf[Number].longValue + fact1.m_total_count.getOrElse(0l).asInstanceOf[Number].longValue;
		val m_correct_res_count = fact2.m_correct_res_count.getOrElse(0l).asInstanceOf[Number].longValue + fact1.m_correct_res_count.getOrElse(0l).asInstanceOf[Number].longValue;
		val m_inc_res_count = fact2.m_inc_res_count.getOrElse(0l).asInstanceOf[Number].longValue + fact1.m_inc_res_count.getOrElse(0l).asInstanceOf[Number].longValue;
		val m_correct_res = (fact2.m_correct_res.getOrElse(List()) ++ fact1.m_correct_res.getOrElse(List())).distinct;
		val m_top5_incorrect_res = (fact1.m_top5_incorrect_res.getOrElse(List()) ++ fact2.m_top5_incorrect_res.getOrElse(List())).groupBy(f => f.resp).mapValues(f => f.map(x => x.count).sum).toList
									.sorted(Ordering.by((_: (String, Int))._2).reverse).take(5).map { x => InCorrectRes(x._1, x._2) }.toList;
		val m_avg_ts = if (m_total_count > 0) CommonUtil.roundDouble(m_total_ts/m_total_count, 2) else 0;
  		ItemUsageSummary(fact1.d_item_id, None, Option(m_total_ts), Option(m_total_count), Option(m_correct_res_count), Option(m_inc_res_count), Option(m_correct_res), Option(m_top5_incorrect_res), Option(m_avg_ts));
  	}
  	
  	override def getSummary(summary: ItemUsageMetrics) : ItemUsageMetrics = {
  		ItemUsageMetrics(None, None, summary.items);
  	}
  	
}