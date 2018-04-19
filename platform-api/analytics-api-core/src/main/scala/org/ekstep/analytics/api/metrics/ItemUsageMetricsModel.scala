package org.ekstep.analytics.api.metrics

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.ekstep.analytics.api._
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.{CommonUtil, DBUtil}
import com.weather.scalacass.syntax._
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

case class ItemUsageTable(d_period: Int, d_content_id: String, d_tag: String, d_item_id: String, m_total_ts: Double, m_total_count: Int, m_correct_res_count: Int, m_inc_res_count: Int, m_correct_res: List[String], m_top5_incorrect_res: List[(String, List[String], Int)], m_avg_ts: Double, m_top5_mmc: List[(String, Int)])

object ItemUsageMetricsModel extends IMetricsModel[ItemUsageSummaryView, ItemUsageMetrics]  with Serializable {
  	override def metric : String = "ius";
  	
  	override def getMetrics(records: Array[ItemUsageSummaryView], period: String, fields: Array[String] = Array())(implicit config: Config): Array[ItemUsageMetrics] = {
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsArray = records.groupBy { x => x.d_period + "-" + x.d_content_id }.map{ f => 
			val items = f._2.map { x => 
				val top5_incorrect_res = if (null == x.m_top5_incorrect_res || x.m_top5_incorrect_res.isEmpty) List() else  x.m_top5_incorrect_res.map(f => InCorrectRes(f._1, f._2, f._3));
				val top5_mmc = if (null == x.m_top5_mmc || x.m_top5_mmc.isEmpty) List() else  x.m_top5_mmc.map(f => Misconception(f._1, f._2));
				ItemUsageSummary(x.d_item_id, Option(x.d_content_id), Option(x.m_total_ts), Option(x.m_total_count), Option(x.m_correct_res_count), Option(x.m_inc_res_count), Option(x.m_correct_res), Option(top5_incorrect_res), Option(x.m_avg_ts), Option(top5_mmc))
			}.toList;
			val first = f._2.head;
			ItemUsageMetrics(Option(first.d_period.get), None, Option(items));
		}.map { x => (x.d_period.get, x) }.toArray;
		val periodsArray = periods.map { period => (period, ItemUsageMetrics(Option(period),  Option(CommonUtil.getPeriodLabel(periodEnum, period)))) };
		periodsArray.map { tup1 =>
            val tmp = recordsArray.filter(tup2 => tup1._1 == tup2._1)
            if (tmp.isEmpty) (tup1._1, (tup1._2, None)) else (tup1._1, (tup1._2, tmp.apply(0)._2))
        }.sortBy(-_._1).map { f => if (None != f._2._2) _merge(f._2._2.asInstanceOf[ItemUsageMetrics], f._2._1) else f._2._1 }
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
		val temp_top5_incorrect_res = (fact1.m_top5_incorrect_res.getOrElse(List()) ++ fact2.m_top5_incorrect_res.getOrElse(List())).groupBy(f => (f.resp, f.mmc)).map(f => InCorrectRes(f._1._1, f._1._2, f._2.map(x => x.count).sum)).toList
		val m_top5_incorrect_res = temp_top5_incorrect_res.sorted(Ordering.by((_: InCorrectRes).count).reverse).take(5).toList

		val m_avg_ts = if (m_total_count > 0) CommonUtil.roundDouble(m_total_ts/m_total_count, 2) else 0;
  		val m_top5_mmc = (fact1.m_top5_mmc.getOrElse(List()) ++ fact2.m_top5_mmc.getOrElse(List())).groupBy(f => f.concept).mapValues(f => f.map(x => x.count).sum).toList
		                 .sorted(Ordering.by((_: (String, Int))._2).reverse).take(5).map { x => Misconception(x._1, x._2) }.toList;
  		
		ItemUsageSummary(fact1.d_item_id, None, Option(m_total_ts), Option(m_total_count), Option(m_correct_res_count), Option(m_inc_res_count), Option(m_correct_res), Option(m_top5_incorrect_res), Option(m_avg_ts), Option(m_top5_mmc));
  	}
  	
  	override def getSummary(summary: ItemUsageMetrics) : ItemUsageMetrics = {
  		ItemUsageMetrics(None, None, summary.items);
  	}

	override def getData(contentId: String, tags: Array[String], period: String, channel: String, userId: String = "all", deviceId: String = "all", metricsType: String = "app", mode: String = "")(implicit mf: Manifest[ItemUsageSummaryView], config: Config): Array[ItemUsageSummaryView] = {

		val periods = _getPeriods(period);

		val queries = tags.map { tag =>
			periods.map { p =>
				QueryBuilder.select().all().from(Constants.CONTENT_DB, Constants.ITEM_USAGE_SUMMARY_FACT).allowFiltering().where(QueryBuilder.eq("d_period", p)).and(QueryBuilder.eq("d_tag", tag)).and(QueryBuilder.eq("d_content_id", contentId)).and(QueryBuilder.eq("d_channel", channel)).toString();
			}
		}.flatMap(x => x)

		queries.map { q =>
			val res = DBUtil.session.execute(q)
			res.all().asScala.map(x => x.as[ItemUsageTable])
		}.flatMap(x => x).map(f => getSummaryFromCass(f))
	}

	private def getSummaryFromCass(summary: ItemUsageTable): ItemUsageSummaryView = {
		ItemUsageSummaryView(Option(summary.d_period), summary.d_content_id, summary.d_tag, summary.d_item_id, summary.m_total_ts, summary.m_total_count, summary.m_correct_res_count, summary.m_inc_res_count, summary.m_correct_res, summary.m_top5_incorrect_res, summary.m_avg_ts, summary.m_top5_mmc);
	}
  	
}