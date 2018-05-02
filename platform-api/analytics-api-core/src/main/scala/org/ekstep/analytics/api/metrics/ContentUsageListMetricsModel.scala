package org.ekstep.analytics.api.metrics

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.ekstep.analytics.api.{Constants, ContentUsageListMetrics, IMetricsModel}
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.{CommonUtil, ContentCacheUtil, DBUtil}
import org.ekstep.analytics.api.service.RecommendationAPIService
import org.ekstep.analytics.framework.util.JSONUtils
import com.weather.scalacass.syntax._
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

case class ContentUsageListTable(d_period: Int, m_contents: Option[List[String]] = Option(List()))

object ContentUsageListMetricsModel  extends IMetricsModel[ContentUsageListMetrics, ContentUsageListMetrics]  with Serializable {
	
	override def metric : String = "gls"; // Because content list is part of GLS.
	
	override def preProcess()(implicit config: Config) = {
		ContentCacheUtil.validateCache()(config);
	}
	
	override def getMetrics(records: Array[ContentUsageListMetrics], period: String, fields: Array[String] = Array())(implicit config: Config): Array[ContentUsageListMetrics] = {
		val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val recordsArray = records.map { x => (x.d_period.get, x) };
		val periodsArray = periods.map { period => (period, ContentUsageListMetrics(Option(period))) }		
		val data = periodsArray.map { tup1 =>
            val tmp = recordsArray.filter(tup2 => tup1._1 == tup2._1)
            if (tmp.isEmpty) (tup1._1, (tup1._2, None)) else (tup1._1, (tup1._2, tmp.apply(0)._2))
        }		
		data.sortBy(-_._1).map { f =>
			if(None != f._2._2) f._2._2 else f._2._1 
		}.map { x => 
		    val temp = x.asInstanceOf[ContentUsageListMetrics]
			val label = Option(CommonUtil.getPeriodLabel(periodEnum, temp.d_period.get));
			val contents = for(id <- temp.m_contents.getOrElse(List())) yield {
				ContentCacheUtil.getContentList.getOrElse(id.toString, Map())
			}
			ContentUsageListMetrics(temp.d_period, label, temp.m_contents, Option(contents.filter(f => !f.isEmpty)));
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

	override def getData(contentId: String, tags: Array[String], period: String, channel: String, userId: String = "all", deviceId: String = "all", metricsType: String = "app", mode: String = "")(implicit mf: Manifest[ContentUsageListMetrics], config: Config): Array[ContentUsageListMetrics] = {

		val periods = _getPeriods(period);

		val queries = tags.map { tag =>
			periods.map { p =>
				QueryBuilder.select().all().from(Constants.CONTENT_DB, Constants.GENIE_LAUNCH_SUMMARY_FACT).allowFiltering().where(QueryBuilder.eq("d_period", p)).and(QueryBuilder.eq("d_tag", tag)).and(QueryBuilder.eq("d_channel", channel)).toString();
			}
		}.flatMap(x => x)

		queries.map { q =>
			val res = DBUtil.session.execute(q)
			res.all().asScala.map(x => x.as[ContentUsageListTable])
		}.flatMap(x => x).map(f => getSummaryFromCass(f))
	}

	private def getSummaryFromCass(summary: ContentUsageListTable): ContentUsageListMetrics = {
		ContentUsageListMetrics(Option(summary.d_period), None, summary.m_contents.asInstanceOf[Option[List[AnyRef]]], None)
	}
}