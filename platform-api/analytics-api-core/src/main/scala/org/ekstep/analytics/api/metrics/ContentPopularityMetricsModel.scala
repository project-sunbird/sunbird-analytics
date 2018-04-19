package org.ekstep.analytics.api.metrics

import org.ekstep.analytics.api.IMetricsModel
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.Rating
import org.ekstep.analytics.api.{ContentPopularityMetrics, Constants}
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.ekstep.analytics.api.util.{CommonUtil, DBUtil}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import com.weather.scalacass.syntax._

case class ContentPopularityTable(d_period: Int, m_comments: Option[List[(String, Long)]] = None, m_downloads: Option[Long] = Option(0), m_side_loads: Option[Long] = Option(0), m_ratings: Option[List[(Double, Long)]] = Option(List()), m_avg_rating: Option[Double] = Option(0.0))

object ContentPopularityMetricsModel extends IMetricsModel[ContentPopularityMetrics, ContentPopularityMetrics]  with Serializable {
	override def metric : String = "cps";
	
	override def getMetrics(records: Array[ContentPopularityMetrics], period: String, fields: Array[String] = Array())(implicit config: Config): Array[ContentPopularityMetrics] = {
	    val periodEnum = periodMap.get(period).get._1;
		val periods = _getPeriods(period);
		val addComments = returnComments(fields);
		val recordsArray = records.map { x => (x.d_period.get, x) };
		val periodsArray = periods.map { period => 
			if(addComments) 
				(period, ContentPopularityMetrics(Option(period), Option(CommonUtil.getPeriodLabel(periodEnum, period)), Option(List())))
			else
				(period, ContentPopularityMetrics(Option(period), Option(CommonUtil.getPeriodLabel(periodEnum, period))))
			};
		periodsArray.map { tup1 =>
            val tmp = recordsArray.filter(tup2 => tup1._1 == tup2._1)
            if (tmp.isEmpty) (tup1._1, (tup1._2, None)) else (tup1._1, (tup1._2, tmp.apply(0)._2))
        }.sortBy(-_._1).map { f => if (None != f._2._2) _merge(f._2._2.asInstanceOf[ContentPopularityMetrics], f._2._1, addComments) else f._2._1 }
	}

	private def _merge(obj: ContentPopularityMetrics, dummy: ContentPopularityMetrics, addComments: Boolean): ContentPopularityMetrics = {
        
	    val m_ratings = obj.m_ratings.getOrElse(List()).map{x=> (x._1, x._2)}
	    if (addComments)
        	ContentPopularityMetrics(dummy.d_period, dummy.label, obj.m_comments, obj.m_downloads, obj.m_side_loads, Option(m_ratings), obj.m_avg_rating)
        else
        	ContentPopularityMetrics(dummy.d_period, dummy.label, None, obj.m_downloads, obj.m_side_loads, Option(m_ratings), obj.m_avg_rating)
    }
	
	private def returnComments(fields: Array[String] = Array()) : Boolean = {
		fields.contains("m_comments");
	}
	
	override def reduce(fact1: ContentPopularityMetrics, fact2: ContentPopularityMetrics, fields: Array[String] = Array()): ContentPopularityMetrics = {
		val m_downloads = fact2.m_downloads.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_downloads.getOrElse(0l).asInstanceOf[Number].longValue();
		val m_side_loads = fact2.m_side_loads.getOrElse(0l).asInstanceOf[Number].longValue() + fact1.m_side_loads.getOrElse(0l).asInstanceOf[Number].longValue();
		val m_ratings = (fact2.m_ratings.getOrElse(List()) ++ fact1.m_ratings.getOrElse(List())).distinct;
		val m_avg_rating = if (m_ratings.length > 0) {
			val total_rating = m_ratings.map(_._1).sum;
			if (total_rating > 0) CommonUtil.roundDouble(total_rating/m_ratings.length, 2) else 0.0;
		} else 0.0;
		if(returnComments(fields)) {
			val m_comments = (fact2.m_comments.getOrElse(List()) ++ fact1.m_comments.getOrElse(List())).distinct;
			ContentPopularityMetrics(fact1.d_period, None, Option(m_comments), Option(m_downloads), Option(m_side_loads), Option(m_ratings), Option(m_avg_rating));
		} else {
			ContentPopularityMetrics(fact1.d_period, None, None, Option(m_downloads), Option(m_side_loads), Option(m_ratings), Option(m_avg_rating));
		}
	}
	
	override def getSummary(summary: ContentPopularityMetrics) : ContentPopularityMetrics = {
		ContentPopularityMetrics(None, None, summary.m_comments, summary.m_downloads, summary.m_side_loads, summary.m_ratings, summary.m_avg_rating);
	}

	override def getData(contentId: String, tags: Array[String], period: String, channel: String, userId: String = "all", deviceId: String = "all", metricsType: String = "app", mode: String = "")(implicit mf: Manifest[ContentPopularityMetrics], config: Config): Array[ContentPopularityMetrics] = {

			val periods = _getPeriods(period);

			val queries = tags.map { tag =>
					periods.map { p =>
							QueryBuilder.select().all().from(Constants.CONTENT_DB, Constants.CONTENT_POPULARITY_SUMMARY_FACT).allowFiltering().where(QueryBuilder.eq("d_period", p)).and(QueryBuilder.eq("d_tag", tag)).and(QueryBuilder.eq("d_content_id", contentId)).and(QueryBuilder.eq("d_channel", channel)).toString();
					}
			}.flatMap(x => x)

			queries.map { q =>
					val res = DBUtil.session.execute(q)
					res.all().asScala.map(x => x.as[ContentPopularityTable])
			}.flatMap(x => x).map(f => getSummaryFromCass(f))
	}

	private def getSummaryFromCass(summary: ContentPopularityTable): ContentPopularityMetrics = {
			ContentPopularityMetrics(Option(summary.d_period), None, summary.m_comments, summary.m_downloads, summary.m_side_loads, summary.m_ratings, summary.m_avg_rating);
	}
}