package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.util.ContentPopularitySummaryFact
import org.ekstep.analytics.util.ContentSummaryIndex
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import org.joda.time.DateTime
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Period._
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.CommonUtil

case class ContentPopularitySummaryFact_T(d_period: Int, d_content_id: String, d_tag: String, m_downloads: Long, m_side_loads: Long, m_comments: List[(String, DateTime)], m_ratings: List[(Double, DateTime)], m_avg_rating: Double)  extends AlgoOutput

object UpdateContentPopularityDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ContentPopularitySummaryFact, ContentSummaryIndex] with Serializable {

	val className = "org.ekstep.analytics.updater.UpdateContentPopularityDB"
    override def name: String = "UpdateContentPopularityDB"
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_CONTENT_POPULARITY_SUMMARY")));
    }
	
	override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentPopularitySummaryFact] = {

        val contentSummary = data.map { x =>

            val period = x.dimensions.period.get;
            val contentId = x.dimensions.content_id.get;
            val tag = x.dimensions.tag.get;

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val publish_date = new DateTime(x.context.date_range.from)
            val m_downloads = eksMap.get("m_downloads").getOrElse(0l).asInstanceOf[Number].longValue
            val m_side_loads = eksMap.get("m_side_loads").getOrElse(0l).asInstanceOf[Number].longValue
            val m_comments = eksMap.get("m_comments").get.asInstanceOf[List[(String, DateTime)]]
            val m_rating = eksMap.get("m_rating").get.asInstanceOf[List[(Double, DateTime)]]
            val m_avg_rating = eksMap.get("m_avg_rating").get.asInstanceOf[Double]
            ContentPopularitySummaryFact_T(period, contentId, tag, m_downloads, m_side_loads, m_comments, m_rating, m_avg_rating);
        }.cache();

        // Roll up summaries
        rollup(contentSummary, DAY).union(rollup(contentSummary, WEEK)).union(rollup(contentSummary, MONTH)).union(rollup(contentSummary, CUMULATIVE)).cache();
    }

	override def postProcess(data: RDD[ContentPopularitySummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSummaryIndex] = {
        // Update the database
        data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT)
        data.map { x => ContentSummaryIndex(x.d_period, x.d_content_id, x.d_tag) };
    }
	
	
	private def rollup(data: RDD[ContentPopularitySummaryFact_T], period: Period): RDD[ContentPopularitySummaryFact] = {
		val currentData = data.map { x =>
            val d_period = x.d_period; //CommonUtil.getPeriod(x.m_last_gen_date.getMillis, period);
            (ContentSummaryIndex(d_period, x.d_content_id, x.d_tag), x);
        }.reduceByKey(reduceCPS);
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[ContentPopularitySummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT).on(SomeColumns("d_period", "d_content_id", "d_tag"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(ContentPopularitySummaryFact(index.d_period, index.d_content_id, index.d_tag, newSumm.m_downloads, newSumm.m_side_loads, newSumm.m_comments, newSumm.m_ratings, newSumm.m_avg_rating))
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
	}
	
	private def reduce(fact1: ContentPopularitySummaryFact, fact2: ContentPopularitySummaryFact_T, period: Period): ContentPopularitySummaryFact = {
		val m_downloads = fact2.m_downloads + fact1.m_downloads;
		val m_side_loads = fact2.m_side_loads + fact1.m_side_loads;
		val m_comments = (fact2.m_comments ++ fact1.m_comments).distinct;
		val m_ratings = (fact2.m_ratings ++ fact1.m_ratings).distinct;
		val m_avg_rating = if (false) {
			val total_rating = m_ratings.map { x => x._1 };
			if (total_rating.length > 0) CommonUtil.roundDouble(total_rating.sum/m_ratings.length, 2) else 0.0;
		} else 0.0;
		
        ContentPopularitySummaryFact(fact1.d_period, fact1.d_content_id, fact1.d_tag, m_downloads, m_side_loads, m_comments, m_ratings, m_avg_rating);
    }
	
	private def reduceCPS(fact1: ContentPopularitySummaryFact_T, fact2: ContentPopularitySummaryFact_T): ContentPopularitySummaryFact_T = {
        val m_downloads = fact2.m_downloads + fact1.m_downloads;
		val m_side_loads = fact2.m_side_loads + fact1.m_side_loads;
		val m_comments = (fact2.m_comments ++ fact1.m_comments).distinct;
		val m_ratings = (fact2.m_ratings ++ fact1.m_ratings).distinct;
		val m_avg_rating = if (false) {
			val total_rating = m_ratings.map(_._1);
			if (total_rating.length > 0) CommonUtil.roundDouble(total_rating.sum/m_ratings.length, 2) else 0.0;
		} else 0.0;
		
        ContentPopularitySummaryFact_T(fact1.d_period, fact1.d_content_id, fact1.d_tag, m_downloads, m_side_loads, m_comments, m_ratings, m_avg_rating);
    }
    
}