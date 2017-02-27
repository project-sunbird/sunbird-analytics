package org.ekstep.analytics.api

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.util.ContentCacheUtil
import scala.util.control.Breaks

trait IRecommendations {

	case class Validation(value: Boolean, message: Option[String] = None);
	
	def isValidRequest(requestBody: RequestBody) : Validation;
	
	def applyLimit(contents: List[Map[String, Any]], total: Int, limit: Int)(implicit config: Config) : List[Map[String, Any]]
	
	def fetch(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String;

	def getLimit(requestBody: RequestBody)(implicit config: Config): Int = {
		val default = config.getInt("service.search.limit");
		if (config.getBoolean("recommendation.enable")) requestBody.request.limit.getOrElse(default); else default;
	}
	
	def getRecommendedContent(records: RDD[List[(String, Double)]], filters: Array[(String, List[String], String)]): List[Map[String, Any]] = {
		if (records.count() > 0) {
			records.first.map(f => ContentCacheUtil.getREList.getOrElse(f._1, Map()) ++ Map("reco_score" -> f._2))
				.filter(p => p.get("identifier").isDefined)
				.filter(p => {
					var valid = true;
					Breaks.breakable {
						filters.foreach { filter =>
							valid = recoFilter(p, filter);
							if (!valid) Breaks.break;
						}
					}
					valid;
				});
		} else {
			List();
		}
	}
	
	private def recoFilter(map: Map[String, Any], filter: (String, List[String], String)): Boolean = {
		if ("LIST".equals(filter._3)) {
			val valueList = map.getOrElse(filter._1, List()).asInstanceOf[List[String]];
			filter._2.isEmpty || !filter._2.filter { x => valueList.contains(x) }.isEmpty
		} else {
			val value = map.getOrElse(filter._1, "").asInstanceOf[String];
			filter._2.isEmpty || (!value.isEmpty() && filter._2.contains(value));
		}
	}
}
