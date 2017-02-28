package org.ekstep.analytics.api.recommend

import org.ekstep.analytics.api.IRecommendations
import org.ekstep.analytics.api.RequestBody
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.api.util.ContentCacheUtil
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.APIIds
import org.ekstep.analytics.api.Constants

object ContentRecommendations extends IRecommendations {
  
	def isValidRequest(requestBody: RequestBody) : Validation = {
		Validation(true);
	}
	
	def fetch(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
		val context = requestBody.request.context.getOrElse(Map());
		val contentId = context.getOrElse("contentid", "").asInstanceOf[String];
		val content: Map[String, AnyRef] = ContentCacheUtil.getREList.getOrElse(contentId, Map());
		val languages = getValueAsList(content, "language");
		if (content.isEmpty || languages.isEmpty) {
			JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> List(), "count" -> Int.box(0))));
		} else {
			val did = context.getOrElse("did", "").asInstanceOf[String];
			val filters: Array[(String, List[String], String)] = Array(("language", languages, "LIST"));
			val contentRecos = sc.cassandraTable[(List[(String, Double)])](Constants.CONTENT_DB, Constants.CONTENT_RECOS_TABLE).select("scores").where("content_id = ?", contentId);
			val recoContents = getRecommendedContent(contentRecos, filters);
			val result = applyLimit(recoContents, recoContents.size, getLimit(requestBody));
			JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> result, "count" -> Int.box(recoContents.size))));
		}
	}
	
	def applyLimit(contents: List[Map[String, Any]], total: Int, limit: Int)(implicit config: Config) : List[Map[String, Any]] = {
		if (!config.getBoolean("recommendation.surprise_find.enable") || (limit >= total)) {
			contents.take(limit);
		} else {
			val surpriseIndex = config.getInt("recommendation.surprise_find.index");
			val factor = config.getDouble("recommendation.surprise_find.serendipity_factor");
			val surpriseCount = Math.ceil((limit * factor)/100.0).toInt 
			val recoCount = limit - surpriseCount
			val surpriseFindStart = surpriseIndex + recoCount - 1
			val surpriseFindEnd = surpriseFindStart + surpriseCount;
			if ((total <= surpriseFindStart + 1) || (total <= surpriseFindEnd + 1)) {
				contents.take(limit);
			} else {
				contents.take(recoCount).union(contents.slice(surpriseFindStart, surpriseFindEnd));
			}
		}
	}
	
	private def getValueAsList(filter: Map[String, AnyRef], key: String): List[String] = {
		val value = filter.get(key);
		if (value.isEmpty) {
			List();
		} else {
			if (value.get.isInstanceOf[String]) {
				List(value.get.asInstanceOf[String]);
			} else {
				value.get.asInstanceOf[List[String]];
			}
		}
	}
}