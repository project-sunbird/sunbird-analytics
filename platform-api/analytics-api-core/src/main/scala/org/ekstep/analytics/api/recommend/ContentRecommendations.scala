package org.ekstep.analytics.api.recommend

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.ekstep.analytics.api.APIIds
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.IRecommendations
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.CacheUtil
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.api.util.JSONUtils

import com.typesafe.config.Config
import com.datastax.driver.core.TupleValue
import com.datastax.driver.core.querybuilder.QueryBuilder

object ContentRecommendations extends IRecommendations {
  
	def isValidRequest(requestBody: RequestBody) : Validation = {
		Validation(true);
	}
	
	def fetch(requestBody: RequestBody)(implicit config: Config): String = {
		val context = requestBody.request.context.getOrElse(Map());
		val contentId = context.getOrElse("contentid", "").asInstanceOf[String];
		val content: Map[String, AnyRef] = CacheUtil.getREList.getOrElse(contentId, Map());
		val languages = getValueAsList(content, "language");
		if (content.isEmpty || languages.isEmpty) {
			JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> List(), "count" -> Int.box(0))));
		} else {
			val did = context.getOrElse("did", "").asInstanceOf[String];
			val filters: Array[(String, List[String], String)] = Array(("language", languages, "LIST"));
			
			val query = QueryBuilder.select().all().from(Constants.CONTENT_DB, Constants.CONTENT_RECOS_TABLE).where(QueryBuilder.eq("content_id", QueryBuilder.bindMarker())).toString();
			val ps = DBUtil.session.prepare(query)
			val contentRecosFact = DBUtil.session.execute(ps.bind(contentId))
			val contentRecos = contentRecosFact.asScala.seq.map( row => row.getList("scores", classOf[TupleValue]).asScala.seq.map { f => (f.get(0, classOf[String]), f.get(1, classOf[Double]))  })
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