package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.api._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.util.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.RestUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import scala.util.control.Breaks
import org.ekstep.analytics.api.exception.ClientException

/**
 * @author mahesh
 */

object RecommendationAPIService {

	var contentBroadcastMap: Broadcast[Map[String, Map[String, AnyRef]]] = null;
	var cacheTimestamp: Long = 0L;

	def initCache()(implicit sc: SparkContext, config: Map[String, String]) {

		val baseUrl = config.get("service.search.url").get;
		val searchUrl = s"$baseUrl/v2/search";
		val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 1000));
		val resp = RestUtil.post[Response](searchUrl, JSONUtils.serialize(request));
		val contentList = resp.result.getOrElse(Map("content" -> List())).getOrElse("content", List()).asInstanceOf[List[Map[String, AnyRef]]];
		val contentMap = contentList.map(f => (f.get("identifier").get.asInstanceOf[String], f)).toMap;
		contentBroadcastMap = sc.broadcast[Map[String, Map[String, AnyRef]]](contentMap);
		cacheTimestamp = DateTime.now(DateTimeZone.UTC).getMillis;
	}

	def validateCache()(implicit sc: SparkContext, config: Map[String, String]) {

		val timeAtStartOfDay = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis;
		if (cacheTimestamp < timeAtStartOfDay) {
			println("cacheTimestamp:" + cacheTimestamp, "timeAtStartOfDay:" + timeAtStartOfDay, " ### Resetting content cache...### ");
			if (null != contentBroadcastMap) contentBroadcastMap.destroy();
			initCache();
		}
	}

	def recommendations(requestBody: String)(implicit sc: SparkContext, config: Map[String, String]): String = {

		validateCache()(sc, config);
		val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
		val context = reqBody.request.context.getOrElse(Map());
		val did = context.getOrElse("did", "").asInstanceOf[String];
		val dlang = context.getOrElse("dlang", "").asInstanceOf[String];
		val contentId = context.getOrElse("contentId", "").asInstanceOf[String];
		val reqFilters = reqBody.request.filters.getOrElse(Map());

		val filters: Array[(String, List[String], String)] = 
			Array(("language", getValueAsList(reqFilters, "language"), "LIST"),
			("domain", getValueAsList(reqFilters, "subject"), "LIST"),
			("contentType", getValueAsList(reqFilters, "contentType"), "STRING"),
			("gradeLevel", getValueAsList(reqFilters, "gradeLevel"), "LIST"),
			("ageGroup", getValueAsList(reqFilters, "ageGroup"), "LIST"))
			.filter(p => !p._2.isEmpty);
		val limit = reqBody.request.limit.getOrElse(10);

		if (context.isEmpty || StringUtils.isBlank(did) || StringUtils.isBlank(dlang)) {
			throw new ClientException("context required data is missing.");
		}

		val deviceRecos = sc.cassandraTable[(List[(String, Double)])](Constants.DEVICE_DB, Constants.DEVICE_RECOS_TABLE).select("scores").where("device_id = ?", did);
		val recoContent = if (deviceRecos.count() > 0) {
			deviceRecos.first.map(f => contentBroadcastMap.value.getOrElse(f._1, Map()) ++ Map("reco_score" -> f._2))
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
		
		val contentFilters = getContentFilter(contentId);
		val result = if (contentFilters.isEmpty) {
			recoContent;
		} else {
			recoContent.map(p => {
				val filterScoreList = for (filter <- contentFilters) yield {
					val valid = recoFilter(p, filter);
					if (valid) 1 else 0;
				};
				val filterscore = filterScoreList.toList.sum;
				(filterscore, p.getOrElse("reco_score", 0).asInstanceOf[Double], p)
			})
			.sortBy(- _._1)
			.sortBy(- _._2)
			.map(x => x._3);
		}
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.recommendations", Map[String, AnyRef]("content" -> result.take(limit), "count" -> Int.box(result.size))));
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
	
	private def getContentFilter(id: String): Array[(String, List[String], String)] = {
		val content: Map[String, AnyRef] = contentBroadcastMap.value.getOrElse(id, Map());
		if (content.isEmpty) {
			Array();
		} else {
			Array(("language", getValueAsList(content, "language"), "LIST"),
				("domain", getValueAsList(content, "domain"), "LIST"),
				("contentType", getValueAsList(content, "contentType"), "STRING"),
				("gradeLevel", getValueAsList(content, "gradeLevel"), "LIST"),
				("ageGroup", getValueAsList(content, "ageGroup"), "LIST"))
				.filter(p => !p._2.isEmpty);
		}
	}
	
	
}