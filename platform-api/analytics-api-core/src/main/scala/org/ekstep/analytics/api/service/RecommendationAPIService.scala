package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.api._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.util.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import scala.util.control.Breaks
import org.ekstep.analytics.api.exception.ClientException
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.ContentCacheUtil
import akka.actor.Props
import akka.actor.Actor

/**
 * @author mahesh
 */

object RecommendationAPIService {

	def props = Props[RecommendationAPIService];
	case class RecommendRequest(requestBody: String, sc: SparkContext, config: Config);

	def recommendations(requestBody: String)(implicit sc: SparkContext, config: Config): String = {
//	  println("config : " , config.getString("service.search.limit"))
		ContentCacheUtil.validateCache()(sc, config);
		val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
		val contentId = reqBody.request.context.getOrElse(Map()).getOrElse("contentid", "").asInstanceOf[String];
		val context = reqBody.request.context.getOrElse(Map());
		val did = context.getOrElse("did", "").asInstanceOf[String];
		val uid = context.getOrElse("uid", "").asInstanceOf[String];
		val dlang = context.getOrElse("dlang", "").asInstanceOf[String];
		val limit = if (config.getBoolean("recommendation.enable")) reqBody.request.limit.getOrElse(config.getInt("service.search.limit"));
		else config.getInt("service.search.limit");

		// If contentID available in request return content recommendations otherwise, device recommendations. 
		if (StringUtils.isEmpty(contentId)) {
			if (context.isEmpty || StringUtils.isBlank(did) || StringUtils.isBlank(dlang)) {
				CommonUtil.errorResponseSerialized(APIIds.RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString())
			} else {
				val langName = ContentCacheUtil.getLanguageByCode(dlang);
				if (StringUtils.isEmpty(langName)) {
					CommonUtil.errorResponseSerialized(APIIds.RECOMMENDATIONS, "dlang should be a language code.", ResponseCode.CLIENT_ERROR.toString())
				} else {
					deviceRecommendations(langName, did, limit);
				}
			}
		} else {
			if (context.isEmpty || StringUtils.isBlank(did)) {
				CommonUtil.errorResponseSerialized(APIIds.RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString())
			} else {
				contentRecommendations(contentId, limit);

			}
		}
	}

	private def deviceRecommendations(langName: String, did: String, limit: Int)(implicit sc: SparkContext, config: Config): String = {
		println("config : " , config.getString("service.search.limit"))
	  val filters: Array[(String, List[String], String)] = Array(("language", List(langName), "LIST"));
		val deviceRecos = sc.cassandraTable[(List[(String, Double)])](Constants.DEVICE_DB, Constants.DEVICE_RECOS_TABLE).select("scores").where("device_id = ?", did);
		getRecommendedContent(deviceRecos, filters, limit, false)
	}

	private def contentRecommendations(contentId: String, limit: Int)(implicit sc: SparkContext, config: Config): String = {
		val content: Map[String, AnyRef] = ContentCacheUtil.getREList.getOrElse(contentId, Map());
		if (content.isEmpty) {
			println("Given content is not live ContentID:" + contentId);
			JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> List(), "count" -> Int.box(0))));
		} else {
			val contentLanguage = getValueAsList(content, "language")
			if (contentLanguage.isEmpty) {
				println("Language value is empty for ContentID:" + contentId);
				JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> List(), "count" -> Int.box(0))));
			} else {
				val contentFilters: Array[(String, List[String], String)] = Array(("language", contentLanguage, "LIST"));
				val contentRecos = sc.cassandraTable[(List[(String, Double)])](Constants.CONTENT_DB, Constants.CONTENT_RECOS_TABLE).select("scores").where("content_id = ?", contentId);
				getRecommendedContent(contentRecos, contentFilters, limit, true)
			}

		}
	}

	private def getRecommendedContent(records: RDD[List[(String, Double)]], filters: Array[(String, List[String], String)], limit: Int, isContentReco: Boolean)(implicit sc: SparkContext, config: Config): String = {
		val result = if (records.count() > 0) {
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

		val index = config.getInt("recommendation.index")
		val serendipityFactor = config.getDouble("recommendation.serendipity_factor")
		val serendipityContentLimit = Math.ceil((limit * serendipityFactor)/100.0).toInt 
		val recoContentLimit = limit - serendipityContentLimit
		val surpriseContentIndex = index + recoContentLimit - 1
		val surpriseContentLimit = surpriseContentIndex + serendipityContentLimit
		// if recommendations is disabled then, always take limit from config otherwise user input.
		val respContent = if(isContentReco) {
		  if(result.size <= surpriseContentLimit) result.take(limit) else result.take(recoContentLimit).union(result.slice(surpriseContentIndex, surpriseContentLimit))
		} else {
		  result.take(limit)
		}
		JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> respContent, "count" -> Int.box(result.size))));
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

}

class RecommendationAPIService extends Actor {
	import RecommendationAPIService._;

	def receive = {
		case RecommendRequest(requestBody: String, sc: SparkContext, config: Config) =>
			sender() ! recommendations(requestBody)(sc, config);
	}
}