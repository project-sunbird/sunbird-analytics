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
		ContentCacheUtil.validateCache()(sc, config);
		val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
		val contentId = reqBody.request.context.getOrElse(Map()).getOrElse("contentid", "").asInstanceOf[String];
		// If contentID available in request return content recommendations otherwise, device recommendations. 
		if (StringUtils.isEmpty(contentId)) {
			deviceRecommendations(reqBody);
		} else {
			contentRecommendations(reqBody);
		}
	}

	private def deviceRecommendations(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
		val context = requestBody.request.context.getOrElse(Map());
		val did = context.getOrElse("did", "").asInstanceOf[String];
		val dlang = context.getOrElse("dlang", "").asInstanceOf[String];
		if (context.isEmpty || StringUtils.isBlank(did) || StringUtils.isBlank(dlang)) {
			CommonUtil.errorResponseSerialized(APIIds.RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString())
		} else {
			val langName = ContentCacheUtil.getLanguageByCode(dlang);
			if(StringUtils.isEmpty(langName)) {
				CommonUtil.errorResponseSerialized(APIIds.RECOMMENDATIONS, "dlang should be a language code.", ResponseCode.CLIENT_ERROR.toString()) 
			} else {
				val reqFilters = requestBody.request.filters.getOrElse(Map());
				val filters: Array[(String, List[String], String)] = 
					Array(("language", List(langName), "LIST"),
					("domain", getValueAsList(reqFilters, "subject"), "LIST"),
					("contentType", getValueAsList(reqFilters, "contentType"), "STRING"),
					("gradeLevel", getValueAsList(reqFilters, "gradeLevel"), "LIST"),
					("ageGroup", getValueAsList(reqFilters, "ageGroup"), "LIST"))
					.filter(p => !p._2.isEmpty);
		
				val deviceRecos = sc.cassandraTable[(List[(String, Double)])](Constants.DEVICE_DB, Constants.DEVICE_RECOS_TABLE).select("scores").where("device_id = ?", did);
				val result = if (deviceRecos.count() > 0) {
					deviceRecos.first.map(f => ContentCacheUtil.getREList.getOrElse(f._1, Map()) ++ Map("reco_score" -> f._2))
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
				// if recommendations is disabled then, always take limit from config otherwise user input.
				 val limit =if (config.getBoolean("recommendation.enable")) requestBody.request.limit.getOrElse(config.getInt("service.search.limit")); 
				 			else config.getInt("service.search.limit");
				val respContent =result.take(limit)
				JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> respContent, "count" -> Int.box(result.size))));
			}
		}
	}
	
	private def contentRecommendations(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
		val context = requestBody.request.context.getOrElse(Map());
	  val contentId = context.getOrElse("contentid", "").asInstanceOf[String];
	  //JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> List(), "count" -> Int.box(0))));
	  val contentFilters = getContentFilter(contentId)
	  val contentRecos = sc.cassandraTable[(List[(String, Double)])](Constants.CONTENT_DB, Constants.CONTENT_RECOS_TABLE).select("scores").where("content_id = ?", contentId);
    println("content count : ", contentRecos.count())
	  val contents = if (contentRecos.count() > 0) contentRecos.first.map(f => ContentCacheUtil.getREList.getOrElse(f._1, Map()) ++ Map("reco_score" -> f._2)) else List()
    val result = if (contentFilters.isEmpty) {
      contents;
    } else {
      contents.map(p => {
        val filterScoreList = for (filter <- contentFilters) yield {
          val valid = recoFilter(p, filter);
          if (valid) 1 else 0;
        };
        val filterscore = filterScoreList.toList.sum;
        (filterscore, p.get("reco_score").get.asInstanceOf[Double], p)
      })
      .sortBy(- _._1)
      .sortBy(- _._2)
      .map(x => x._3);
    }
    // if recommendations is disabled then, always take limit from config otherwise user input.
	  val limit =if (config.getBoolean("recommendation.enable")) requestBody.request.limit.getOrElse(config.getInt("service.search.limit")); 
		 		else config.getInt("service.search.limit");
		val respContent =result.take(limit)
	  JSONUtils.serialize(CommonUtil.OK(APIIds.RECOMMENDATIONS, Map[String, AnyRef]("content" -> respContent, "count" -> Int.box(0))));
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
		val content: Map[String, AnyRef] = ContentCacheUtil.getREList.getOrElse(id, Map());
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

class RecommendationAPIService extends Actor {
    import RecommendationAPIService._;

    def receive = {
        case RecommendRequest(requestBody: String, sc: SparkContext, config: Config) => 
        	sender() ! recommendations(requestBody)(sc, config);
    }
}