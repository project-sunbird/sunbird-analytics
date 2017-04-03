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
import org.ekstep.analytics.api.recommend._

/**
 * @author mahesh
 */

object RecommendationAPIService {

	val DEVICE_RECO = "Device";
	val CONTENT_RECO = "Content";
	
	def props = Props[RecommendationAPIService];
	case class Consumption(requestBody: String, sc: SparkContext, config: Config);
	case class Creation(requestBody: String, sc: SparkContext, config: Config);
	
	def consumptionRecommendations(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
		ContentCacheUtil.validateCache()(sc, config);
		if (hasRequired(requestBody, "CONSUMPTION")) {
			val recoType = 	recommendType(requestBody);
			if (StringUtils.equals(DEVICE_RECO, recoType)) 
				DeviceRecommendations.fetch(requestBody);
			else
				ContentRecommendations.fetch(requestBody);
		} else {
			CommonUtil.errorResponseSerialized(APIIds.RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString());
		}
	}
	
	def creationRecommendations(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
		if (hasRequired(requestBody, "CREATION")) {
			CreationRecommendations.fetch(requestBody);
		} else {
			CommonUtil.errorResponseSerialized(APIIds.CREATION_RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString());
		}
	}

	private def hasRequired(requestBody: RequestBody, action: String): Boolean = {
		val context = requestBody.request.context.getOrElse(Map());
		if (StringUtils.equals("CONSUMPTION", action)) {
			val did = context.get("did");
			val dlang = context.get("dlang");
			if (did.isEmpty || dlang.isEmpty) false else true;
		} else if (StringUtils.equals("CREATION", action)) {
			val uid = context.get("uid");
			if (uid.isEmpty) false else true;
		} else false;
	}
	
	private def recommendType(requestBody: RequestBody) : String = {
		val contentId = requestBody.request.context.getOrElse(Map()).getOrElse("contentid", "").asInstanceOf[String]
		if (StringUtils.isEmpty(contentId)) DEVICE_RECO else CONTENT_RECO;
	}
}

class RecommendationAPIService extends Actor {
	import RecommendationAPIService._;

	def receive = {
		case Consumption(requestBody: String, sc: SparkContext, config: Config) =>
			sender() ! consumptionRecommendations(JSONUtils.deserialize[RequestBody](requestBody))(sc, config);
			
		case Creation(requestBody: String, sc: SparkContext, config: Config) =>
			sender() ! creationRecommendations(JSONUtils.deserialize[RequestBody](requestBody))(sc, config);
	}
}