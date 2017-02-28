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
	case class RecommendRequest(requestBody: String, sc: SparkContext, config: Config);
	
	def recommendations(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
		ContentCacheUtil.validateCache()(sc, config);
		if (hasRequired(requestBody)) {
			val recoType = 	recommendType(requestBody);
			if (StringUtils.equals(DEVICE_RECO, recoType)) 
				DeviceRecommendations.fetch(requestBody);
			else
				ContentRecommendations.fetch(requestBody);
		} else {
			CommonUtil.errorResponseSerialized(APIIds.RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString());
		}
	}

	private def hasRequired(requestBody: RequestBody): Boolean = {
		val context = requestBody.request.context.getOrElse(Map());
		val did = context.get("did");
		val dlang = context.get("dlang");
		if (did.isEmpty || dlang.isEmpty) false else true;
	}
	
	private def recommendType(requestBody: RequestBody) : String = {
		if (requestBody.request.context.getOrElse(Map()).get("contentid").isEmpty) DEVICE_RECO else CONTENT_RECO;
	}
	

}

class RecommendationAPIService extends Actor {
	import RecommendationAPIService._;

	def receive = {
		case RecommendRequest(requestBody: String, sc: SparkContext, config: Config) =>
			sender() ! recommendations(JSONUtils.deserialize[RequestBody](requestBody))(sc, config);
	}
}