package org.ekstep.analytics.api.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.framework.util.RestUtil
import org.joda.time.DateTimeZone
import org.joda.time.DateTime
import org.ekstep.analytics.api._

object ContentCacheUtil {
	private var contentBroadcastMap: Broadcast[Map[String, Map[String, AnyRef]]] = null;
	private var cacheTimestamp: Long = 0L;

	def initCache()(implicit sc: SparkContext, config: Config) {
		try {
			val baseUrl = config.getString("service.search.url");
			val searchPath = config.getString("service.search.path");
			val searchUrl = s"$baseUrl$searchPath";
			val request = config.getString("service.search.requestbody");
			val resp = RestUtil.post[Response](searchUrl, request);
			val contentList = resp.result.getOrElse(Map("content" -> List())).getOrElse("content", List()).asInstanceOf[List[Map[String, AnyRef]]];
			val contentMap = contentList.map(f => (f.get("identifier").get.asInstanceOf[String], f)).toMap;
			contentBroadcastMap = sc.broadcast[Map[String, Map[String, AnyRef]]](contentMap);
			cacheTimestamp = DateTime.now(DateTimeZone.UTC).getMillis;
		} catch {
			case ex: Throwable =>
				println("Error at RecommendationAPIService.initCache:" +ex.getMessage);
				contentBroadcastMap = sc.broadcast[Map[String, Map[String, AnyRef]]](Map());
		}
	}

	def validateCache()(implicit sc: SparkContext, config: Config) {

		val timeAtStartOfDay = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis;
		if (cacheTimestamp < timeAtStartOfDay) {
			println("cacheTimestamp:" + cacheTimestamp, "timeAtStartOfDay:" + timeAtStartOfDay, " ### Resetting content cache...### ");
			if (null != contentBroadcastMap) contentBroadcastMap.destroy();
			initCache();
		}
	}
	
	def get() : Map[String, Map[String, AnyRef]] = {
		contentBroadcastMap.value;
	}
}