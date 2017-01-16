package org.ekstep.analytics.api.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.framework.util.RestUtil
import org.joda.time.DateTimeZone
import org.joda.time.DateTime
import org.ekstep.analytics.api._
import scala.annotation.tailrec

case class ContentResult(count: Int, content: Array[Map[String, AnyRef]]);
case class ContentResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: ContentResult);

object ContentCacheUtil {
	private var contentBroadcastMap: Broadcast[Map[String, Map[String, AnyRef]]] = null;
	private var cacheTimestamp: Long = 0L;

	def initCache()(implicit sc: SparkContext, config: Config) {
		try {
			val contentList = getPublishedContent().toList;
			println("Cached content count:", contentList.length);
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
	
	def getPublishedContent()(implicit config: Config): Array[Map[String, AnyRef]] = {

        @tailrec
        def search(offset: Int, limit: Int, contents: Array[Map[String, AnyRef]])(implicit config: Config): Array[Map[String, AnyRef]] = {
            val result = _search(offset, limit);
            val c = contents ++ result.content;
            if (result.count > (offset + limit)) {
                search((offset + limit), limit, c);
            } else {
                c;
            }
        }
        search(0, 200, Array[Map[String, AnyRef]]());
    }

	def _search(offset: Int, limit: Int)(implicit config: Config) : ContentResult = {
		val baseUrl = config.getString("service.search.url");
		val searchPath = config.getString("service.search.path");
		val searchUrl = s"$baseUrl$searchPath";
        val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Draft", "Review", "Redraft", "Flagged", "Live", "Retired", "Mock", "Processing", "FlagDraft", "FlagReview")), "exists" -> List("lastPublishedOn", "downloadUrl"), "offset" -> offset, "limit" -> limit));
        val resp = RestUtil.post[ContentResponse](searchUrl, JSONUtils.serialize(request));
        resp.result;
    }
}