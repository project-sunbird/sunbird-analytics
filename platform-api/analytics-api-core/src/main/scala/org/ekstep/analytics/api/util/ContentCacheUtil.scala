package org.ekstep.analytics.api.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.ekstep.analytics.framework.util.RestUtil
import org.joda.time.DateTimeZone
import org.joda.time.DateTime
import org.ekstep.analytics.api._
import scala.annotation.tailrec
import org.apache.commons.lang3.StringUtils

case class ContentResult(count: Int, content: Array[Map[String, AnyRef]]);
case class ContentResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: ContentResult);
case class LanguageResult(languages: Array[Map[String, AnyRef]]);
case class LanguageResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: LanguageResult);

// TODO: Need to refactor this file. Reduce case classes, combine broadcast objects. Proper error handling. 
object ContentCacheUtil {
	private var contentListBroadcastMap: Broadcast[Map[String, Map[String, AnyRef]]] = null;
	private var recommendListBroadcastMap: Broadcast[Map[String, Map[String, AnyRef]]] = null;
	private var languageMap: Broadcast[Map[String, String]] = null;
	private var cacheTimestamp: Long = 0L;

	def initCache()(implicit sc: SparkContext, config: Config) {
		try {
			prepareContentCache();
			prepareLanguageCache();
			cacheTimestamp = DateTime.now(DateTimeZone.UTC).getMillis;
		} catch {
			case ex: Throwable =>
				println("Error at ContentCacheUtil.initCache:" +ex.getMessage);
				contentListBroadcastMap = sc.broadcast[Map[String, Map[String, AnyRef]]](Map());
				recommendListBroadcastMap = sc.broadcast[Map[String, Map[String, AnyRef]]](Map());
				languageMap = sc.broadcast[Map[String, String]](Map());
		}
	}

	def validateCache()(implicit sc: SparkContext, config: Config) {

		val timeAtStartOfDay = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis;
		if (cacheTimestamp < timeAtStartOfDay) {
			println("cacheTimestamp:" + cacheTimestamp, "timeAtStartOfDay:" + timeAtStartOfDay, " ### Resetting content cache...### ");
			if (null != contentListBroadcastMap) contentListBroadcastMap.destroy();
			if (null != recommendListBroadcastMap) recommendListBroadcastMap.destroy();
			if (null != languageMap) languageMap.destroy();
			initCache();
		}
	}
	
	def getContentList() : Map[String, Map[String, AnyRef]] = {
		contentListBroadcastMap.value;
	}
	
	def getREList() : Map[String, Map[String, AnyRef]] = {
		recommendListBroadcastMap.value;
	}
	
	def getLanguageByCode(code: String) : String = {
		if(null != languageMap) {
			languageMap.value.getOrElse(code, "");
		} else {
			"";
		}
	}
	
	private def prepareContentCache()(implicit sc: SparkContext, config: Config) {
		val contentList = getPublishedContent().toList;
		val contentMap = contentList.map(f => (f.get("identifier").get.asInstanceOf[String], f)).toMap;
		contentListBroadcastMap = sc.broadcast[Map[String, Map[String, AnyRef]]](contentMap);
		println("Cached Content List count:", contentListBroadcastMap.value.size);
		prepareREBroadcastMap(contentList)
	}
	
	private def prepareLanguageCache()(implicit sc: SparkContext, config: Config) {
		val langsList = getLanguages().toList
		val langsMap = langsList.filter(f => f.get("isoCode").isDefined).map(f => (f.get("isoCode").get.asInstanceOf[String], f.get("name").get.asInstanceOf[String])).toMap;
		languageMap = sc.broadcast[Map[String, String]](langsMap);
		println("Cached Langauge List count:", languageMap.value.size);
	}
	
	private def prepareREBroadcastMap(contentList:  List[Map[String, AnyRef]])(implicit sc: SparkContext, config: Config) = {
		val contentMap = contentList.filterNot(f => StringUtils.equals(f.getOrElse("visibility", "").asInstanceOf[String], "Parent"))
			.map(f => (f.get("identifier").get.asInstanceOf[String], f)).toMap
		recommendListBroadcastMap = sc.broadcast[Map[String, Map[String, AnyRef]]](contentMap);
		println("Cached RE List count:", recommendListBroadcastMap.value.size);
	}
	
	private def getLanguages()(implicit config: Config): Array[Map[String, AnyRef]] = {
		val baseUrl = config.getString("content2vec.content_service_url");
		val languagePath = "/v1/language"
		val languageUrl = s"$baseUrl$languagePath";
		val resp = RestUtil.get[LanguageResponse](languageUrl);
		resp.result.languages;
	}
	
	private def getPublishedContent()(implicit config: Config): Array[Map[String, AnyRef]] = {

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
        val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "exists" -> List("downloadUrl"), "offset" -> offset, "limit" -> limit));
        val resp = RestUtil.post[ContentResponse](searchUrl, JSONUtils.serialize(request));
        resp.result;
    }
}