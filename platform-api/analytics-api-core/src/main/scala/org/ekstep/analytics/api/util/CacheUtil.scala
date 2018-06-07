package org.ekstep.analytics.api.util

import java.sql.Timestamp

import com.typesafe.config.Config
import org.ekstep.analytics.framework.util.RestUtil
import org.joda.time.DateTimeZone
import org.joda.time.DateTime
import org.ekstep.analytics.api._

import scala.annotation.tailrec
import org.apache.commons.lang3.StringUtils
import com.google.common.collect._
import scalikejdbc._

import scala.util.Try

case class ContentResult(count: Int, content: Array[Map[String, AnyRef]])
case class ContentResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: ContentResult)
case class LanguageResult(languages: Array[Map[String, AnyRef]])
case class LanguageResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: LanguageResult)

// TODO: Need to refactor this file. Reduce case classes, combine objects. Proper error handling. 
object CacheUtil {

    implicit val className = "org.ekstep.analytics.api.util.CacheUtil"

    private var contentListMap: Map[String, Map[String, AnyRef]] = Map();
    private var recommendListMap: Map[String, Map[String, AnyRef]] = Map();
    private var languageMap: Map[String, String] = Map();
    private var cacheTimestamp: Long = 0L;
    private var consumerChannelTable: Table[String, String, Integer] = HashBasedTable.create();

    def initCache()(implicit config: Config) {
        Try {
            prepareContentCache()
            prepareLanguageCache()
            cacheTimestamp = DateTime.now(DateTimeZone.UTC).getMillis
        }.recover {
            case ex: Throwable =>
                println("Error at CacheUtil.initCache:" + ex.getMessage)
                contentListMap = Map()
                recommendListMap = Map()
                languageMap = Map()
        }
    }

    def initConsumerChannelCache()(implicit config: Config) {

        APILogger.log("Refreshing ChannelConsumer Cache")
        consumerChannelTable.clear()
        val sql = "select * from consumer_channel_mapping"
        Try {
            PostgresDBUtil.read(sql).map {
                consumerChannel =>
                    consumerChannelTable.put(consumerChannel.consumerId, consumerChannel.channel, consumerChannel.status)
            }
            APILogger.log("ChannelConsumer Cache Refreshed Successfully!!")
        }.recover {
            case ex: Throwable =>
                APILogger.log(s"Failed to refresh ChannelConsumer Cache: ${ex.getMessage}")
        }
    }

    def getConsumerChannlTable()(implicit config: Config): Table[String, String, Integer] = {
        if (consumerChannelTable.size() > 0)
            consumerChannelTable
        else {
            initConsumerChannelCache()
            consumerChannelTable
        }
    }
    def validateCache()(implicit config: Config) {

        val timeAtStartOfDay = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis;
        if (cacheTimestamp < timeAtStartOfDay) {
            println("cacheTimestamp:" + cacheTimestamp, "timeAtStartOfDay:" + timeAtStartOfDay, " ### Resetting content cache...### ");
            if (!contentListMap.isEmpty) contentListMap.empty;
            if (!recommendListMap.isEmpty) recommendListMap.empty;
            if (!languageMap.isEmpty) languageMap.empty;
            initCache();
        }
    }

    def getContentList(): Map[String, Map[String, AnyRef]] = {
        contentListMap;
    }

    def getREList(): Map[String, Map[String, AnyRef]] = {
        recommendListMap;
    }

    def getLanguageByCode(code: String): String = {
        if (!languageMap.isEmpty) {
            languageMap.getOrElse(code, "");
        } else {
            "";
        }
    }

    private def prepareContentCache()(implicit config: Config) {
        val contentList = getPublishedContent().toList;
        val contentMap = contentList.map(f => (f.get("identifier").get.asInstanceOf[String], f)).toMap;
        contentListMap = contentMap;
        println("Cached Content List count:", contentListMap.size);
        prepareREMap(contentList)
    }

    private def prepareLanguageCache()(implicit config: Config) {
        val langsList = getLanguages().toList
        val langsMap = langsList.filter(f => f.get("isoCode").isDefined).map(f => (f.get("isoCode").get.asInstanceOf[String], f.get("name").get.asInstanceOf[String])).toMap;
        languageMap = langsMap;
        println("Cached Langauge List count:", languageMap.size);
    }

    private def prepareREMap(contentList: List[Map[String, AnyRef]])(implicit config: Config) = {
        val contentMap = contentList.filterNot(f => StringUtils.equals(f.getOrElse("visibility", "").asInstanceOf[String], "Parent"))
            .map(f => (f.get("identifier").get.asInstanceOf[String], f)).toMap
        recommendListMap = contentMap;
        println("Cached RE List count:", recommendListMap.size);
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

    def _search(offset: Int, limit: Int)(implicit config: Config): ContentResult = {
        val baseUrl = config.getString("service.search.url");
        val searchPath = config.getString("service.search.path");
        val searchUrl = s"$baseUrl$searchPath";
        val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "exists" -> List("downloadUrl"), "offset" -> offset, "limit" -> limit));
        val resp = RestUtil.post[ContentResponse](searchUrl, JSONUtils.serialize(request));
        resp.result;
    }
}

case class ConsumerChannel(consumerId: String, channel: String, status: Int, createdBy: String, createdOn: Timestamp, updatedOn: Timestamp)

object ConsumerChannel extends SQLSyntaxSupport[ConsumerChannel] {
    def apply(rs: WrappedResultSet) = new ConsumerChannel(
        rs.string("consumer_id"), rs.string("channel"), rs.int("status"),
        rs.string("created_by"), rs.timestamp("created_on"), rs.timestamp("updated_on")
    )
}
