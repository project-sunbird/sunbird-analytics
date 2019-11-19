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
import javax.inject.Inject
import scalikejdbc._
import javax.inject._

import scala.util.Try

case class ContentResult(count: Int, content: Array[Map[String, AnyRef]])

case class ContentResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: ContentResult)

case class LanguageResult(languages: Array[Map[String, AnyRef]])

case class LanguageResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: LanguageResult)

// TODO: Need to refactor this file. Reduce case classes, combine objects. Proper error handling.

@Singleton
class CacheUtil @Inject()(postgresDB: PostgresDBUtil, H2DB: H2DBUtil) {

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
    lazy val tableName = config.getString("postgres.table_name")

    val sql = s"select * from $tableName"
    Try {
      postgresDB.read(sql).map {
        consumerChannel =>
          consumerChannelTable.put(consumerChannel.consumerId, consumerChannel.channel, consumerChannel.status)
      }
      APILogger.log("ChannelConsumer Cache Refreshed Successfully!!")
    }.recover {
      case ex: Throwable =>
        APILogger.log(s"Failed to refresh ChannelConsumer Cache: ${ex.getMessage}")
        ex.printStackTrace()
    }
  }

  def initDeviceLocationCache()(implicit config: Config) {

    APILogger.log("Refreshing DeviceLocation Cache")
    val geoLocationCityTableName: String = config.getString("postgres.table.geo_location_city.name")
    val geoLocationCityIpv4TableName: String = config.getString("postgres.table.geo_location_city_ipv4.name")

    val truncateCityTableQuery = s"TRUNCATE TABLE $geoLocationCityTableName;"
    val truncateRangeTableQuery = s"TRUNCATE TABLE $geoLocationCityIpv4TableName;"
    val createCityTableQuery = s"CREATE TABLE IF NOT EXISTS $geoLocationCityTableName(geoname_id INTEGER UNIQUE, subdivision_1_name VARCHAR(100), subdivision_2_custom_name VARCHAR(100));"
    val createRangeTableQuery = s"CREATE TABLE IF NOT EXISTS $geoLocationCityIpv4TableName(network_start_integer BIGINT, network_last_integer BIGINT, geoname_id INTEGER);"

    H2DB.executeQuery(createCityTableQuery)
    H2DB.executeQuery(createRangeTableQuery)
    H2DB.executeQuery(truncateCityTableQuery)
    H2DB.executeQuery(truncateRangeTableQuery)

    val cityQuery = s"select geoname_id,subdivision_1_name,subdivision_2_custom_name from $geoLocationCityTableName"
    val rangeQuery = s"select network_start_integer, network_last_integer, geoname_id from $geoLocationCityIpv4TableName"
    Try {
      val locCityData = postgresDB.readGeoLocationCity(cityQuery)
      locCityData.map{
        loc =>
          val insertQuery = s"INSERT INTO $geoLocationCityTableName(geoname_id, subdivision_1_name, subdivision_2_custom_name) VALUES (${loc.geoname_id}, '${loc.subdivision_1_name}', '${loc.subdivision_2_custom_name}')"
          H2DB.executeQuery(insertQuery)
      }

      val locRangeData = postgresDB.readGeoLocationRange(rangeQuery)
      locRangeData.map{
        loc =>
          val insertQuery = s"INSERT INTO $geoLocationCityIpv4TableName(network_start_integer, network_last_integer, geoname_id) VALUES (${loc.network_start_integer}, ${loc.network_last_integer}, ${loc.geoname_id})"
          H2DB.executeQuery(insertQuery)
      }

      // checking row counts in h2 database after refreshing
      val countCityTableQuery = s"Select count(*) AS count from $geoLocationCityTableName"
      val cityTableCount = H2DB.execute(countCityTableQuery)
      var h2CityTableCount = 0L
      while (cityTableCount.next()) {
        h2CityTableCount = cityTableCount.getLong("count")
      }

      val countRangeTableQuery = s"Select count(*) AS count from $geoLocationCityIpv4TableName"
      val rangeTableCount = H2DB.execute(countRangeTableQuery)
      var h2RangeTableCount = 0L
      while (rangeTableCount.next()) {
        h2RangeTableCount = rangeTableCount.getLong("count")
      }

      println("h2 db city table count after refreshing: " + h2CityTableCount)
      println("h2 db city table count after refreshing: " + h2RangeTableCount)
      APILogger.log(s"DeviceLocation Cache Refreshed Successfully!! postgress city table records: ${locCityData.length}, postgress range table records: ${locRangeData.length}, h2 db city table records: $h2CityTableCount, h2 db range table records: $h2RangeTableCount")
    }.recover {
      case ex: Throwable =>
        APILogger.log(s"Failed to refresh DeviceLocation Cache: ${ex.getMessage}")
        ex.printStackTrace()
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

  //    private def getLanguages()(implicit config: Config): Array[Map[String, AnyRef]] = {
  //        val baseUrl = config.getString("content2vec.content_service_url");
  //        val languagePath = "/v1/language"
  //        val languageUrl = s"$baseUrl$languagePath";
  //        try {
  //            val resp = RestUtil.get[LanguageResponse](languageUrl)
  //            resp.result.languages
  //        } catch {
  //            case t: Throwable =>
  //                val res: Map[String, AnyRef] = Map("identifier" -> "lang_en", "code" -> "en", "isoCode" -> "en", "consumerId" -> "f6878ac4-e9c9-4bc4-80be-298c5a73b447", "appId" -> "dev.ekstep.in", "channel" -> "in.ekstep", "words" -> Int.box(93583), "name" -> "English", "lastUpdatedOn" -> "2017-06-21T07->07->39.857+0000", "liveWords" -> Int.box(-517411), "versionKey" -> "1498028859857", "status" -> "Live")
  //                return Array(res)
  //        }
  //
  //    }

  private def getLanguages()(implicit config: Config): Array[Map[String, AnyRef]] = {
    val res: Array[Map[String, AnyRef]] = Array(Map("identifier" -> "lang_en", "code" -> "en", "isoCode" -> "en", "consumerId" -> "f6878ac4-e9c9-4bc4-80be-298c5a73b447", "appId" -> "dev.ekstep.in", "channel" -> "in.ekstep", "words" -> Int.box(93583), "name" -> "English", "lastUpdatedOn" -> "2017-06-21T07->07->39.857+0000", "liveWords" -> Int.box(-517411), "versionKey" -> "1498028859857", "status" -> "Live"))
    return res
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
    val requestStr = config.getString("service.search.requestbody");
    val request = if (requestStr != null) requestStr else JSONUtils.serialize(Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Resource"), "status" -> List("Live")), "exists" -> List("downloadUrl"), "offset" -> offset, "limit" -> limit)))
    val resp = RestUtil.post[ContentResponse](searchUrl, request);
    resp.result;
  }
}

case class ConsumerChannel(consumerId: String, channel: String, status: Int, createdBy: String, createdOn: Timestamp, updatedOn: Timestamp)

object ConsumerChannel extends SQLSyntaxSupport[ConsumerChannel] {
  def apply(rs: WrappedResultSet) = new ConsumerChannel(
    rs.string("consumer_id"), rs.string("channel"), rs.int("status"),
    rs.string("created_by"), rs.timestamp("created_on"), rs.timestamp("updated_on"))
}
