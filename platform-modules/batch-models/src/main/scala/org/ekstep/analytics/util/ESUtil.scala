package org.ekstep.analytics.util

import org.ekstep.analytics.framework.util.RestUtil
import org.sunbird.cloud.storage.conf.AppConf
trait ESService {
  def createIndex(indexName: String, mapping: String): EsResponse
  def addIndexToAlias(indexName: String, aliasName: String): EsResponse
  def removeIndexFromAlias(indexName: List[String], aliasName: String): EsResponse
  def removeAllIndexFromAlias(aliasName: String): EsResponse
  def listIndexByAlias(aliasName: String): List[Map[String, String]]
  def deleteIndex(index: List[String]): EsResponse
}
case class EsResponse(acknowledged: Boolean, shards_acknowledged: Boolean, index: String, error: Any, status: Any)

object ESUtil extends ESService {
  val elasticSearchURL: String = AppConf.getConfig("es.host") + AppConf.getConfig("es.port")

  def createIndex(indexName: String, mapping: String): EsResponse = {
    val requestURL = elasticSearchURL + "/" + indexName
    RestUtil.put[EsResponse](requestURL, mapping, None)
  }

  def addIndexToAlias(indexName: String, aliasName: String): EsResponse = {
    val requestURL = elasticSearchURL + "/_aliases"
    val request =
      s"""
         |{
         |    "actions" : [
         |        { "add" : { "index" : "$indexName", "alias" : "$aliasName" } }
         |    ]
         |}
    """.stripMargin
    RestUtil.post[EsResponse](requestURL, request, None)
  }

  def removeIndexFromAlias(indexName: List[String], aliasName: String): EsResponse = {
    val requestURL = elasticSearchURL + "/_aliases"
    val request =
      s"""
         |{
         |    "actions" : [
         |        { "remove" : { "index" : "$indexName", "alias" : "$aliasName" } }
         |    ]
         |}
    """.stripMargin
    RestUtil.post[EsResponse](requestURL, request, None)
  }

  def removeAllIndexFromAlias(aliasName: String): EsResponse = {

    val requestURL = elasticSearchURL + "/_aliases"
    val request =
      s"""
         |{
         |    "actions" : [
         |        { "remove" : { "index" : "*", "alias" : "$aliasName" } }
         |    ]
         |}
    """.stripMargin
    RestUtil.post[EsResponse](requestURL, request, None)
  }

  def listIndexByAlias(aliasName: String): List[Map[String, String]] = {
    val requestURL = elasticSearchURL + "/_cat/aliases/" + aliasName + "?format=json&pretty"
    RestUtil.get[List[Map[String, String]]](requestURL)
  }

  def deleteIndex(index: List[String]): EsResponse = {
    val requestURL = elasticSearchURL + "/" + index.mkString(",")
    RestUtil.delete[EsResponse](requestURL)
  }


}
