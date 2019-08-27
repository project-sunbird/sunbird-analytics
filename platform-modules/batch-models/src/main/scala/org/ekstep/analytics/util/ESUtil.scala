package org.ekstep.analytics.util

import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.{JobLogger, RestUtil}
import org.ekstep.analytics.job.ESIndexResponse
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable.ListBuffer

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
  val elasticSearchURL: String = AppConf.getConfig("es.host") + ":" + AppConf.getConfig("es.port")
  implicit val className = "org.ekstep.analytics.util.ESUtil"

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

  def getIndexName(aliasName: String): List[String] = {
    val indexMap = listIndexByAlias(aliasName)
    var indexListBuffer = new ListBuffer[String]()
    indexMap.foreach(element => {
      indexListBuffer += element.getOrElse("index", null)
    })
    indexListBuffer.toList
  }

  def rolloverIndex(indexName: String, aliasName: String): ESIndexResponse = {
    val olderIndexList = getIndexName(aliasName)
    val addIndexToAliasResponse = addIndexToAlias(indexName, aliasName)
    if (addIndexToAliasResponse.acknowledged && olderIndexList.nonEmpty) {
      JobLogger.log("Adding index (" + indexName + ") to alias(" + aliasName + ") is success", None, INFO)
      val deleteIndexResponse = deleteIndex(olderIndexList)
      if (deleteIndexResponse.acknowledged) {
        JobLogger.log("Delete index is success! Index Name: " + olderIndexList, None, INFO)
        ESIndexResponse(deleteIndexResponse.acknowledged, addIndexToAliasResponse.acknowledged)
      } else {
        JobLogger.log("Delete index is failed! Index Name: " + olderIndexList, None, ERROR)
        JobLogger.log(deleteIndexResponse.toString, None, ERROR)
        ESIndexResponse(deleteIndexResponse.acknowledged, addIndexToAliasResponse.acknowledged)
      }
    } else {
      JobLogger.log("Adding alias " + aliasName + " to index " + indexName + " status is: " + addIndexToAliasResponse.acknowledged, None, INFO)
      JobLogger.log(addIndexToAliasResponse.toString, None, INFO)
      ESIndexResponse(false, addIndexToAliasResponse.acknowledged)
    }
  }


}
