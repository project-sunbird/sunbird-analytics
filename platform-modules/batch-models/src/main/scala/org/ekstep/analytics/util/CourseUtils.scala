package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.ekstep.analytics.framework.Params
import org.ekstep.analytics.framework.util.{JSONUtils, RestUtil}
import org.sunbird.cloud.storage.conf.AppConf

case class TenantInfo(id: String, slug: String)
case class TenantResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: TenantResult)
case class TenantResult(response: ContentList)
case class ContentList(count: Int, content: List[TenantInfo])
case class ESResponse(took: Double, timed_out: Boolean, _shards: _shards, hits: Hit)
case class _shards(total: Option[Double], successful: Option[Double], skipped: Option[Double], failed: Option[Double])
case class Hit(total: Double, max_score: Double, hits: List[Hits])
case class Hits(_source: _source)
case class _source(batchId: String, courseId: String, status: String, name: String, participantCount: Integer, completedCount: Integer)
case class CourseDetails(result: Result)
case class Result(content: List[CourseInfo])
case class CourseInfo(channel: String, identifier: String, name: String)

object CourseUtils {

  def getLiveCourses(config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CourseInfo] = {
    val apiURL = Constants.COMPOSITE_SEARCH_URL

    val status = JSONUtils.serialize(config.get("status").get.asInstanceOf[List[String]])
    val courseIds = JSONUtils.serialize(config.get("courseIds").get.asInstanceOf[List[String]])

    val request = s"""{
                     | "request": {
                     |        "filters":{
                     |            "objectType": ["Content"],
                     |            "contentType": ["Course"],
                     |            "identifiers": $courseIds,
                     |            "status": $status
                     |        },
                     |        "limit": 10000
                     |    }
                     |}""".stripMargin

    val response = RestUtil.post[CourseDetails](apiURL, request).result.content
    sc.parallelize(response)
  }

  def getCourseBatchDetails()(implicit sc: SparkContext): RDD[_source] = {
    val apiURL = Constants.ELASTIC_SEARCH_SERVICE_ENDPOINT + "/" + Constants.ELASTIC_SEARCH_INDEX_COURSEBATCH_NAME + "/_search"
    val request = s"""{
                     | "query":{
                     |    "match_all":{}
                     |  }
                     |}""".stripMargin
    val ESresponse = RestUtil.post[ESResponse](apiURL, request).hits.hits
    val response = ESresponse.map(f => JSONUtils.deserialize[Hits](JSONUtils.serialize(f))._source)
    sc.parallelize(response)
  }

  def getTenantInfo()(implicit sc: SparkContext):  RDD[TenantInfo] = {
    val url = Constants.ORG_SEARCH_URL + Constants.ORG_SEARCH_PATH
    val body = """{
                 |    "params": { },
                 |    "request":{
                 |        "filters": {
                 |            "isRootOrg": true
                 |        },
                 |        "offset": 0,
                 |        "limit": 1000,
                 |        "fields": ["id", "channel", "slug", "orgName"]
                 |    }
                 |}""".stripMargin
    val header = Option(Map("cache-control" -> "no-cache", "Accept" -> "application/json"))
    sc.parallelize(RestUtil.post[TenantResponse](url, body, header).result.response.content)
  }

  def writeToCSVAndRename(data: DataFrame, config: Map[String, AnyRef])(implicit sc: SparkContext): String = {
    val filePath = config.getOrElse("filePath", AppConf.getConfig("spark_output_temp_dir")).asInstanceOf[String]
    val key = config.getOrElse("key", null).asInstanceOf[String]
    val reportId = config.getOrElse("reportId", "").asInstanceOf[String]
    val fileParameters = config.getOrElse("fileParameters", List("")).asInstanceOf[List[String]]
    var dims = config.getOrElse("folderPrefix", List()).asInstanceOf[List[String]]

    dims = if (fileParameters.nonEmpty && fileParameters.contains("date")) dims else dims
    val finalPath = filePath + key.split("/").last

    if(dims.nonEmpty) {
      data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").partitionBy(dims: _*).mode("overwrite").save(finalPath)
    } else
      data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(finalPath)

    val renameDir = finalPath + "/renamed/"
    FileUtil.renameHadoopFiles(finalPath, renameDir, reportId, dims)
  }

}
