package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext}
import org.ekstep.analytics.framework.{FrameworkContext, Params}
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.util.{JSONUtils, RestUtil}
import org.ekstep.analytics.model.ReportConfig
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

  implicit val fc = new FrameworkContext()


  def getLiveCourses(config: Map[String, AnyRef])(implicit sc: SparkContext): DataFrame = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val status = JSONUtils.serialize(config.get("courseStatus").get.asInstanceOf[List[String]])
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
    val resRDD = sc.parallelize(response)
    resRDD.toDF()
  }

  def getCourseBatchDetails()(implicit sc: SparkContext): RDD[_source] = {
    sc.emptyRDD
  }

  def postDataToBlob(data: DataFrame, config: Map[String, AnyRef])(implicit sc: SparkContext) = {
    val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

    reportConfig.metrics.flatMap{c => List()}
    val dimFields = reportConfig.metrics.flatMap { m =>
      if (m.druidQuery.dimensions.nonEmpty) m.druidQuery.dimensions.get.map(f => f.aliasName.getOrElse(f.fieldName))
      else List()
    }

    val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
    implicit val sqlContext = new SQLContext(sc)

    // Using foreach as parallel execution might conflict with local file path
    val key = config.getOrElse("key", null).asInstanceOf[String]
    reportConfig.output.foreach { f =>
      if ("csv".equalsIgnoreCase(f.`type`)) {
        val metricFields = f.metrics
        val fieldsList = data.columns
        val dimsLabels = labelsLookup.filter(x => f.dims.contains(x._1)).values.toList
        val filteredDf = data.select(fieldsList.head, fieldsList.tail: _*)
        val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
        val reportFinalId = if (f.label.nonEmpty && f.label.get.nonEmpty) reportConfig.id + "/" + f.label.get else reportConfig.id
        renamedDf.show()
        val dirPath = ReportUtil.writeToCSVAndRename(renamedDf, config ++ Map("dims" -> dimsLabels, "reportId" -> reportFinalId, "fileParameters" -> f.fileParameters))
        AzureDispatcher.dispatchDirectory(config ++ Map("dirPath" -> (dirPath + reportFinalId + "/"), "key" -> (key + reportFinalId + "/")))
      } else {
        val encoder = Encoders.STRING
        val strData = data.map(f => JSONUtils.serialize(f))(encoder)
        AzureDispatcher.dispatch(strData.collect(), config)
      }
    }
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
