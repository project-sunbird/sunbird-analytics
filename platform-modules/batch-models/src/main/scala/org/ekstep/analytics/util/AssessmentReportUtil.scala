package org.ekstep.analytics.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.job.AssessmentMetricsJob.transposeDF
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.{Map, Seq}

object AssessmentReportUtil {
  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJob"

  def save(reportDF: DataFrame, url: String, batchId: String): String = {
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val provider = AppConf.getConfig("assessment.metrics.cloud.provider")
    val container = AppConf.getConfig("course.metrics.cloud.container")
    val objectKey = AppConf.getConfig("assessment.metrics.cloud.objectKey")
    reportDF.coalesce(1).write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(url)
    FileUtil.renameReport(tempDir, renamedDir, batchId)
    FileUtil.uploadReport(renamedDir, provider, container, Some(objectKey))
  }

  def saveToElastic(index: String, alias: String, reportDF: DataFrame): Unit = {
    val assessmentReportDF = reportDF.select(
      col("userid").as("userId"),
      col("username").as("userName"),
      col("courseid").as("courseId"),
      col("batchid").as("batchId"),
      col("grand_total").as("score"),
      col("maskedemail").as("maskedEmail"),
      col("maskedphone").as("maskedPhone"),
      col("district_name").as("districtName"),
      col("orgname_resolved").as("rootOrgName"),
      col("externalid").as("externalId"),
      col("schoolname_resolved").as("subOrgName"),
      col("total_sum_score").as("totalScore"),
      col("content_name").as("contentName"),
      col("bloburl").as("blobUrl")
    )
    try {
      val indexList = ESUtil.getIndexName(alias)
      val oldIndex = indexList.mkString("")
      ESUtil.saveToIndex(assessmentReportDF, index)
      JobLogger.log("Indexing of assessment report data is success: " + index, None, INFO)
      if (!oldIndex.equals(index)) ESUtil.rolloverIndex(index, alias)
    } catch {
      case ex: Exception => {
        JobLogger.log(ex.getMessage, None, ERROR)
        ex.printStackTrace()
      }
    }
  }

  def saveToAzure(courseBatchList: Array[Map[String, Any]], reportDF: DataFrame, url: String): Map[String, String] = {
    val urlBatch = scala.collection.mutable.Map[String, String]()
    courseBatchList.foreach(item => {
      JobLogger.log("Course batch mappings: " + item, None, INFO)
      val courseId = item.getOrElse("courseid", "").asInstanceOf[String]
      val batchList = item.getOrElse("batchid", "").asInstanceOf[Seq[String]].distinct
      batchList.foreach(batchId => {
        if (!courseId.isEmpty && !batchId.isEmpty) {
          val reportData = transposeDF(reportDF, courseId, batchId)
          try {
            urlBatch(batchId) = AssessmentReportUtil.save(reportData, url, batchId)
          } catch {
            case e: Exception => JobLogger.log("File upload is failed due to " + e)
          }
        } else {
          JobLogger.log("Report failed to create since course_id is " + courseId + "and batch_id is " + batchId, None, INFO)
        }
      })
    })
    urlBatch
  }

  def suffixDate(index: String): String = {
    index + DateTimeFormat.forPattern("dd-MM-yyyy-HH-mm").print(DateTime.now())
  }

}