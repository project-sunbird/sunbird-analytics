package org.ekstep.analytics.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.job.AssessmentMetricsJob.transposeDF
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.{Map, Seq}

object AssessmentReportUtil {
  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJob"

  private val indexName: String = AppConf.getConfig("assessment.metrics.es.index.prefix") + DateTimeFormat.forPattern("dd-MM-yyyy-HH-mm").print(DateTime.now())

  def saveToAzure(reportDF: DataFrame, url: String, batchId: String): String = {
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
      col("grand_score").as("score"),
      col("maskedemail").as("maskedEmail"),
      col("maskedphone").as("maskedPhone"),
      col("district_name").as("districtName"),
      col("orgname_resolved").as("rootOrgName"),
      col("externalid").as("externalId"),
      col("schoolname_resolved").as("subOrgName"),
      col("total_sum_score").as("totalScore"),
      col("content_name").as("contentName"),
      col("reportUrl").as("reportUrl")
    )
    ESUtil.saveToIndex(assessmentReportDF, index)
  }

  def rollOverIndex(index: String, alias: String): Unit = {
    val indexList = ESUtil.getIndexName(alias)
    if(!indexList.contains(index)) ESUtil.rolloverIndex(index, alias)
  }


  def save(courseBatchList: Array[Map[String, Any]], reportDF: DataFrame, url: String, spark: SparkSession): Unit = {
    val aliasName = AppConf.getConfig("assessment.metrics.es.alias")
    val indexToEs = AppConf.getConfig("course.es.index.enabled")
    courseBatchList.foreach(item => {
      JobLogger.log("Course batch mappings: " + item, None, INFO)
      val courseId = item.getOrElse("courseid", "").asInstanceOf[String]
      val batchList = item.getOrElse("batchid", "").asInstanceOf[Seq[String]].distinct
      batchList.foreach(batchId => {
        if (!courseId.isEmpty && !batchId.isEmpty) {
          val filteredDF = reportDF.filter(col("courseid") === courseId && col("batchid") === batchId)
          val reportData = transposeDF(filteredDF)
          try {
            val urlBatch = AssessmentReportUtil.saveToAzure(reportData, url, batchId)
            val resolvedDF = filteredDF.withColumn("reportUrl", lit(urlBatch))
            if (StringUtils.isNotBlank(indexToEs) && StringUtils.equalsIgnoreCase("true", indexToEs)) {
              AssessmentReportUtil.saveToElastic(this.getIndexName, aliasName, resolvedDF)
              JobLogger.log("Indexing of assessment report data is success: " + this.getIndexName, None, INFO)
            } else {
              JobLogger.log("Skipping Indexing assessment report into ES", None, INFO)
            }
          } catch {
            case e: Exception => JobLogger.log("File upload is failed due to " + e, None, ERROR)
          }
        } else {
          JobLogger.log("Report failed to create since course_id is " + courseId + "and batch_id is " + batchId, None, ERROR)
        }
      })
    })
    AssessmentReportUtil.rollOverIndex(getIndexName, aliasName)
  }

  def getIndexName: String = {
    this.indexName
  }
}
