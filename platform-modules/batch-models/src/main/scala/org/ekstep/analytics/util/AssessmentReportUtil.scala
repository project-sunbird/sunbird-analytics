package org.ekstep.analytics.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.JobLogger
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.cloud.storage.conf.AppConf

object AssessmentReportUtil {
  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJob"

  def save(reportDF: DataFrame, url: String, batchId:String): Unit = {
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val provider = AppConf.getConfig("assessment.metrics.cloud.provider")
    val container = AppConf.getConfig("course.metrics.cloud.container")
    val objectKey = AppConf.getConfig("assessment.metrics.cloud.objectKey")

    if (!reportDF.take(1).isEmpty) {
      reportDF.coalesce(1).write
        .mode("overwrite")
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(url)
      FileUtil.renameReport(tempDir, renamedDir, batchId)
      FileUtil.uploadReport(renamedDir, provider, container, Some(objectKey))
    }
  }

  def saveToElastic(index: String, alias: String, reportDF: DataFrame): Unit = {
    val assessmentReportDF = reportDF.select(
      col("userid").as("userId"),
      col("username").as("userName"),
      col("courseid").as("courseId"),
      col("batchid").as("batchId"),
      col("total_score").as("score"),
      col("maskedemail").as("maskedEmail"),
      col("maskedphone").as("maskedPhone"),
      col("district_name").as("districtName"),
      col("orgname_resolved").as("rootOrgName"),
      col("externalid").as("externalId"),
      col("schoolname_resolved").as("subOrgName"),
      col("total_sum_score").as("totalScore"),
      col("content_name").as("contentName")
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

  def suffixDate(index: String): String = {
    index + DateTimeFormat.forPattern("dd-MM-yyyy-HH-mm").print(DateTime.now())
  }

}