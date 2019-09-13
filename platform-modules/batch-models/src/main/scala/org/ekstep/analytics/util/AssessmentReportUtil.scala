package org.ekstep.analytics.util

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.elasticsearch.spark.sql._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

object AssessmentReportUtil {
  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJob"

  def save(reportDF: DataFrame, url: String): Unit = {
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"

    if (!reportDF.take(1).isEmpty) {
      reportDF.coalesce(1).write.partitionBy("batchid", "courseid")
        .mode("overwrite")
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(url)
      AssessmentReportUtil.renameReport(tempDir, renamedDir)
      AssessmentReportUtil.uploadReport(renamedDir)
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
      col("name").as("contentName")
    )
    try {
      val indexList = ESUtil.getIndexName(alias)
      val oldIndex = indexList.mkString("")
      assessmentReportDF.saveToEs(s"$index/_doc")
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

  def renameReport(tempDir: String, outDir: String) = {

    val regex = """\=.*/""".r // to get batchid from the path "somepath/batchid=12313144/part-0000.csv"
    val temp = new File(tempDir)
    val out = new File(outDir)

    if (!temp.exists()) throw new Exception(s"path $tempDir doesn't exist")
    if (out.exists()) {
      purgeDirectory(out)
      JobLogger.log(s"cleaning out the directory ${out.getPath}")
    } else {
      out.mkdirs()
      JobLogger.log(s"creating the directory ${out.getPath}")
    }

    val fileList = recursiveListFiles(temp, ".csv")

    JobLogger.log(s"moving ${fileList.length} files to ${out.getPath}")

    fileList.foreach(file => {
      val value = regex.findFirstIn(file.getPath).getOrElse("")
      if (value.length > 1) {
        val report_name = value.split("/")
        val batchValue = report_name.toList.head
        val batchId = batchValue.substring(1, batchValue.length)
        val courseId = report_name.toList(1).split("=").toList(1)
        JobLogger.log(s"Creating a Report: report-$courseId-$batchId.csv")
        Files.copy(file.toPath, new File(s"${out.getPath}/report-$courseId-$batchId.csv").toPath, StandardCopyOption.REPLACE_EXISTING)
      }
    })

  }

  private def recursiveListFiles(file: File, ext: String): Array[File]
  = {
    val fileList = file.listFiles
    val extOnly = fileList.filter(file => file.getName.endsWith(ext))
    extOnly ++ fileList.filter(_.isDirectory).flatMap(recursiveListFiles(_, ext))
  }

  private def purgeDirectory(dir: File): Unit

  = {
    for (file <- dir.listFiles) {
      if (file.isDirectory) purgeDirectory(file)
      file.delete
    }
  }

  def uploadReport(sourcePath: String) = {
    val provider = AppConf.getConfig("assessment.metrics.cloud.provider")
    val container = AppConf.getConfig("course.metrics.cloud.container")
    val objectKey = AppConf.getConfig("assessment.metrics.cloud.objectKey")

    val storageService = StorageServiceFactory
      .getStorageService(StorageConfig(provider, AppConf.getStorageKey(provider), AppConf.getStorageSecret(provider)))
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
    println("report is uploaded to azure cloud storage from this path: " + sourcePath)
  }

  def getContentNames(spark: SparkSession, content: List[String]): DataFrame = {
    case class content_identifiers(identifiers: List[String])
    val contentList = JSONUtils.serialize(content_identifiers(content).identifiers)
    JobLogger.log(s"Total number of unique content identifiers are ${contentList.length}")
    val request =
      s"""
         {
         |  "_source": {
         |    "includes": [
         |      "name"
         |    ]
         |  },
         |  "query": {
         |    "bool": {
         |      "must": [
         |        {
         |          "terms": {
         |            "identifier": $contentList
         |          }
         |        }
         |      ]
         |    }
         |  }
         |}
       """.stripMargin
    spark.read.format("org.elasticsearch.spark.sql")
      .option("query", request)
      .option("pushdown", "true")
      .load(AppConf.getConfig("assessment.metrics.content.index"))
      .select("name", "identifier") // Fields need to capture from the elastic search
  }
}