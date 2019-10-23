package org.ekstep.analytics.job

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util.Optional

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

import scala.collection.{Map, _}


object UserStatus extends Enumeration {
  type UserStatus = Value

  //  UNCLAIMED: 0 // initial Status <br>
  val unclaimed = Value(0, "UNCLAIMED")
  //  ELIGIBLE: 1 // When the user is eligible to migrate.<br>
  val eligible = Value(1, "ELIGIBLE")
  //  REJECTED: 2 // when user says NO
  val rejected = Value(2, "REJECTED")
  //  FAILED: 3  // when user failed to verify the ext user id.
  val failed = Value(3, "FAILED")
  //  MULTIMATCH: 4 // when multiple user found with the identifier.<br>
  val multimatch = Value("FAILED")
  //  ORGEXTIDMISMATCH: 5 // when provided ext org id is incorrect.<br>
  val orgextidmismatch = Value("ORGEXTIDMISMATCH")
}


object StateAdminReportJob extends optional.Application with IJob with ReportGenerator {

  implicit val className = "org.ekstep.analytics.job.StateAdminReportJob"

  def name(): String = "StateAdminReportJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {

    JobLogger.init("StateAdminReportJob")
    JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = 10

    def runJob(sc: SparkContext): Unit = {
      try {
        execute(jobConfig)(sc)
      } finally {
        CommonUtil.closeSparkContext()(sc)
      }
    }

    sc match {
      case Some(value) => {
        implicit val sparkContext: SparkContext = value
        runJob(value)
      }
      case None => {
        val sparkCassandraConnectionHost =
          jobConfig.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkCassandraConnectionHost")
        val sparkElasticsearchConnectionHost =
          jobConfig.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkElasticsearchConnectionHost")
        implicit val sparkContext: SparkContext =
          CommonUtil.getSparkContext(JobContext.parallelization,
            jobConfig.appName.getOrElse(jobConfig.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost)
        runJob(sparkContext)
      }
    }
  }

  private def execute(config: JobConfig)(implicit sc: SparkContext) = {
    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val sparkConf = sc.getConf
      .set("es.write.operation", "upsert")
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val reportDF = prepareReport(spark, loadData)
    uploadReport(renamedDir)
    JobLogger.end("StateAdminReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(settings)
      .load()
  }

  def prepareReport(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
    val distinctChannelDF = loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace)).select(col = "channel").distinct()

    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"

    // For distinct channel names, do the following:
    // 1. Create a json, csv report - user-summary.json, user-detail.csv.
    distinctChannelDF.collect().foreach(rowUnit => {
      var channelName = rowUnit.mkString
      JobLogger.log(channelName)

      val oneOrgUsersDF = loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace))
          .where(col("channel").===(channelName))
                                         // .where(condition = s"channel == ${rowUnit.mkString}")
      JobLogger.log(s"${rowUnit.mkString} has ${oneOrgUsersDF.cache().count()} many users in shadow_user table")
      //println(s"${rowUnit.mkString} has ${oneOrgUsersDF.cache().count()} many users in shadow_user table")

      saveReport(oneOrgUsersDF, tempDir)

      renameReport(tempDir, renamedDir, ".csv", "user-detail")
      renameReport(tempDir, renamedDir, ".json", "user-summary")
      //uploadReport(renamedDir)
    })

    // TODO Geo-data

    return distinctChannelDF
  }


  def saveReportES(reportDF: DataFrame): Unit = {
  }

  def saveReport(reportDF: DataFrame, url: String): Unit = {
    saveDetailsReport(reportDF, url);
    saveSummaryReport(reportDF, url);
  }

  /**
    * Saves the raw data as a .csv.
    * Appends /detail to the URL to prevent overwrites.
    * Check function definition for the exact column ordering.
    * @param reportDF
    * @param url
    */
  def saveDetailsReport(reportDF: DataFrame, url: String): Unit = {
    reportDF.coalesce(1)
      .write
      .partitionBy(colNames = "channel")
      .mode("overwrite")
      .option("header", "true")
      .csv(url + "/detail")

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
  }

  /**
    * Saves the raw data as a .json.
    * Appends /detail to the URL to prevent overwrites.
    * Check function definition for the exact column ordering.
    * @param reportDF
    * @param url
    */
  def saveSummaryReport(reportDF: DataFrame, url: String): Unit = {
    reportDF.coalesce(1)
      .write
      .partitionBy(colNames = "channel")
      .mode("overwrite")
      .json(url + "/summary")

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
    println(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()} and ${url}")
  }

  def uploadReport(sourcePath: String) = {
    val provider = AppConf.getConfig("course.metrics.cloud.provider")

    // Container name can be generic - we dont want to create as many container as many reports
    val container = AppConf.getConfig("course.metrics.cloud.container")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

    val storageService = StorageServiceFactory
      .getStorageService(StorageConfig(provider, AppConf.getStorageKey(provider), AppConf.getStorageSecret(provider)))
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
  }

  private def recursiveListFiles(file: File, ext: String): Array[File] = {
    val fileList = file.listFiles
    val extOnly = fileList.filter(file => file.getName.endsWith(ext))
    extOnly ++ fileList.filter(_.isDirectory).flatMap(recursiveListFiles(_, ext))
  }

  private def purgeDirectory(dir: File): Unit = {
    for (file <- dir.listFiles) {
      if (file.isDirectory) purgeDirectory(file)
      file.delete
    }
  }

  def renameReport(tempDir: String, outDir: String, fileExt: String, fileNameSuffix: String = null) = {
    val regex = """\=.*/""".r // example path "somepath/partitionFieldName=12313144/part-0000.csv"
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

    val fileList = recursiveListFiles(temp, fileExt)

    JobLogger.log(s"moving ${fileList.length} files to ${out.getPath}")

    fileList.foreach(file => {
      val value = regex.findFirstIn(file.getPath).getOrElse("")
      if (value.length > 1) {
        val partitionFieldName = value.substring(1, value.length() - 1)
        var filePath = s"${out.getPath}/$partitionFieldName-$fileNameSuffix" + fileExt
        Files.copy(file.toPath, new File(filePath).toPath, StandardCopyOption.REPLACE_EXISTING)
      }
    })
  }
}

