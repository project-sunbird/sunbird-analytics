package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.util.HDFSFileUtils
import org.sunbird.cloud.storage.conf.AppConf

case class RootOrgData(rootorgjoinid: String, rootorgchannel: String, rootorgslug: String)

case class SubOrgRow(id: String, isrootorg: Boolean, rootorgid: String, channel: String, status: String, locationid: String, locationids: Seq[String], orgname: String,
                     explodedlocation: String, locid: String, loccode: String, locname: String, locparentid: String, loctype: String, rootorgjoinid: String, rootorgchannel: String, externalid: String)

object StateAdminGeoReportJob extends optional.Application with IJob with StateAdminReportHelper {

  implicit val className: String = "org.ekstep.analytics.job.StateAdminGeoReportJob"
  val fSFileUtils = new HDFSFileUtils(className, JobLogger)

  def name(): String = "StateAdminGeoReportJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = 10

    implicit val sparkSession: SparkSession = openSparkSession(jobConfig);
    implicit val frameworkContext = getReportingFrameworkContext();
    execute(jobConfig)
    closeSparkSession()
  }

  private def execute(config: JobConfig)(implicit sparkSession: SparkSession, fc: FrameworkContext) = {

    generateGeoReport()
    uploadReport(renamedDir)
    JobLogger.end("StateAdminGeoReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
  }

  def generateGeoReport() (implicit sparkSession: SparkSession): DataFrame = {
    val organisationDF: DataFrame = loadOrganisationDF()
    val blockData:DataFrame = generateGeoBlockData(organisationDF)
    blockData.write
      .partitionBy("slug")
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$detailDir")

    blockData
          .groupBy(col("slug"))
          .agg(countDistinct("School id").as("schools"),
            countDistinct(col("District id")).as("districts"),
            countDistinct(col("Block id")).as("blocks"))
          .coalesce(1)
          .write
          .partitionBy("slug")
          .mode("overwrite")
          .json(s"$summaryDir")

    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "geo-summary")
    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "geo-detail")
    fSFileUtils.purgeDirectory(detailDir)
    fSFileUtils.purgeDirectory(summaryDir)
    districtSummaryReport(blockData)
    blockData
  }

  def districtSummaryReport(blockData: DataFrame): Unit = {
    val blockDataWithSlug = blockData.
      groupBy(col("slug"),col("index"),col("District name").as("districtName")).
      agg(countDistinct("Block id").as("blocks"),count("School id").as("schools"))
    dataFrameToJsonFile(blockDataWithSlug)
    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "geo-summary-district")
    fSFileUtils.purgeDirectory(summaryDir)
  }

  def dataFrameToJsonFile(dataFrame: DataFrame): Unit = {
    dataFrame.write
      .partitionBy("slug")
      .mode("overwrite")
      .json(s"$summaryDir")
  }

  def uploadReport(sourcePath: String)(implicit fc: FrameworkContext) = {
    // Container name can be generic - we dont want to create as many container as many reports
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

    val storageService = getReportStorageService();
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
  }
}

