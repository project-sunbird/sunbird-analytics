package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, _}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.util.HDFSFileUtils
import org.sunbird.cloud.storage.conf.AppConf

case class RootOrgData(rootorgjoinid: String, rootorgchannel: String, rootorgslug: String)

case class SubOrgRow(id: String, isrootorg: Boolean, rootorgid: String, channel: String, status: String, locationid: String, locationids: Seq[String], orgname: String,
                     exploded_location: String, locid: String, loccode: String, locname: String, locparentid: String, loctype: String, rootorgjoinid: String, rootorgchannel: String)

object StateAdminGeoReportJob extends optional.Application with IJob with BaseReportsJob {

  implicit val className: String = "org.ekstep.analytics.job.StateAdminGeoReportJob"

  def name(): String = "StateAdminGeoReportJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {

    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = 10

    implicit val sparkSession: SparkSession = openSparkSession(jobConfig);
    execute(jobConfig)
    closeSparkSession()
    System.exit(0)
  }

  private def execute(config: JobConfig)(implicit sparkSession: SparkSession) = {

    val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"

    generateGeoSummaryReport()
    uploadReport(renamedDir)
    JobLogger.end("StateAdminGeoReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
  }

   def generateGeoSummaryReport()(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
    val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"

    val fSFileUtils = new HDFSFileUtils(className, JobLogger)

    val detailDir = s"$tempDir/detail"
    val summaryDir = s"$tempDir/summary"

    val locationDF = loadData(sparkSession, Map("table" -> "location", "keyspace" -> sunbirdKeyspace), None).select(
      col("id").as("locid"),
      col("code").as("loccode"),
      col("name").as("locname"),
      col("parentid").as("locparentid"),
      col("type").as("loctype"))

    val organisationDF = loadData(sparkSession, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace), None).select(
      col("id").as("id"),
      col("isrootorg").as("isrootorg"),
      col("rootorgid").as("rootorgid"),
      col("channel").as("channel"),
      col("status").as("status"),
      col("locationid").as("locationid"),
      col("orgname").as("orgname"),
      col("locationids").as("locationids"),
      col("slug").as("slug")).cache();

    val rootOrgs = organisationDF.select(col("id").as("rootorgjoinid"), col("channel").as("rootorgchannel"),col("slug").as("rootorgslug")).where(col("isrootorg") && col("status").===(1)).collect();
    val rootOrgRDD = sparkSession.sparkContext.parallelize(rootOrgs.toSeq);
    val rootOrgEncoder = Encoders.product[RootOrgData].schema
    val rootOrgDF = sparkSession.createDataFrame(rootOrgRDD, rootOrgEncoder);

    val subOrgDF = organisationDF
      .withColumn("exploded_location", explode(when(size(col("locationids")).equalTo(0), array(lit(null).cast("string")))
        .otherwise(when(col("locationids").isNotNull, col("locationids"))
          .otherwise(array(lit(null).cast("string"))))))

    val subOrgJoinedDF = subOrgDF
      .where(col("status").equalTo(1) && not(col("isrootorg")))
      .join(locationDF, subOrgDF.col("exploded_location") === locationDF.col("locid"), "left")
      .join(rootOrgDF, subOrgDF.col("rootorgid") === rootOrgDF.col("rootorgjoinid")).as[SubOrgRow]

    subOrgJoinedDF
      .groupBy("slug")
      .agg(countDistinct("id").as("schools"), count(col("locType").equalTo("district")).as("districts"),
          count(col("locType").equalTo("block")).as("blocks"))
      .coalesce(1)
      .write
      .partitionBy("slug")
      .mode("overwrite")
      .json(s"$summaryDir")


    val districtDF = subOrgJoinedDF.where(col("loctype").equalTo("district")).select(col("channel").as("channel"), col("slug"), col("id").as("schoolid"), col("orgname").as("schoolname"), col("locid").as("districtid"), col("locname").as("districtname"));
    val blockDF = subOrgJoinedDF.where(col("loctype").equalTo("block")).select(col("id").as("schooljoinid"), col("locid").as("blockid"), col("locname").as("blockname"));

    blockDF.join(districtDF, blockDF.col("schooljoinid").equalTo(districtDF.col("schoolid")), "left").drop(col("schooljoinid")).coalesce(1)
      .select(col("schoolid").as("School id"),
        col("schoolname").as("School name"),
        col("channel").as("Channel"),
        col("districtid").as("District id"),
        col("districtname").as("District name"),
        col("blockid").as("Block id"),
        col("blockname").as("Block name"),
        col("slug"))
      .write
      .partitionBy("slug")
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$detailDir")

    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "geo-summary")
    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "geo-detail")
    fSFileUtils.purgeDirectory(detailDir)
    fSFileUtils.purgeDirectory(summaryDir)

     blockDF.distinct()
  }

  def saveGeoDetailsReport(reportDF: DataFrame, url: String): Unit = {
    reportDF.coalesce(1)
      .select(
        col("id").as("School id"),
        col("orgname").as("School name"),
        col("channel").as("Channel"),
        col("status").as("Status"),
        col("locid").as("Location id"),
        col("name").as("Location name"),
        col("parentid").as("Parent location id"),
        col("type").as("type"))
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(url)

    JobLogger.log(s"StateAdminGeoReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
  }

  /**
   * Saves the raw data as a .json.
   * Appends /summary to the URL to prevent overwrites.
   * Check function definition for the exact column ordering.
   * * If we don't partition, the reports get subsequently updated and we dont want so
   * @param reportDF
   * @param url
   */
  def saveSummaryReport(reportDF: DataFrame, url: String): Unit = {
    reportDF.coalesce(1)
      .write
      .partitionBy("channel")
      .mode("overwrite")
      .json(url)

    JobLogger.log(s"StateAdminGeoReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
    println(s"StateAdminGeoReportJob: uploadedSuccess nRecords = ${reportDF.count()} and ${url}")
  }

  def uploadReport(sourcePath: String) = {
    // Container name can be generic - we dont want to create as many container as many reports
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

    reportStorageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
    // TODO: Purge the files after uploaded to blob store
  }
}

object StateAdminGeoReportJobTest {

  def main(args: Array[String]): Unit = {
    StateAdminGeoReportJob.main("""{"model":"Test"}""");
  }
}
