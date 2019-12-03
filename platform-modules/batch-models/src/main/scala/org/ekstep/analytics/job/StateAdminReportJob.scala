package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, _}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.util.HDFSFileUtils
import org.sunbird.cloud.storage.conf.AppConf

case class UserStatus(id: Long, status: String)
object UnclaimedStatus extends UserStatus(0, "UNCLAIMED")
object ClaimedStatus extends UserStatus(1, "CLAIMED")
object RejectedStatus extends UserStatus(2, "REJECTED")
object FailedStatus extends UserStatus(3, "FAILED")
object MultiMatchStatus extends UserStatus(4, "MULTIMATCH")
object OrgExtIdMismatch extends UserStatus(5, "ORGEXTIDMISMATCH")

case class ShadowUserData(channel: String, userextid: String, addedby: String, claimedon: java.sql.Timestamp, claimstatus: Int,
                          createdon: java.sql.Timestamp, email: String, name: String, orgextid: String, processid: String,
                          phone: String, updatedon: java.sql.Timestamp, userid: String, userids: List[String], userstatus: Int)

// Shadow user summary in the json will have this POJO
case class UserSummary(accounts_validated: Long, accounts_rejected: Long, accounts_unclaimed: Long, accounts_failed: Long)

object StateAdminReportJob extends optional.Application with IJob with BaseReportsJob {

  implicit val className: String = "org.ekstep.analytics.job.StateAdminReportJob"
  val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val tempDir = AppConf.getConfig("admin.metrics.temp.dir")


  def name(): String = "StateAdminReportJob"

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

    generateReport();
    val channelSlugMap: Map[String, String] = getChannelSlugMap()
    renameChannelDirsToSlug(renamedDir, channelSlugMap)
    uploadReport(renamedDir)
    JobLogger.end("StateAdminReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
  }

   def generateReport()(implicit sparkSession: SparkSession) = {

    import sparkSession.implicits._
    val renamedDir = s"$tempDir/renamed"
    val fSFileUtils = new HDFSFileUtils(className, JobLogger)
    val detailDir = s"$tempDir/detail"
    val summaryDir = s"$tempDir/summary"

    val shadowDataEncoder = Encoders.product[ShadowUserData].schema
    val shadowUserDF = loadData(sparkSession, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace), Some(shadowDataEncoder)).as[ShadowUserData]
     val claimedShadowUserDF = shadowUserDF.where(col("claimstatus")=== ClaimedStatus.id)

    val shadowDataSummary = generateSummaryData(shadowUserDF)

    saveUserSummaryReport(shadowDataSummary, s"$summaryDir")
    saveUserDetailsReport(shadowUserDF.toDF(), s"$detailDir")

    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "user-detail")
    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "user-summary")

    // Purge the directories after copying to the upload staging area
    fSFileUtils.purgeDirectory(detailDir)
    fSFileUtils.purgeDirectory(summaryDir)

     // Only claimed used
     val claimedShadowDataSummaryDF = claimedShadowUserDF.groupBy("channel")
       .pivot("claimstatus").agg(count("claimstatus")).na.fill(0)

     saveUserValidatedSummaryReport(claimedShadowDataSummaryDF, s"$summaryDir")
     saveUserDetailsReport(claimedShadowUserDF.toDF(), s"$detailDir")

     fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "validated-user-detail")
     fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "validated-user-summary")

    // Purge the directories after copying to the upload staging area
     fSFileUtils.purgeDirectory(detailDir)
     fSFileUtils.purgeDirectory(summaryDir)

     val blockDataWithSlug = generateGeoSummaryReport()
     val resultDF = blockDataWithSlug.join(claimedShadowUserDF, blockDataWithSlug.col("externalid") === (claimedShadowUserDF.col("orgextid")),"left")
     resultDF.groupBy(col("slug"),col("District name").as("districtName")).
       agg(countDistinct("Block id").as("blocks"),countDistinct(claimedShadowUserDF.col("orgextid")).as("schools"), count("userextid").as("registered")).write
       .partitionBy("slug")
       .mode("overwrite")
       .json(s"$summaryDir")

     fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "validated-user-summary-district")
     fSFileUtils.purgeDirectory(summaryDir)
    shadowUserDF.distinct()
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
      col("externalid").as("externalid"),
      col("slug").as("slug")).cache();

    val rootOrgs = organisationDF.select(col("id").as("rootorgjoinid"), col("channel").as("rootorgchannel"),col("slug").as("rootorgslug")).where(col("isrootorg") && col("status").===(1)).collect();
    val rootOrgRDD = sparkSession.sparkContext.parallelize(rootOrgs.toSeq);
    val rootOrgEncoder = Encoders.product[RootOrgData].schema
    val rootOrgDF = sparkSession.createDataFrame(rootOrgRDD, rootOrgEncoder);

    val subOrgDF = organisationDF
      .withColumn("explodedlocation", explode(when(size(col("locationids")).equalTo(0), array(lit(null).cast("string")))
        .otherwise(when(col("locationids").isNotNull, col("locationids"))
          .otherwise(array(lit(null).cast("string"))))))

    val subOrgJoinedDF = subOrgDF
      .where(col("status").equalTo(1) && not(col("isrootorg")))
      .join(locationDF, subOrgDF.col("explodedlocation") === locationDF.col("locid"), "left")
      .join(rootOrgDF, subOrgDF.col("rootorgid") === rootOrgDF.col("rootorgjoinid")).as[SubOrgRow]

    val districtDF = subOrgJoinedDF.where(col("loctype").equalTo("district")).select(col("channel").as("channel"), col("slug"), col("id").as("schoolid"), col("orgname").as("schoolname"), col("locid").as("districtid"), col("locname").as("districtname"));

    val blockDF = subOrgJoinedDF.where(col("loctype").equalTo("block")).select(col("id").as("schooljoinid"), col("locid").as("blockid"), col("locname").as("blockname"), col("externalid"));

    val blockData = blockDF.join(districtDF, blockDF.col("schooljoinid").equalTo(districtDF.col("schoolid")), "left").drop(col("schooljoinid")).coalesce(1)
      .select(col("schoolid").as("School id"),
        col("schoolname").as("School name"),
        col("channel").as("Channel"),
        col("districtid").as("District id"),
        col("districtname").as("District name"),
        col("blockid").as("Block id"),
        col("blockname").as("Block name"),
        col("slug").as("slug"),
        col("externalid"))

    blockData
  }

  private def getChannelSlugMap()(implicit sparkSession: SparkSession): Map[String, String] = {
    val channelSlugMap: Map[String, String] = loadData(sparkSession, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace), None)
      .select(col("channel"), col("slug")).where(col("isrootorg") && col("status").===(1))
      .collect().groupBy(f => f.get(0).asInstanceOf[String]).mapValues(f => f.head.get(1).asInstanceOf[String]);
    return channelSlugMap
  }

  def generateSummaryData(shadowUserDF: Dataset[ShadowUserData])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    def transformClaimedStatusValue()(ds: Dataset[ShadowUserData]) = {
      ds.withColumn(
        "claim_status",
        when($"claimstatus" === UnclaimedStatus.id, lit(UnclaimedStatus.status))
          .when($"claimstatus" === ClaimedStatus.id, lit(ClaimedStatus.status))
          .when($"claimstatus" === FailedStatus.id, lit(FailedStatus.status))
          .when($"claimstatus" === RejectedStatus.id, lit(RejectedStatus.status))
          .when($"claimstatus" === MultiMatchStatus.id, lit(MultiMatchStatus.status))
          .when($"claimstatus" === OrgExtIdMismatch.id, lit(OrgExtIdMismatch.status))
          .otherwise(lit("")))
    }

    val shadowDataSummary = shadowUserDF.transform(transformClaimedStatusValue()).groupBy("channel")
      .pivot("claim_status").agg(count("claim_status")).na.fill(0)

    shadowDataSummary
  }

  /**
   * Saves the raw data as a .csv.
   * Appends /detail to the URL to prevent overwrites.
   * Check function definition for the exact column ordering.
   * @param reportDF
   * @param url
   */
  def saveUserDetailsReport(reportDF: DataFrame, url: String): Unit = {
    // List of fields available
    //channel,userextid,addedby,claimedon,claimstatus,createdon,email,name,orgextid,phone,processid,updatedon,userid,userids,userstatus

    reportDF.coalesce(1)
      .select(
        col("channel"),
        col("userextid").as("User external id"),
        col("userstatus").as("User account status"),
        col("userid").as("User id"),
        concat_ws(",", col("userids")).as("Matching User ids"),
        col("claimedon").as("Claimed on"),
        col("orgextid").as("School external id"),
        col("claimstatus").as("Claimed status"),
        col("createdon").as("Created on"),
        col("updatedon").as("Last updated on"))
      .write
      .partitionBy("channel")
      .mode("overwrite")
      .option("header", "true")
      .csv(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
  }

  def saveUserSummaryReport(reportDF: DataFrame, url: String): Unit = {
    val dfColumns = reportDF.columns.toSet

    // Get claim status not in the current dataframe to add them.
    val columns: Seq[String] = Seq(
      UnclaimedStatus.status,
      ClaimedStatus.status,
      RejectedStatus.status,
      FailedStatus.status,
      MultiMatchStatus.status,
      OrgExtIdMismatch.status).filterNot(dfColumns)
    val correctedReportDF = columns.foldLeft(reportDF)((acc, col) => {
      acc.withColumn(col, lit(0))
    })
    JobLogger.log(s"columns to add in this report $columns")

    correctedReportDF.coalesce(1)
      .select(
        col("channel"),
        when(col(UnclaimedStatus.status).isNull, 0).otherwise(col(UnclaimedStatus.status)).as("accounts_unclaimed"),
        when(col(ClaimedStatus.status).isNull, 0).otherwise(col(ClaimedStatus.status)).as("accounts_validated"),
        when(col(RejectedStatus.status).isNull, 0).otherwise(col(RejectedStatus.status)).as("accounts_rejected"),
        when(col(FailedStatus.status).isNull, 0).otherwise(col(FailedStatus.status)).as(FailedStatus.status),
        when(col(MultiMatchStatus.status).isNull, 0).otherwise(col(MultiMatchStatus.status)).as(MultiMatchStatus.status),
        when(col(OrgExtIdMismatch.status).isNull, 0).otherwise(col(OrgExtIdMismatch.status)).as(OrgExtIdMismatch.status))
      .withColumn(
        "accounts_failed",
        col(FailedStatus.status) + col(MultiMatchStatus.status) + col(OrgExtIdMismatch.status))
      .write
      .partitionBy("channel")
      .mode("overwrite")
      .json(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
  }

  def saveUserValidatedSummaryReport(reportDF: DataFrame, url: String): Unit = {
    reportDF.coalesce(1)
      .select(
        col("channel"),
        when(col(ClaimedStatus.id.toString).isNull, 0).otherwise(col(ClaimedStatus.id.toString)).as("registered"))
      .write
      .partitionBy("channel")
      .mode("overwrite")
      .json(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
  }

  private def renameChannelDirsToSlug(sourcePath: String, channelSlugMap: Map[String, String]) = {
    val fsFileUtils = new HDFSFileUtils(className, JobLogger)
    val files = fsFileUtils.getSubdirectories(sourcePath)
    files.map { oneChannelDir =>
      val name = oneChannelDir.getName()
      val slugName = channelSlugMap.get(name);
      if(slugName.nonEmpty) {
        JobLogger.log("name = ${name} and slugname = ${slugName}")
        val newDirName = oneChannelDir.getParent() + "/" + slugName.get.asInstanceOf[String]
        fsFileUtils.renameDirectory(oneChannelDir.getAbsolutePath(), newDirName)
      } else {
        JobLogger.log("Slug not found for - " + name);
      }
    }
  }

  def uploadReport(sourcePath: String) = {
    // Container name can be generic - we dont want to create as many container as many reports
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

    reportStorageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
    // TODO: Purge the files after uploaded to blob store
  }
}

object StateAdminReportJobTest {

  def main(args: Array[String]): Unit = {
    StateAdminReportJob.main("""{"model":"Test"}""");
  }
}
