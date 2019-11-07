package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.util.HDFSFileUtils
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

import scala.collection.{Map, _}

case class UserStatus(id: Long, status: String)
object UnclaimedStatus extends UserStatus(0, "UNCLAIMED")
object ClaimedStatus extends UserStatus(1, "CLAIMED")
object RejectedStatus extends UserStatus(2, "REJECTED")
object FailedStatus extends UserStatus(3, "FAILED")
object MultiMatchStatus extends UserStatus(4, "MULTIMATCH")
object OrgExtIdMismatch extends UserStatus(5, "ORGEXTIDMISMATCH")

case class ShadowUserData(channel: String, userextid: String, addedby: String, claimedon: Long, claimstatus: Int,
                          createdon: Long, email: String, name: String, orgextid: String, processid: String,
                          phone: String, updatedon: Long, userid: String, userids: List[String], userstatus: String)
case class RootOrgData(id: String, channel: String)

// Shadow user summary in the json will have this POJO
case class UserSummary(accounts_validated: Long, accounts_rejected: Long, accounts_unclaimed: Long, accounts_failed: Long)

// Geo user summary in the json will have this POJO
case class GeoSummary(districts: Long, blocks: Long, school: Long)

trait AdminReportGenerator extends Serializable {

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(settings)
      .load()
  }

  def loadDataWithSchema(spark: SparkSession, settings: Map[String, String], schema: Option[StructType] = None): DataFrame = {
    val dataFrameReader = spark.read.format("org.apache.spark.sql.cassandra").options(settings)
    val dataFrameReaderWithSchema = schema.map(schema => dataFrameReader.schema(schema)).getOrElse(dataFrameReader)
    dataFrameReaderWithSchema.load()
  }

  def generateReport(spark: SparkSession, fetchTable: (SparkSession, Map[String, String], Option[StructType]) => DataFrame): DataFrame

}


object StateAdminReportJob extends optional.Application with IJob with AdminReportGenerator {

  implicit val className: String = "org.ekstep.analytics.job.StateAdminReportJob"

  def name(): String = "StateAdminReportJob"

  private val DETAIL_STR = "detail"
  private val SUMMARY_STR = "summary"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {

    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
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

    val reportDF = generateReport(spark, loadDataWithSchema)
    uploadReport(renamedDir)
    JobLogger.end("StateAdminReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
  }

  override def generateReport(spark: SparkSession, loadData: (SparkSession, Map[String, String], Option[StructType]) => DataFrame): DataFrame = {

    import spark.implicits._
    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")

    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"

    val fSFileUtils = new HDFSFileUtils(className, JobLogger)

    val detailDir = s"$tempDir/detail"
    val summaryDir = s"$tempDir/summary"

    val locationDF = loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace), None).select(
      col("id").as("locid"),
      col("code").as("code"),
      col("name").as("name"),
      col("parentid").as("parentid"),
      col("type").as("type")
    )

    implicit val rootOrgDataEncoder: Encoder[RootOrgData] = Encoders.product[RootOrgData]

    val shadowDataEncoder = Encoders.product[ShadowUserData].schema
    val shadowUserDF = loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace), Some(shadowDataEncoder)).as[ShadowUserData]

    val shadowDataSummary = generateSummaryData(shadowUserDF.as[ShadowUserData])(spark)

    saveUserSummaryReport(shadowDataSummary, s"$summaryDir")
    saveUserDetailsReport(shadowUserDF.toDF(), s"$detailDir")

    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "user-detail")
    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "user-summary")

    // Purge the directories after copying to the upload staging area
    fSFileUtils.purgeDirectory(detailDir)
    fSFileUtils.purgeDirectory(summaryDir)

    val organisationDF = loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace), None)

    val activeRootOrganisationDF = organisationDF
      .select(col("id"), col("channel"))
      .where(col("isrootorg") && col("status").=== (1))
      .as[RootOrgData]
    JobLogger.log(s"Active root org count = ${activeRootOrganisationDF.count()}")

    // iterating through all active rootOrg and fetching all active suborg for a particular rootOrg
    activeRootOrganisationDF.foreach(rootOrgData => {
      val rootOrgId = rootOrgData.id
      val channelName = rootOrgData.channel
      JobLogger.log(s"RootOrg id found = $rootOrgId and channel = $channelName")

      // fetching all suborg for a particular rootOrg
      val schoolCountDf = organisationDF
        .where(col("status").equalTo(1) && not(col("isrootorg"))
          && col("rootorgid").equalTo(rootOrgId))

      // getting count of suborg , that will provide me school count.
      val schoolCount = schoolCountDf.count()

      val locationExplodedSchoolDF = schoolCountDf
        .withColumn("exploded_location", explode(array("locationids")))

      // collecting District Details and count from organisation and location table
      val districtDF = locationExplodedSchoolDF
        .join(locationDF,
          col("exploded_location").cast("string")
            .contains(col("locid"))
           && locationDF.col("type") === "district")
      // collecting district count
      val districtCount = districtDF.count()

      // collecting block count and details.
      val blockDetailsDF = locationExplodedSchoolDF
        .join(locationDF,
          col("exploded_location").cast("string").contains(col("locid"))
            && locationDF.col("type") === "block")

      //collecting block count
      val blockCount = blockDetailsDF.count()

      val geoDetailsDF = districtDF.union(blockDetailsDF)
      //geoDetailsDF.show(false)

      val geoSummary = GeoSummary(districtCount, blockCount, schoolCount)
      val jsonStr = JSONUtils.serialize(geoSummary)
      val rdd = spark.sparkContext.parallelize(Seq(jsonStr))
      val summaryDF = spark.read.json(rdd)

      saveGeoDetailsReport(geoDetailsDF, s"$detailDir/channel=$channelName")
      saveSummaryReport(summaryDF, s"$summaryDir/channel=$channelName")
    })

    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "geo-detail")
    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "geo-summary")

    // Purge the directories after copying to the upload staging area
    fSFileUtils.purgeDirectory(detailDir)
    fSFileUtils.purgeDirectory(summaryDir)

    JobLogger.log("Finished with generateReport")

    shadowUserDF.toDF()
  }


  def generateSummaryData(shadowUserDF: Dataset[ShadowUserData])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    def transformClaimedStatusValue()(ds: Dataset[ShadowUserData]) = {
      ds.withColumn("claim_status",
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

    shadowDataSummary.show(10, false)
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
        col("updatedon").as("Last updated on")
      )
      .write
      .partitionBy("channel")
      .mode("overwrite")
      .option("header", "true")
      .csv(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
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
        col("type").as("type")
      )
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
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

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
    println(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()} and ${url}")
  }


  def saveUserSummaryReport(reportDF: DataFrame, url: String): Unit = {
    val dfColumns = reportDF.columns.toSet

    // Get claim status not in the current dataframe to add them.
    val columns: Seq[String] = Seq(UnclaimedStatus.status,
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
        when(col(OrgExtIdMismatch.status).isNull, 0).otherwise(col(OrgExtIdMismatch.status)).as(OrgExtIdMismatch.status)
      )
      .withColumn("accounts_failed",
          col(FailedStatus.status) + col(MultiMatchStatus.status) + col(OrgExtIdMismatch.status))
      .write
      .partitionBy("channel")
      .mode("overwrite")
      .json(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
  }

  def uploadReport(sourcePath: String) = {
    val provider = AppConf.getConfig("course.metrics.cloud.provider")

    // Container name can be generic - we dont want to create as many container as many reports
    val container = AppConf.getConfig("admin.reports.cloud.container")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

    val storageService = StorageServiceFactory
      .getStorageService(StorageConfig(provider, AppConf.getStorageKey(provider), AppConf.getStorageSecret(provider)))
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
  }
}

