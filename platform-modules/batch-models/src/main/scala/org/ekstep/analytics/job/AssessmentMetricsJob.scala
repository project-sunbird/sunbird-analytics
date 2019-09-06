package org.ekstep.analytics.job

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import org.apache.spark.sql.SQLContext

import org.elasticsearch.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.util.ESUtil
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


import scala.collection.{Map, _}


object AssessmentMetricsJob extends optional.Application with IJob with ReportGenerator {

  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJob"

  def name(): String = "AssessmentMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {

    JobLogger.init("Assessment Metrics")
    JobLogger.start("Assessment Job Started executing", Option(Map("config" -> config, "model" -> name)))
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
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")
    val renamedDir = s"$tempDir/renamed"
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val reportDF = prepareReport(spark, loadData)
    val denormedDF = denormAssessment(spark, reportDF)
    saveReport(denormedDF, tempDir)
    JobLogger.end("AssessmentReport Generation Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(settings)
      .load()
  }

  override def saveReportES(reportDF: DataFrame): Unit = ???

  /**
    * Loading the specific tables from the cassandra db.
    */
  def prepareReport(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
    val sunbirdCoursesKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
    val courseBatchDF = loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
    val userCoursesDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
    val userDF = loadData(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
    val userOrgDF = loadData(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace)).filter(lower(col("isdeleted")) === "false")
    val organisationDF = loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
    val locationDF = loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
    val externalIdentityDF = loadData(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
    val assessmentProfileDF = loadData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))

    /*
    * courseBatchDF has details about the course and batch details for which we have to prepare the report
    * courseBatchDF is the primary source for the report
    * userCourseDF has details about the user details enrolled for a particular course/batch
    * */
    val userCourseDenormDF = courseBatchDF.join(userCoursesDF, userCoursesDF.col("batchid") === courseBatchDF.col("batchid") && lower(userCoursesDF.col("active")).equalTo("true"), "inner")
      .select(userCoursesDF.col("batchid"),
        col("userid"),
        col("active"),
        courseBatchDF.col("courseid"))

    /*
    *userCourseDenormDF lacks some of the user information that need to be part of the report
    *here, it will add some more user details
    * */
    val userDenormDF = userCourseDenormDF
      .join(userDF, Seq("userid"), "inner")
      .select(
        userCourseDenormDF.col("*"),
        col("firstname"),
        col("lastname"),
        col("maskedemail"),
        col("maskedphone"),
        col("rootorgid"),
        col("userid"),
        col("locationids"),
        concat_ws(" ", col("firstname"), col("lastname")).as("username"))
    /**
      * externalIdMapDF - Filter out the external id by idType and provider and Mapping userId and externalId
      */
    val externalIdMapDF = userDF.join(externalIdentityDF, externalIdentityDF.col("idtype") === userDF.col("channel") && externalIdentityDF.col("provider") === userDF.col("channel") && externalIdentityDF.col("userid") === userDF.col("userid"), "inner")
      .select(externalIdentityDF.col("externalid"), externalIdentityDF.col("userid"))

    /*
    * userDenormDF lacks organisation details, here we are mapping each users to get the organisationids
    * */
    val userRootOrgDF = userDenormDF
      .join(userOrgDF, userOrgDF.col("userid") === userDenormDF.col("userid") && userOrgDF.col("organisationid") === userDenormDF.col("rootorgid"))
      .select(userDenormDF.col("*"), col("organisationid"))

    val userSubOrgDF = userDenormDF
      .join(userOrgDF, userOrgDF.col("userid") === userDenormDF.col("userid") && userOrgDF.col("organisationid") =!= userDenormDF.col("rootorgid"))
      .select(userDenormDF.col("*"), col("organisationid"))

    val rootOnlyOrgDF = userRootOrgDF
      .join(userSubOrgDF, Seq("userid"), "leftanti")
      .select(userRootOrgDF.col("*"))

    val userOrgDenormDF = rootOnlyOrgDF.union(userSubOrgDF)

    /**
      * Get the District name for particular user based on the location identifiers
      */
    val locationDenormDF = userOrgDenormDF
      .withColumn("exploded_location", explode(col("locationids")))
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
      .dropDuplicates(Seq("userid"))
      .select(col("name").as("district_name"), col("userid"))

    val userLocationResolvedDF = userOrgDenormDF
      .join(locationDenormDF, Seq("userid"), "left_outer")


    val groupdedDF = Window.partitionBy("user_id", "batch_id", "course_id", "content_id").orderBy(desc("updated_on"))
    val latestAssessmentDF = assessmentProfileDF.withColumn("rownum", row_number.over(groupdedDF)).where(col("rownum") === 1).drop("rownum")

    /**
      * Compute the sum of all the worksheet contents score.
      */
    val assessmentAggDf = Window.partitionBy("user_id","batch_id","course_id","attempt_id")
    val aggregatedDF = latestAssessmentDF.withColumn("total_sum_score", sum("total_score") over assessmentAggDf)

    /**
      * Filter only valid enrolled userid for the specific courseid
      */
    val userAssessmentResolvedDF = userLocationResolvedDF.join(aggregatedDF, userLocationResolvedDF.col("userid") === aggregatedDF.col("user_id") && userLocationResolvedDF.col("batchid") === aggregatedDF.col("batch_id") && userLocationResolvedDF.col("courseid") === aggregatedDF.col("course_id"), "left_outer")
    val resolvedExternalIdDF = userAssessmentResolvedDF.join(externalIdMapDF, Seq("userid"), "left_outer")

    /*
    * Resolve organisation name from `rootorgid`
    * */
    val resolvedOrgNameDF = resolvedExternalIdDF
      .join(organisationDF, organisationDF.col("id") === resolvedExternalIdDF.col("rootorgid"), "left_outer")
      .dropDuplicates(Seq("userid"))
      .select(resolvedExternalIdDF.col("userid"), col("orgname").as("orgname_resolved"))


    /*
    * Resolve school name from `orgid`
    * */
    val resolvedSchoolNameDF = resolvedExternalIdDF
      .join(organisationDF, organisationDF.col("id") === resolvedExternalIdDF.col("organisationid"), "left_outer")
      .dropDuplicates(Seq("userid"))
      .select(resolvedExternalIdDF.col("userid"), col("orgname").as("schoolname_resolved"))


    /*
    * merge orgName and schoolName based on `userid` and calculate the course progress percentage from `progress` column which is no of content visited/read
    * */

    resolvedExternalIdDF
      .join(resolvedSchoolNameDF, Seq("userid"), "left_outer")
      .join(resolvedOrgNameDF, Seq("userid"), "left_outer")
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
  }

  /**
    *  De-norming the assessment report - Adding content name column to the content id
    * @return - Assessment denormalised dataframe
    */
  def denormAssessment(spark:SparkSession, report: DataFrame):DataFrame = {
    val contentIds = report.select(col("content_id")).rdd.map(r => r.getString(0)).collect.toList.distinct.filter(_!=null)
    val contentNameDF = getContentNames(spark, contentIds)
     report.join(contentNameDF, report.col("content_id") === contentNameDF.col("identifier"), "right_outer")
      .select(col("name"),
        col("attempt_id"), col("max_score"),col("total_sum_score"), report.col("userid"), report.col("courseid"), report.col("batchid"),
        col("total_score"), report.col("maskedemail"),report.col("district_name"),report.col("maskedphone"),
        report.col("orgname_resolved"),report.col("externalid"),report.col("schoolname_resolved"),report.col("username")
      )
  }

  /**
    * This method is used to upload the report the azure cloud service.
    */
  def saveReport(reportDF: DataFrame, url: String): Unit = {
    val courseids = reportDF.select(col("courseid")).distinct().collect()
    val batchids = reportDF.select(col("batchid")).distinct().collect()
    JobLogger.log(s"Number of courses are ${courseids.length} and number of batchs are ${batchids.length}")
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val courseList = courseids.map(x => x(0).toString)
    val batchList = batchids.map(x => x(0).toString)
    for (courseId <- courseList) {
      for (batchId <- batchList) {
        val resultDF = reportDF.filter(col("courseid") === courseId && col("batchid") === batchId).groupBy("courseid", "batchid", "userid", "maskedemail", "max_score", "district_name", "maskedphone", "orgname_resolved", "externalid", "schoolname_resolved", "username", "total_sum_score").pivot("name").agg(first("total_score"))
        if (resultDF.count() > 0) {
          resultDF.select(
            col("externalid").as("External ID"),
            col("userid").as("User ID"),
            col("username").as("User Name"),
            col("maskedemail").as("Email ID"),
            col("maskedphone").as("Mobile Number"),
            col("orgname_resolved").as("Organisation Name"),
            col("district_name").as("District Name"),
            col("schoolname_resolved").as("School Name"),
            col("*"),
            col("total_sum_score").as("Total Score")).drop("userid", "externalid", "username", "maskedemail", "maskedphone", "orgname_resolved", "district_name", "schoolname_resolved", "max_score","total_sum_score")
            .coalesce(1).write.partitionBy("batchid", "courseid")
            .mode("overwrite")
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .save(url)
          renameReport(tempDir, renamedDir)
          uploadReport(renamedDir)
        }
      }
    }
  }

  /**
    * Upload reports to azure cloud storage
    */
  def uploadReport(sourcePath: String) = {
    val provider = AppConf.getConfig("assessment.metrics.cloud.provider")
    val container = AppConf.getConfig("assessment.metrics.cloud.container")
    val objectKey = AppConf.getConfig("assessment.metrics.cloud.objectKey")

    val storageService = StorageServiceFactory
      .getStorageService(StorageConfig(provider, AppConf.getStorageKey(provider), AppConf.getStorageSecret(provider)))
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))

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

  /**
    * This method is used to get the content names from the elastic search
    * @param spark
    * @param contentIds
    * @return
    */
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
      .select("name", "identifier")
  }
}

