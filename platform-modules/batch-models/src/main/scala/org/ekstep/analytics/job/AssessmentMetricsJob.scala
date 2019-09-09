package org.ekstep.analytics.job


import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.DataTypes
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.util.AssessmentReportUtil
import org.sunbird.cloud.storage.conf.AppConf

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

    // Enable this below code to get only last attempted question
    //val groupdedDF = Window.partitionBy("user_id", "batch_id", "course_id", "content_id").orderBy(desc("last_attempted_on"))
    //val latestAssessmentDF = assessmentProfileDF.withColumn("rownum", row_number.over(groupdedDF)).where(col("rownum") === 1).drop("rownum")


    /** attempt_id
      * Compute the sum of all the worksheet contents score.
      */
    val assessmentAggDf = Window.partitionBy("user_id", "batch_id", "course_id")
    val aggregatedDF = assessmentProfileDF.withColumn("total_sum_score", sum("total_score") over assessmentAggDf)

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
    * De-norming the assessment report - Adding content name column to the content id
    *
    * @return - Assessment denormalised dataframe
    */
  def denormAssessment(spark: SparkSession, report: DataFrame): DataFrame = {
    val contentIds = report.select(col("content_id")).rdd.map(r => r.getString(0)).collect.toList.distinct.filter(_ != null)
    val contentNameDF = AssessmentReportUtil.getContentNames(spark, contentIds)
    report.join(contentNameDF, report.col("content_id") === contentNameDF.col("identifier"), "right_outer")
      .select(col("name"),
        col("max_score"), col("total_sum_score"), report.col("userid"), report.col("courseid"), report.col("batchid"),
        col("total_score"), report.col("maskedemail"), report.col("district_name"), report.col("maskedphone"),
        report.col("orgname_resolved"), report.col("externalid"), report.col("schoolname_resolved"), report.col("username")
      )
  }

  /**
    * This method is used to upload the report the azure cloud service.
    */
  def saveReport(reportDF: DataFrame, url: String): Unit = {
    val course_batch_df = reportDF.select("courseid", "batchid").collect()
    JobLogger.log(s"Number of courses are ${course_batch_df.length} and number of batchs are ${course_batch_df.length}")
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val courseList = course_batch_df.map(x => x(0).toString).distinct
    val batchList = course_batch_df.map(x => x(1).toString).distinct
    courseList.foreach(courseId => {
      batchList.foreach(batchId => {
        val reshapedDF = reportDF.filter(col("courseid") === courseId && col("batchid") === batchId).
          groupBy("courseid", "batchid", "userid").pivot("name").agg(first("total_score"))
        val resultDF = reshapedDF.join(reportDF, reshapedDF.col("courseid") === reportDF.col("courseid") && reshapedDF.col("batchid") === reportDF.col("batchid") && reshapedDF.col("userid") === reportDF.col("userid"), "left_outer")
          .select(
            reportDF.col("externalid").as("External ID"),
            reportDF.col("userid").as("User ID"),
            reportDF.col("username").as("User Name"),
            reportDF.col("maskedemail").as("Email ID"),
            reportDF.col("maskedphone").as("Mobile Number"),
            reportDF.col("orgname_resolved").as("Organisation Name"),
            reportDF.col("district_name").as("District Name"),
            reportDF.col("schoolname_resolved").as("School Name"),
            reshapedDF.col("*"),
            reportDF.col("total_sum_score").as("Total Score")
          ).dropDuplicates("userid", "courseid", "batchid").drop("userid")
        if (!resultDF.take(1).isEmpty) {
          resultDF.coalesce(1).write.partitionBy("batchid", "courseid")
            .mode("overwrite")
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .save(url)
          AssessmentReportUtil.renameReport(tempDir, renamedDir)
          AssessmentReportUtil.uploadReport(renamedDir)
        }
      })
    })
  }
}
