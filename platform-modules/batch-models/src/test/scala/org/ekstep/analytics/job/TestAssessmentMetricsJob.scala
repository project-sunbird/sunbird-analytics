package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.job.AssessmentMetricsJob.saveReport
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.AssessmentReportUtil
import org.scalamock.scalatest.MockFactory
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.Map


class TestAssessmentMetricsJob extends SparkSpec(null) with MockFactory {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var userOrgDF: DataFrame = _
  var externalIdentityDF: DataFrame = _
  var assessmentProfileDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder.config(sc.getConf.set("es.nodes", AppConf.getConfig("es.host"))).getOrCreate()
    //spark = SparkSession.builder.config(sc.getConf.set("es.nodes","11.2.3.58")).getOrCreate()

    /*
     * Data created with 31 active batch from batchid = 1000 - 1031
     * */
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/courseBatchTable.csv")
      .cache()

    externalIdentityDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/usr_external_identity.csv")
      .cache()

    assessmentProfileDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/assessment.csv")
      .cache()


    /*
     * Data created with 35 participants mapped to only batch from 1001 - 1010 (10), so report
     * should be created for these 10 batch (1001 - 1010) and 34 participants (1 user is not active in the course)
     * and along with 5 existing users from 31-35 has been subscribed to another batch 1003-1007 also
     * */
    userCoursesDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/userCoursesTable.csv")
      .cache()

    /*
     * This has users 30 from user001 - user030
     * */
    userDF = spark
      .read
      .json("src/test/resources/assessment-metrics-updater/userTable.json")
      .cache()

    /*
     * This has 30 unique location
     * */
    locationDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/locationTable.csv")
      .cache()

    /*
     * There are 8 organisation added to the data, which can be mapped to `rootOrgId` in user table
     * and `organisationId` in userOrg table
     * */
    orgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/orgTable.csv")
      .cache()

    /*
     * Each user is mapped to organisation table from any of 8 organisation
     * */
    userOrgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/userOrgTable.csv")
      .cache()
  }

  "TestAssessmentMetricsJob" should "define all the configurations" in {
    assert(AppConf.getConfig("assessment.metrics.content.index").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cassandra.input.consistency").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cloud.objectKey").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cloud.provider").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cloud.container").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.temp.dir").isEmpty === false)
  }

  "TestUpdateAssessmentMetricsJob" should "generate reports" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    //println(reportDF.show(false))
    val denormedDF = AssessmentMetricsJob.denormAssessment(spark, reportDF)
    saveReport(denormedDF, tempDir)
    assert(denormedDF.count == 7)
  }

  "TestAssessmentMetricsJob" should "presnet 1001 batchid and do_2123101488779837441168 courseid " in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(spark, reportDF)
    val denormedDFCount = denormedDF.groupBy("courseid", "batchid")
    denormedDF.createOrReplaceTempView("course_batch")
    val df = spark.sql("select * from course_batch where batchid ='1001' and courseid='do_2123101488779837441168' and name='Whole Numbers' and userid='user021' ")
    println(df.show(false))
    assert(df.count() === 1)
  }

  "TestAssessmentMetricsJob" should "fetch the content names from the elastic search" in {
    val contentESIndex = AppConf.getConfig("assessment.metrics.content.index")
    assert(contentESIndex.isEmpty === false)
    val contentList = List("do_112835336280596480151", "do_112835336960000000152")
    val contentDF = AssessmentReportUtil.getContentNames(spark, contentList)
    assert(contentDF.count() === 2)
  }


  "TestAssessmentMetricsJob" should "have computed total score for the specific userid, batch, course" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(spark, reportDF)
    val denormedDFCount = denormedDF.groupBy("courseid", "batchid")
    denormedDF.createOrReplaceTempView("course_batch")
    val df = spark.sql("select * from course_batch where batchid =1006 and courseid='do_1126458775024025601296' and  userid='user026' ")
    println(df.show(false))
    assert(df.count() === 1)
    df.select("total_sum_score").take(0).foreach(value => {
      println("e is" + value)
      assert(value === "10.0")
    })

  }

  "TestAssessmentMetricsJob" should "valid score in the assessment content for the specific course" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(spark, reportDF)
    val denormedDFCount = denormedDF.groupBy("courseid", "batchid")
    denormedDF.createOrReplaceTempView("course_batch")
    val df = spark.sql("select * from course_batch where batchid ='1005' and courseid='do_112695422838472704115' and name ='TEST'")
    println(df.show(false))
    assert(df.count() === 1)
    df.select("total_score").take(0).foreach(value => {
      assert(value === "30")
    })
  }

  "TestAssessmentMetricsJob" should "confirm all required column names are present or not" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(spark, reportDF)
    assert(denormedDF.columns.contains("name") === true)
    assert(denormedDF.columns.contains("total_score") === true)
    assert(denormedDF.columns.contains("courseid") === true)
    assert(denormedDF.columns.contains("userid") === true)
    assert(denormedDF.columns.contains("batchid") === true)
    assert(denormedDF.columns.contains("maskedemail") === true)
    assert(denormedDF.columns.contains("district_name") === true)
    assert(denormedDF.columns.contains("maskedphone") === true)
    assert(denormedDF.columns.contains("orgname_resolved") === true)
    assert(denormedDF.columns.contains("district_name") === true)
    assert(denormedDF.columns.contains("externalid") === true)
    assert(denormedDF.columns.contains("schoolname_resolved") === true)
    assert(denormedDF.columns.contains("username") === true)
  }

}