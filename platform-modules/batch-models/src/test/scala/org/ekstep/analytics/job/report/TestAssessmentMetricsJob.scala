package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.job.report.{AssessmentMetricsJob, BaseReportSpec, ReportGenerator}
import org.ekstep.analytics.util.{ESUtil, EmbeddedES}
import org.scalamock.scalatest.MockFactory
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.Map
import scala.collection.mutable.Buffer

class TestAssessmentMetricsJob extends BaseReportSpec with MockFactory {

  implicit var spark: SparkSession = _

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
    spark = getSparkSession();

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

    EmbeddedES.loadData("compositesearch", "cs", Buffer(
      """{"contentType":"SelfAssess","name":"My content 1","identifier":"do_112835335135993856149"}""",
      """{"contentType":"SelfAssess","name":"My content 2","identifier":"do_112835336280596480151"}""",
      """{"contentType":"SelfAssess","name":"My content 3","identifier":"do_112832394979106816112"}"""
    ))
  }

  "AssessmentMetricsJob" should "define all the configurations" in {
    assert(AppConf.getConfig("assessment.metrics.bestscore.report").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.content.index").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cassandra.input.consistency").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cloud.objectKey").isEmpty === false)
    assert(AppConf.getConfig("cloud.container.reports").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.temp.dir").isEmpty === false)
    assert(AppConf.getConfig("course.upload.reports.enabled").isEmpty === false)
    assert(AppConf.getConfig("course.es.index.enabled").isEmpty === false)
  }

  it should "Ensure for the user `user030` should have all proper records values" in {
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
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    denormedDF.createOrReplaceTempView("report_df")
    val content1_DF = spark.sql("select * from report_df where userid ='user030' and content_name = 'My content 1' ")
    println(content1_DF.show(100, false));

    val username = content1_DF.select("username").collect().map(_ (0)).toList
    assert(username(0) === "Karishma Kapoor")

    val total_score_value = content1_DF.select("total_sum_score").collect().map(_ (0)).toList
    assert(total_score_value(0) === "10.0/20.0")

    val courseid_value = content1_DF.select("courseid").collect().map(_ (0)).toList
    assert(courseid_value(0) === "do_1125559882615357441175")

    val batchid_value = content1_DF.select("batchid").collect().map(_ (0)).toList
    assert(batchid_value(0) === "1010")

    val district_name = content1_DF.select("district_name").collect().map(_ (0)).toList
    assert(district_name(0) === "GULBARGA")

    val org_name = content1_DF.select("orgname_resolved").collect().map(_ (0)).toList
    assert(org_name(0) === "SACRED HEART(B)PS,TIRUVARANGAM")

    val schoolname = content1_DF.select("schoolname_resolved").collect().map(_ (0)).toList
    assert(schoolname(0) === "RAILWAY MIXED HIGH SCHOOL, ARAKKONAM VELLORE")


  }

  it should "Sort and get the best score" in {
    val df = spark.createDataFrame(Seq(
      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-04 09:59:51.000+0000", "10", "5", "2019-09-06 09:59:51.000+0000", "10/2", ""),
      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-05 09:59:51.000+0000", "12", "4", "2019-09-06 09:59:51.000+0000", "10/2", "")
    )).toDF("content_id", "attempt_id", "user_id", "course_id", "batch_id", "created_on", "last_attempted_on", "total_max_score", "total_score", "updated_on", "grand_total", "question")
    val bestScoreDF = AssessmentMetricsJob.getAssessmentData(df);
    val batchid_value = bestScoreDF.select("total_score").collect().map(_ (0)).toList
    assert(batchid_value(0) === "5")
  }

  it should "Ensure CSV Report Should have all proper columns names" in {
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
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    val finalReport = AssessmentMetricsJob.transposeDF(denormedDF)
    val column_names = finalReport.columns
    // Validate the column names are proper or not.
    assert(column_names.contains("External ID") === true)
    assert(column_names.contains("User ID") === true)
    assert(column_names.contains("User Name") === true)
    assert(column_names.contains("Email ID") === true)
    assert(column_names.contains("Mobile Number") === true)
    assert(column_names.contains("Organisation Name") === true)
    assert(column_names.contains("District Name") === true)
    assert(column_names.contains("School Name") === true)
    assert(column_names.contains("Total Score") === true)
  }

  it should "generate reports" in {
    implicit val mockFc = mock[FrameworkContext];
    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String, _: String, _: String)).expects(*, *, *).returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload _).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
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
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    AssessmentMetricsJob.saveReport(denormedDF, tempDir)

  }

  it should "fetch the content names from the elastic search" in {
    val contentESIndex = AppConf.getConfig("assessment.metrics.content.index")
    assert(contentESIndex.isEmpty === false)
    val contentList = List("do_112835335135993856149", "do_112835336280596480151")
    val contentDF = ESUtil.getAssessmentNames(spark, contentList, AppConf.getConfig("assessment.metrics.content.index"), AppConf.getConfig("assessment.metrics.supported.contenttype"))
    assert(contentDF.count() === 2)
  }

  it should "generate reports for the best score" in {

    assessmentProfileDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/assessment_best_score.csv")
      .cache()

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
    reportDF.createOrReplaceTempView("best_attempt")
    val df = spark.sql("select * from best_attempt where batchid ='1010' and courseid='do_1125559882615357441175' and content_id='do_112835335135993856149' and userid ='user030'")
    val best_attempt_score = df.select("total_score").collect().map(_ (0)).toList
    assert(best_attempt_score.head === "50")
  }

}