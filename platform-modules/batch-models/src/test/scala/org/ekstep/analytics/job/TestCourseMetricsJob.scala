package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import scala.collection.Map

class TestCourseMetricsJob extends SparkSpec(null) with MockFactory {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var userOrgDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator]

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    /*
     * Data created with 31 active batch from batchid = 1000 - 1031
     * */
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/courseBatchTable.csv")

    /*
     * Data created with 30 participants mapped to only batch from 1001 - 1010 (10), so report
     * should be created for these 10 batch (1001 - 1010) and 29 participants (1 user is not active in the course)
     * */
    userCoursesDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/userCoursesTable.csv")

    /*
     * This has users 30 from user001 - user030
     * */
    userDF = spark.read.json("src/test/resources/course-metrics-updater/userTable.json")

    /*
     * This has 30 unique location
     * */
    locationDF = spark.read.format("com.databricks.spark.csv").option("header", "true").load("src/test/resources/course-metrics-updater/locationTable.csv")

    /*
     * There are 8 organisation added to the data, which can be mapped to `rootOrgId` in user table
     * and `organisationId` in userOrg table
     * */
    orgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/orgTable.csv")

    /*
     * Each user is mapped to organisation table from any of 8 organisation
     * */
    userOrgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/userOrgTable.csv")
  }


  "TestUpdateCourseMetrics" should "generate reports" in {

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> "sunbird"))
      .returning(courseBatchDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> "sunbird"))
      .returning(userCoursesDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> "sunbird"))
      .returning(userDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> "sunbird"))
      .returning(userOrgDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> "sunbird"))
      .returning(orgDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> "sunbird"))
      .returning(locationDF).atLeastOnce()

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)

    assert(reportDF.count == 29)
    assert(reportDF.groupBy(col("batchid")).count().count() == 10)

    val reportData = reportDF
      .groupBy(col("batchid"))
      .count()
      .collect()

    assert(reportData.filter(row => row.getString(0) == "1001").head.getLong(1) == 2)
    assert(reportData.filter(row => row.getString(0) == "1002").head.getLong(1) == 3)
    assert(reportData.filter(row => row.getString(0) == "1003").head.getLong(1) == 3)
    assert(reportData.filter(row => row.getString(0) == "1004").head.getLong(1) == 3)
    assert(reportData.filter(row => row.getString(0) == "1005").head.getLong(1) == 3)
    assert(reportData.filter(row => row.getString(0) == "1006").head.getLong(1) == 3)
    assert(reportData.filter(row => row.getString(0) == "1007").head.getLong(1) == 3)
    assert(reportData.filter(row => row.getString(0) == "1008").head.getLong(1) == 3)
    assert(reportData.filter(row => row.getString(0) == "1009").head.getLong(1) == 3)
    assert(reportData.filter(row => row.getString(0) == "1010").head.getLong(1) == 3)

    CourseMetricsJob.saveReportES(reportDF)
  }

  it should "should calculate the progress" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> "sunbird"))
      .returning(courseBatchDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> "sunbird"))
      .returning(userCoursesDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> "sunbird"))
      .returning(userDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> "sunbird"))
      .returning(userOrgDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> "sunbird"))
      .returning(orgDF).atLeastOnce()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> "sunbird"))
      .returning(locationDF).atLeastOnce()

    val reportDF = CourseMetricsJob.prepareReport(spark, reporterMock.loadData)

    //sampling report
    val data1 = reportDF
      .select("course_completion")
      .where(col("batchid") === "1007" and col("userid") === "user017")
      .collect()

    assert(data1.head.getDouble(0) == 65)

    val data2 = reportDF
      .select("course_completion")
      .where(col("batchid") === "1009" and col("userid") === "user019")
      .collect()

    assert(data2.head.getDouble(0) == 92.5)
  }

  it should "show course_completion as 100% if no. of leafnodeCount is 0" in {

  }
}