package org.ekstep.analytics.updater

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory

import scala.collection.Map

class TestUpdateCourseMetrics extends SparkSpec( file = null) with MockFactory{
    var spark: SparkSession = _
    var courseBatchDF: DataFrame = _
    var userCoursesDF: DataFrame = _
    var userDF: DataFrame = _
    var locationDF: DataFrame = _
    var orgDF: DataFrame = _
    var userOrgDF: DataFrame = _

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

        val reporterMock = mock[Reporter]

            (reporterMock.fetchTable _)
                .expects(spark, Map( "table" -> "course_batch", "keyspace" -> "sunbird"))
                .returning(courseBatchDF).once()

            (reporterMock.fetchTable _)
                .expects(spark, Map("table" -> "user_courses", "keyspace" -> "sunbird"))
                .returning(userCoursesDF).once()

            (reporterMock.fetchTable _)
                .expects(spark, Map("table" -> "user", "keyspace" -> "sunbird"))
                .returning(userDF).once()

            (reporterMock.fetchTable _)
                .expects(spark, Map("table" -> "user_org", "keyspace" -> "sunbird"))
                .returning(userOrgDF).once()

            (reporterMock.fetchTable _)
                .expects(spark, Map("table" -> "organisation", "keyspace" -> "sunbird"))
                .returning(orgDF).once()

            (reporterMock.fetchTable _)
                .expects(spark, Map("table" -> "location", "keyspace" -> "sunbird"))
                .returning(locationDF).once()

        val reportDF = UpdateCourseMetrics.prepareReport(spark, reporterMock.fetchTable)

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
        val url = "/Users/sunil/Downloads/course_report/dashboard"
        UpdateCourseMetrics.uploadReportToCloud(reportDF, url)
    }
}