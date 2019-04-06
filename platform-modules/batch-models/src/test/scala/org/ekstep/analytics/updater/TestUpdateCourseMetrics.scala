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
        userDF = spark
            .read
            .json("src/test/resources/course-metrics-updater/userTable.json")

        /*
        * This has 30 unique location
        * */
        locationDF = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .load("src/test/resources/course-metrics-updater/locationTable.csv")

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

        reportDF
            .groupBy(col("batchid"))
            .count()
            .select(col("count"))
            .collect.map { r =>
                r.get

            }

        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .select(col("count"))
            .where(col("batchid") === "1002").collect.map(_.getLong(0)).head == 3)
        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .where(col("batchid") === "1003").collect.map(_.getLong(0)).head == 3)
        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .where(col("batchid") === "1004").collect.map(_.getLong(0)).head == 3)
        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .where(col("batchid") === "1005").collect.map(_.getLong(0)).head == 3)
        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .where(col("batchid") === "1006").collect.map(_.getLong(0)).head == 3)
        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .where(col("batchid") === "1007").collect.map(_.getLong(0)).head == 3)
        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .where(col("batchid") === "1008").collect.map(_.getLong(0)).head == 3)
        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .where(col("batchid") === "1009").collect.map(_.getLong(0)).head == 3)
        assert(reportDF
            .groupBy(col("batchid"))
            .count()
            .where(col("batchid") === "1010").collect.map(_.getLong(0)).head == 3)
//        val url = "/Users/sunil/Downloads/course_report/dashboard"
//        UpdateCourseMetrics.uploadReportToCloud(reportDF, url)
    }
}