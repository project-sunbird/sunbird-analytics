package org.ekstep.analytics.updater

import org.apache.spark.sql.{DataFrame, SparkSession}
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

        courseBatchDF = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .load("src/test/resources/course-metrics-updater/courseBatchTable.csv")

        userCoursesDF = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .load("src/test/resources/course-metrics-updater/userCoursesTable.csv")

        userDF = spark
            .read
            .json("src/test/resources/course-metrics-updater/userTable.json")

        locationDF = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .load("src/test/resources/course-metrics-updater/locationTable.csv")

        orgDF = spark
            .read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .load("src/test/resources/course-metrics-updater/orgTable.csv")

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
        // assert report data
    }
}