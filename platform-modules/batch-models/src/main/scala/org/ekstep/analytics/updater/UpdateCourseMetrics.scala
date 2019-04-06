package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework._
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.{Map, _}

trait Reporter {
    def fetchTable(spark: SparkSession, settings: Map[String, String]): DataFrame
    def prepareReport(spark: SparkSession, fetchTable: (SparkSession, Map[String, String]) => DataFrame): DataFrame
    def saveReportToES(reportDF: DataFrame): Unit
    def uploadReportToCloud(reportDF: DataFrame, url: String): Unit
}

object UpdateCourseMetrics extends IBatchModelTemplate[Empty, Empty, Empty, Empty] with Serializable with Reporter {

    val className = "org.ekstep.analytics.updater.UpdateCourseMetrics"
    override def name(): String = "UpdateCourseMetrics"

    override def preProcess(data: RDD[Empty], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        val accName = AppConf.getStorageKey("azure")

        //val url = s"wasbs://course-metrics@$accName.blob.core.windows.net/reports"
        val url = "/Users/sunil/Downloads/course_report/dashboard"

        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
        val reportDF = prepareReport(spark, fetchTable)
        //uploadReportToCloud(reportDF, url)
        //saveReportToES(reportDF)
        sc.emptyRDD[Empty]
    }

    val stringifySeqUDF = udf((value: Seq[String]) => value match {
        case null => null
        case _    => s"${value.mkString(",")}"
    })

    def fetchTable(spark: SparkSession, settings: Map[String, String]): DataFrame = {
        spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(settings)
            .load()
    }

    def prepareReport(spark: SparkSession, fetchTable: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
        val courseBatchDF = fetchTable(spark, Map( "table" -> "course_batch", "keyspace" -> "sunbird"))
        val userCoursesDF = fetchTable(spark, Map( "table" -> "user_courses", "keyspace" -> "sunbird"))
        val userDF = fetchTable(spark, Map( "table" -> "user", "keyspace" -> "sunbird"))
        val userOrgDF = fetchTable(spark, Map( "table" -> "user_org", "keyspace" -> "sunbird"))
        val organisationDF = fetchTable(spark, Map( "table" -> "organisation", "keyspace" -> "sunbird"))
        val locationDF = fetchTable(spark, Map( "table" -> "location", "keyspace" -> "sunbird"))

        /*
        * courseBatchDF has details about the course and batch details for which we have to prepare the report
        * courseBatchDF is the primary source for the report
        * userCourseDF has details about the user details enrolled for a particular course/batch
        * */
        val userCourseDenormDF = courseBatchDF.join(userCoursesDF, userCoursesDF.col("batchid") === courseBatchDF.col("id") && lower(userCoursesDF.col("active")).equalTo("true"), "inner")
            .select(col("batchid"),
                col("userid"),
                col("leafnodescount"),
                col("progress"),
                col("enddate"),
                col("startdate"),
                col("enrolleddate"),
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
                col("email"),
                col("phone"),
                col("rootorgid"),
                col("locationids"))

        /*
        * userDenormDF lacks organisation details, here we are mapping each users to get the organisationids
        * */
        val userOrgDenormDF =  userDenormDF
            .join(userOrgDF, userOrgDF.col("userid") === userDenormDF.col("userid") && lower(userOrgDF.col("isdeleted")).equalTo("false"), "inner")
            .select(userDenormDF.col("*"), col("organisationid"))
        /*
        * User table `loctionids` column is list of locationids for state, district, block, cluster.
        * list is flatten out here into multiple columns as below.
        * */
        val userLocationDF = userOrgDenormDF
            .select(col("*"),
                col("locationids")(0).as("loc1"),
                col("locationids")(1).as("loc2"),
                col("locationids")(2).as("loc3"),
                col("locationids")(3).as("loc4"))
        /*
        * here, each locationids in the columns `loc*` can be mapped to either state or district or block or cluster.
        * we have to map the loctionid and filter by district to find out the district name
        * */
        val userLoc1DF =  userLocationDF
            .join(locationDF, locationDF.col("id") === userLocationDF.col("loc1") and locationDF.col("type") === "district", "inner")
            .select(userLocationDF.col("userid"), locationDF.col("name").as("loc1_name"))
        val userLoc2DF =  userLocationDF
            .join(locationDF, locationDF.col("id") === userLocationDF.col("loc2") and locationDF.col("type") === "district", "inner")
            .select(userLocationDF.col("userid"), locationDF.col("name").as("loc2_name"))
        val userLoc3DF =  userLocationDF
            .join(locationDF, locationDF.col("id") === userLocationDF.col("loc3") and locationDF.col("type") === "district", "inner")
            .select(userLocationDF.col("userid"), locationDF.col("name").as("loc3_name"))
        val userLoc4DF =  userLocationDF
            .join(locationDF, locationDF.col("id") === userLocationDF.col("loc4") and locationDF.col("type") === "district", "inner")
            .select(userLocationDF.col("userid"), locationDF.col("name").as("loc4_name"))

        /*
        * merge all the `userLoc*DenormDF` to `userLocationDF` to have all the details in one Dataframe
        * */
        val userLocationDenormDF =  userLocationDF
            .join(userLoc1DF, Seq("userid"), "FullOuter")
            .join(userLoc2DF, Seq("userid"), "FullOuter")
            .join(userLoc3DF, Seq("userid"), "FullOuter")
            .join(userLoc4DF, Seq("userid"), "FullOuter")
            .drop("loc1", "loc2", "loc3", "loc4")

        val toDistrictNameUDF = udf((loc1: String, loc2: String, loc3: String, loc4: String) => {
            Seq(Option(loc1), Option(loc2), Option(loc3), Option(loc4)).flatten
        })

        /*
        * combine the `loc*_name` fields to get the district name
        * */
        val foldLocationNameDF = userLocationDenormDF
            .withColumn("district_name",
                toDistrictNameUDF(col("loc1_name"),
                    col("loc2_name"),
                    col("loc3_name"),
                    col("loc4_name")
                ))
            .drop("loc1_name", "loc2_name", "loc3_name", "loc4_name")

        /*
        * Resolve organisation name from `rootorgid`
        * */
        val resolvedOrgNameDF = foldLocationNameDF
            .join(organisationDF, organisationDF.col("id") === userLocationDenormDF.col("rootorgid"))
            .select(foldLocationNameDF.col("userid"), col("orgname").as("orgname_resolved"))

        /*
        * Resolve school name from `orgid`
        * */
        val resolvedSchoolNameDF = foldLocationNameDF
            .join(organisationDF, organisationDF.col("id") === userLocationDenormDF.col("organisationid"))
            .select(foldLocationNameDF.col("userid"), col("orgname").as("schoolname_resolved"))

        val toLongUDF = udf((value: Double) => value.toLong)

        /*
        * merge orgName and schoolName based on `userid` and calculate the course progress percentage from `progress` column which is no of content visited/read
        * */
        val resolvedDF = resolvedOrgNameDF
            .join(resolvedSchoolNameDF, Seq("userid"))
            .join(foldLocationNameDF, Seq("userid"))
            .select(foldLocationNameDF.col("*"), resolvedSchoolNameDF.col("schoolname_resolved"), resolvedOrgNameDF.col("orgname_resolved"))
            .withColumn("course_completion", toLongUDF(round(expr("progress/leafnodescount * 100"))))
            .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ssXXX'Z'"))
        resolvedDF
    }



    def saveReportToES(reportDF: DataFrame): Unit = {

        import org.elasticsearch.spark.sql._
        val participantsCountPerBatchDF = reportDF.groupBy(col("batchid"))
            .agg(count("*").as("participantsCountPerBatch"))

        val courseCompletionCountPerBatchDF = reportDF.groupBy(col("batchid"))
            .agg(count(col("course_completion") === "100%").as("courseCompletionCountPerBatch"))

        val ESDocDF = participantsCountPerBatchDF
            .join(courseCompletionCountPerBatchDF, Seq("batchid"))
            .join(reportDF, Seq("batchid"))
            .select(
                concat_ws(" ", col("firstname"), col("lastname")).as("name"),
                col("email").as("maskedEmail"),
                col("phone").as("maskedPhone"),
                col("orgname_resolved").as("rootOrgName"),
                col("schoolname_resolved").as("subOrgName"),
                col("startdate").as("startDate"),
                col("enddate").as("endDate"),
                col("courseid").as("courseId"),
                col("generatedOn").as("lastUpdatedOn"),
                col("batchid").as("batchId"),
                col("participantsCountPerBatch").as("participantCount"),
                col("courseCompletionCountPerBatch").as("completedCount"),
                col("course_completion").as("completedPercent"),
                stringifySeqUDF(col("district_name")).as("districtName"),
                date_format(col("enrolleddate"), "yyyy-MM-dd'T'HH:mm:ssXXX'Z'").as("enrolledOn")
            )

        //Save to sunbird platform Elasticsearch instance
        ESDocDF.saveToEs("cbatchstats/doc")
    }

    def uploadReportToCloud(reportDF: DataFrame, url: String): Unit = {

        val toPercentageStringUDF = udf((value: Double) => s"${value.toInt}%")

        val defaultProgressUDF = udf((value: String) => value match {
            case null => "100%"
            case _    => value
        })

        reportDF
            .select(
                concat_ws(" ", col("firstname"), col("lastname")).as("User Name"),
                col("batchid"),
                col("email").as("Email ID"),
                col("phone").as("Mobile Number"),
                stringifySeqUDF(col("district_name")).as("District Name"),
                col("orgname_resolved").as("Organisation Name"),
                col("schoolname_resolved").as("School Name"),
                defaultProgressUDF(toPercentageStringUDF(col("course_completion"))).as("Course Progress"),
                col("generatedOn").as("last updated")
            )
            .coalesce(1)
            .write
            .partitionBy("batchid")
            .mode("overwrite")
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .save(url)
    }

    override def algorithm(events: RDD[Empty], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        sc.emptyRDD[Empty]
    }

    override def postProcess(data: RDD[Empty], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext) : RDD[Empty] = {
        sc.emptyRDD[Empty]
    }
}
