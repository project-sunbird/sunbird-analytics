package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.ekstep.analytics.framework._
import org.elasticsearch.spark.rdd.EsSpark
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.{Map, _}

object UpdateCourseMetrics extends IBatchModelTemplate[Empty, Empty, Empty, Empty] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateCourseMetrics"
    override def name(): String = "UpdateCourseMetrics"

    override def preProcess(data: RDD[Empty], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        val reportDF = prepareReport(sc)
        uploadReportToCloud(reportDF)
        saveReportToES(reportDF)
        sc.emptyRDD[Empty]
    }

    val stringifySeqUDF = udf((value: Seq[String]) => value match {
        case null => null
        case _    => s"${value.mkString(",")}"
    })

    def prepareReport(sc: SparkContext): DataFrame = {
        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
        val courseBatchDF = spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map[String, String]( "table" -> "course_batch", "keyspace" -> "sunbird"))
            .load()

        val userCoursesDF = spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map[String, String]( "table" -> "user_courses", "keyspace" -> "sunbird"))
            .load()

        val userDF = spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map[String, String]( "table" -> "user", "keyspace" -> "sunbird"))
            .load()

        val userOrgDF = spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map[String, String]( "table" -> "user_org", "keyspace" -> "sunbird"))
            .load()

        val organisationDF = spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map[String, String]( "table" -> "organisation", "keyspace" -> "sunbird"))
            .load()

        val locationDF = spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map[String, String]( "table" -> "location", "keyspace" -> "sunbird"))
            .load()


        /*
        * courseBatchDF has details about the course and batch details for which we have to prepare the report
        * courseBatchDF is the primary source for the report
        * userCourseDF has details about the user details enrolled for a particular course/batch
        * */
        val userCourseDenormDF = courseBatchDF.join(userCoursesDF, userCoursesDF.col("batchid") === courseBatchDF.col("id"), "inner")
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
            .join(userOrgDF, userOrgDF.col("userid") === userDenormDF.col("userid") && userOrgDF.col("isdeleted") === false, "inner")
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
        val userLoc1DenormDF =  userLocationDF
            .join(locationDF, locationDF.col("id") === userLocationDF.col("loc1") and locationDF.col("type") === "district", "inner")
            .select(userLocationDF.col("userid"), locationDF.col("name").as("loc1_name"))
        val userLoc2DenormDF =  userLocationDF
            .join(locationDF, locationDF.col("id") === userLocationDF.col("loc2") and locationDF.col("type") === "district", "inner")
            .select(userLocationDF.col("userid"), locationDF.col("name").as("loc2_name"))
        val userLoc3DenormDF =  userLocationDF
            .join(locationDF, locationDF.col("id") === userLocationDF.col("loc3") and locationDF.col("type") === "district", "inner")
            .select(userLocationDF.col("userid"), locationDF.col("name").as("loc3_name"))
        val userLoc4DenormDF =  userLocationDF
            .join(locationDF, locationDF.col("id") === userLocationDF.col("loc4") and locationDF.col("type") === "district", "inner")
            .select(userLocationDF.col("userid"), locationDF.col("name").as("loc4_name"))

        /*
        * merge all the `userLoc*DenormDF` to `userLocationDF` to have all the details in one Dataframe
        * */
        val userLocationDenormDF =  userLocationDF
            .join(userLoc1DenormDF, Seq("userid"), "FullOuter")
            .join(userLoc2DenormDF, Seq("userid"), "FullOuter")
            .join(userLoc3DenormDF, Seq("userid"), "FullOuter")
            .join(userLoc4DenormDF, Seq("userid"), "FullOuter")
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
            .select(foldLocationNameDF.col("*"), col("orgname").as("orgname_resolved"))

        /*
        * Resolve school name from `orgid`
        * */
        val resolvedSchoolNameDF = foldLocationNameDF
            .join(organisationDF, organisationDF.col("id") === userLocationDenormDF.col("organisationid"))
            .select(foldLocationNameDF.col("*"), col("orgname").as("schoolname_resolved"))

        val resolvedDF = resolvedOrgNameDF
            .join(resolvedSchoolNameDF, Seq("userid")).select(resolvedOrgNameDF.col("*"), resolvedSchoolNameDF.col("schoolname_resolved"))

        val toPercentageUDF = udf((value: Double) => s"${value.toInt}%")

        val defaultProgressUDF = udf((value: String) => value match {
            case null => "100%"
            case _    => value
        })

        /*
        * calculate the course progress percentage from `progress` column which is no of content visited/read
        * */
        resolvedDF
            .withColumn("course_completion", defaultProgressUDF(toPercentageUDF(round(expr("progress/leafnodescount * 100")))))
            .withColumn("generatedOn", lit(current_timestamp))
    }



    def saveReportToES(reportDF: DataFrame) = {
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
                col("courseCompletionCountPerBatch").as("completedPercent"),
                stringifySeqUDF(col("district_name")).as("districtName"),
                col("enrolleddate").as("enrolledOn")
            )

        //Save to sunbird platform Elasticsearch instance
        EsSpark.saveToEs(ESDocDF.rdd, "cbatchstats/_doc")
    }

    def uploadReportToCloud(reportDF: DataFrame) = {

        val accName = AppConf.getStorageKey("azure")

        val baseDir = s"wasbs://course-metrics@$accName.blob.core.windows.net/reports"

        reportDF
            .select(
                concat_ws(" ", col("firstname"), col("lastname")).as("User Name"),
                col("batchid"),
                col("email").as("Email ID"),
                col("phone").as("Mobile Number"),
                stringifySeqUDF(col("district_name")).as("District Name"),
                col("orgname_resolved").as("Organisation Name"),
                col("schoolname_resolved").as("School Name"),
                col("course_completion").as("Course Progress"),
                col("generatedOn").as("last updated")
            )
            .coalesce(1)
            .write
            .partitionBy("batchid")
            .mode("overwrite")
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .save(baseDir)
    }

    override def algorithm(events: RDD[Empty], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        sc.emptyRDD[Empty]
    }

    override def postProcess(data: RDD[Empty], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext) : RDD[Empty] = {
        sc.emptyRDD[Empty]
    }
}
