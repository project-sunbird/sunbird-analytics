package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.CourseUtils.{getCourseBatchDetails, getLiveCourses, getTenantInfo, loadData}
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.Map

case class BaseCourseMetricsOutput(courseName: String, batchName: String, status: String, slug: String, courseid: String, batchid: String)

trait BaseCourseMetricsJob[T <: AnyRef, A <: AlgoInput, B <: AlgoOutput, R <: Output] extends IBatchModelTemplate[Empty,Empty,Empty,Empty] {

  def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[BaseCourseMetricsOutput] = {
    val data = getCourseMetrics(config)
    val encoder = Encoders.product[BaseCourseMetricsOutput]
    data.as[BaseCourseMetricsOutput](encoder).rdd
  }

  def algorithm(events: RDD[BaseCourseMetricsOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty]

  def postProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty]

  def getCourseMetrics(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): DataFrame = {
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel).set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port","9042")
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    val liveCourses = getLiveCourses(config)
    val courseBatch = getCourseBatchDetails(spark, loadData)
    val tenantInfo = getTenantInfo(spark, loadData)

    val courses = liveCourses.withColumnRenamed("name","courseName")
    val joinCourses = courses.join(courseBatch, courses.col("identifier") === courseBatch.col("courseid"), "inner")
    val joinWithTenant = joinCourses.join(tenantInfo, joinCourses.col("channel") === tenantInfo.col("id"), "inner")

    joinWithTenant.select("courseName","name","status","slug", "courseid", "batchid")
  }

}
