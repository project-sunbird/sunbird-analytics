package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.CourseUtils.{getCourseBatchDetails, getLiveCourses, getTenantInfo, loadData}
import org.sunbird.cloud.storage.conf.AppConf

case class BaseCourseMetricsOutput(courseName: String, batchName: String, status: String, slug: String, courseid: String, batchid: String) extends AlgoInput

trait BaseCourseMetrics[T <: AnyRef, A <: BaseCourseMetricsOutput, B <: AlgoOutput, R <: Output] extends IBatchModelTemplate[T,BaseCourseMetricsOutput,B,R]{

  override def preProcess(events: RDD[T], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[BaseCourseMetricsOutput] = {
    val data = getCourseMetrics(config)
    val encoder = Encoders.product[BaseCourseMetricsOutput]
    data.as[BaseCourseMetricsOutput](encoder).rdd
  }

  def getCourseMetrics(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): DataFrame = {
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel).set("spark.cassandra.connection.host", "11.2.3.63")
      .set("spark.cassandra.connection.port","9042")
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    val liveCourses = getLiveCourses(config).withColumnRenamed("name","courseName")
    val courseBatch = getCourseBatchDetails(spark, loadData).withColumnRenamed("name","batchName")
    val tenantInfo = getTenantInfo(spark, loadData)

    val joinCourses = liveCourses.join(courseBatch, liveCourses.col("identifier") === courseBatch.col("courseid"), "inner")
    val joinWithTenant = joinCourses.join(tenantInfo, joinCourses.col("channel") === tenantInfo.col("id"), "inner")

    joinWithTenant.select("courseName","batchName","status","slug", "courseid", "batchid")
  }

}

