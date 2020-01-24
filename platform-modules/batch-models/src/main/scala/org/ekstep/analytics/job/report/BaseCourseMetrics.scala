package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.util.CourseUtils

case class BaseCourseMetricsOutput(courseName: String, batchName: String, status: String, slug: String, courseId: String, batchId: String) extends AlgoInput

trait BaseCourseMetrics[T <: AnyRef, A <: BaseCourseMetricsOutput, B <: AlgoOutput, R <: Output] extends IBatchModelTemplate[T,BaseCourseMetricsOutput,B,R] {

  override def preProcess(events: RDD[T], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[BaseCourseMetricsOutput] = {
    val data = getCourseMetrics(config)
    val encoder = Encoders.product[BaseCourseMetricsOutput]
    val finalData = data.as[BaseCourseMetricsOutput](encoder).rdd
    finalData.map(f => {
      val batchStatus = if(f.status.equals("0")) "Upcoming" else if(f.status.equals("1")) "Ongoing" else if(f.status.equals("2")) "Expired" else ""
      BaseCourseMetricsOutput(f.courseName,f.batchName,batchStatus,f.slug, f.courseId,f.batchId)
    })
  }

  def getCourseMetrics(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): DataFrame = {
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel).set("spark.cassandra.connection.host", AppConf.getConfig("spark.cassandra.connection.host"))
      .set("spark.cassandra.connection.port",AppConf.getConfig("cassandra.service.embedded.connection.port"))

    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    val liveCourses = CourseUtils.getLiveCourses(config)
    val courseBatch = CourseUtils.getCourseBatchDetails(spark, CourseUtils.loadData)
      .withColumnRenamed("name", "batchName")
      .withColumnRenamed("batchid", "batchId")
      .withColumnRenamed("courseid", "courseId")
    val tenantInfo = CourseUtils.getTenantInfo(spark, CourseUtils.loadData)

    val joinCourses = liveCourses.join(courseBatch, liveCourses.col("identifier") === courseBatch.col("courseId"), "inner")
    val joinWithTenant = joinCourses.join(tenantInfo, joinCourses.col("channel") === tenantInfo.col("id"), "inner").select("courseName","batchName","status","slug", "courseId", "batchId")
    joinWithTenant
  }

}

