package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.CourseUtils.{getCourseBatchDetails, getLiveCourses, getTenantInfo, loadData}
import org.sunbird.cloud.storage.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil

case class BaseCourseMetricsOutput(courseName: String, batchName: String, status: String, slug: String, courseId: String, batchId: String) extends AlgoInput

trait BaseCourseMetrics[T <: AnyRef, A <: BaseCourseMetricsOutput, B <: AlgoOutput, R <: Output] extends IBatchModelTemplate[T,BaseCourseMetricsOutput,B,R]{

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
    implicit val spark: SparkSession = CommonUtil.getSparkSession(AppConf.getConfig("default.parallelization").toInt,"BaseCourseMetrics",
      Option(AppConf.getConfig("spark.cassandra.connection.host")), Option(readConsistencyLevel))

    val liveCourses = getLiveCourses(config).withColumnRenamed("name","courseName")
    val courseBatch = getCourseBatchDetails(spark, loadData).withColumnRenamed("name","batchName")
    val tenantInfo = getTenantInfo(spark, loadData)

    val joinCourses = liveCourses.join(courseBatch, liveCourses.col("identifier") === courseBatch.col("courseid"), "inner")
    val joinWithTenant = joinCourses.join(tenantInfo, joinCourses.col("channel") === tenantInfo.col("id"), "inner")

    joinWithTenant.na.fill("unknown", Seq("slug")).select("courseName","batchName","status","slug", "courseid", "batchid")
  }

}

