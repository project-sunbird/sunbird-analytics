package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.CourseUtils
import org.ekstep.analytics.util.BatchStatus
import org.sunbird.cloud.storage.conf.AppConf

case class BaseCourseMetricsOutput(courseName: String, batchName: String, status: String, slug: String, courseId: String, batchId: String) extends AlgoInput

trait BaseCourseMetrics[T <: AnyRef, A <: BaseCourseMetricsOutput, B <: AlgoOutput, R <: Output] extends IBatchModelTemplate[T,BaseCourseMetricsOutput,B,R]{

  override def preProcess(events: RDD[T], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[BaseCourseMetricsOutput] = {
    val data = getCourseMetrics(config)
    val encoder = Encoders.product[BaseCourseMetricsOutput]
    val finalData = data.as[BaseCourseMetricsOutput](encoder).rdd
    finalData.map(f => {
      val batchStatus = BatchStatus.values.filter(c => (c.id==Integer.parseInt(f.status)))
      if (batchStatus.isEmpty) f.copy(status = "") else f.copy(status = batchStatus.firstKey.toString)
    })
  }

  def getCourseMetrics(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): DataFrame = {
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")

    implicit val spark: SparkSession = CommonUtil.getSparkSession(AppConf.getConfig("default.parallelization").toInt,"BaseCourseMetrics",
      Option(AppConf.getConfig("spark.cassandra.connection.host")), Option(readConsistencyLevel))

    val liveCourses = CourseUtils.getLiveCourses(config)
    val courseBatch = CourseUtils.getCourseBatchDetails(spark)
    val tenantInfo = CourseUtils.getTenantInfo(spark)
    val joinCourses = liveCourses.join(courseBatch, liveCourses.col("identifier") === courseBatch.col("courseId"), "inner")
    val joinWithTenant = joinCourses.join(tenantInfo, joinCourses.col("channel") === tenantInfo.col("id"), "inner")

    joinWithTenant.na.fill("unknown", Seq("slug")).select("courseName","batchName","status","slug", "courseId", "batchId")
  }

}

