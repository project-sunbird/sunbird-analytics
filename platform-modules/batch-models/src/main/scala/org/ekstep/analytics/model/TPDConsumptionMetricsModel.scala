package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.job.report.{BaseCourseMetrics, BaseCourseMetricsOutput}
import org.ekstep.analytics.util.CourseUtils

//Timespent In Mins for a course: getCoursePlays
case class CoursePlays(date: String, courseId: String, batchId: String, timespent: Option[Double] = Option(0))
case class CourseKeys(courseId: String, batchId: String)

//Final Output
case class CourseConsumptionOutput(date: String, courseName: String, batchName: String, status: String, timespent: Option[Double] = Option(0), slug: String, reportName: String) extends AlgoOutput with Output

object TPDConsumptionMetricsModel extends BaseCourseMetrics[Empty, BaseCourseMetricsOutput, CourseConsumptionOutput, CourseConsumptionOutput] with Serializable {

  implicit val className = "org.ekstep.analytics.model.TPDConsumptionMetricsModel"
  override def name: String = "TPDConsumptionMetricsModel"

  implicit val fc = new FrameworkContext()

  override def algorithm(events: RDD[BaseCourseMetricsOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseConsumptionOutput] = {
    implicit val sqlContext = new SQLContext(sc)

    val druidConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(config.get("druidConfig").get)).metrics.map(_.druidQuery)
    val druidResponse = DruidDataFetcher.getDruidData(druidConfig(0))
    val coursePlays = druidResponse.map{f => JSONUtils.deserialize[CoursePlays](f)}
    val coursePlaysRDD = sc.parallelize(coursePlays)

    val courseBatchKeys = events.map(f => (CourseKeys(f.courseId, f.batchId), f))
    val coursePlaysKeys = coursePlaysRDD.map(f => (CourseKeys(f.courseId,f.batchId), f))

    val joinResponse = coursePlaysKeys.leftOuterJoin(courseBatchKeys)
      val courseConsumption = joinResponse.map{f =>
      val coursePlay = f._2._1
        val courseMetrics = f._2._2.get
        CourseConsumptionOutput(coursePlay.date, courseMetrics.courseName, courseMetrics.batchName, courseMetrics.status, coursePlay.timespent, courseMetrics.slug, "course_usage")
    }
    courseConsumption
  }

  override def postProcess(data: RDD[CourseConsumptionOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseConsumptionOutput] = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    if (data.count() > 0) {
      val df = data.toDF().na.fill(0L)
      val postDataToAzure = CourseUtils.postDataToBlob(df, config)
    } else {
      JobLogger.log("No data found from druid", None, Level.INFO)
    }
    data
  }

}
