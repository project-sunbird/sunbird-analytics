package org.ekstep.analytics.model.report

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.job.report.{BaseCourseMetrics, BaseCourseMetricsOutput}
import org.ekstep.analytics.util.CourseUtils
import org.sunbird.cloud.storage.conf.AppConf

case class CourseEnrollmentOutput(date: String, courseName: String, batchName: String, status: String, enrollmentCount: BigInt, completionCount: BigInt, slug: String, reportName: String) extends AlgoOutput with Output
case class ESResponse(participantCount: BigInt, completedCount: BigInt, courseId: String)

object CourseEnrollmentModel extends BaseCourseMetrics[Empty, BaseCourseMetricsOutput, CourseEnrollmentOutput, CourseEnrollmentOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.model.CourseEnrollmentModel"
  implicit val fc = new FrameworkContext()

  override def algorithm(events: RDD[BaseCourseMetricsOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseEnrollmentOutput] = {
    val finalRDD = getCourseEnrollmentOutput(events)
    val date = (new SimpleDateFormat("dd-MM-yyyy")).format(Calendar.getInstance().getTime)
    finalRDD.map(f => CourseEnrollmentOutput(date,f._1.courseName,f._1.batchName,f._1.status,
      f._2.getOrElse(ESResponse(0,0,"")).participantCount,f._2.getOrElse(ESResponse(0,0,"")).completedCount,f._1.slug,"course_enrollments"))
  }

  override def postProcess(data: RDD[CourseEnrollmentOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseEnrollmentOutput] = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    if (data.count() > 0) {
      val df = data.toDF().na.fill(0L)
      CourseUtils.postDataToBlob(df, config)
    } else {
      JobLogger.log("No data found", None, Level.INFO)
    }
    data
  }

  def getCourseEnrollmentOutput(events: RDD[BaseCourseMetricsOutput])(implicit sc: SparkContext, fc: FrameworkContext): RDD[(BaseCourseMetricsOutput, Option[ESResponse])] =  {
    val batchId = events.collect().map(f => f.batchId)
    val courseId = events.collect().map(f => f.courseId)
    val courseCounts = getCourseBatchCounts(JSONUtils.serialize(courseId),JSONUtils.serialize(batchId))
    val baseCourseMetricsOutput = events.map(f=> (f.courseId,f))

    val encoder = Encoders.product[ESResponse]
    val courseInfo = courseCounts.as[ESResponse](encoder).rdd

    val courses = courseInfo.map(f => (f.courseId, f))
    val finalRDD = baseCourseMetricsOutput.leftOuterJoin(courses)
    finalRDD.map(f => f._2)
  }

  def getCourseBatchCounts(courseIds: String, batchIds: String)(implicit sc: SparkContext) : DataFrame = {
    implicit val sqlContext = new SQLContext(sc)

    val request = s"""{
                     |  "query": {
                     |    "bool": {
                     |      "filter": [
                     |        {
                     |          "terms": {
                     |            "courseId.raw": $courseIds
                     |          }
                     |        },
                     |        {
                     |          "terms": {
                     |            "batchId.raw": $batchIds
                     |          }
                     |        }
                     |      ]
                     |    }
                     |  }
                     |}""".stripMargin

    sqlContext.sparkSession.read.format("org.elasticsearch.spark.sql")
      .option("query", request)
      .option("pushdown", "true")
      .option("es.nodes", AppConf.getConfig("es.composite.host"))
      .option("es.port", AppConf.getConfig("es.port"))
      .option("es.scroll.size", AppConf.getConfig("es.scroll.size"))
      .option("inferSchema", "true")
      .load("course-batch").select("participantCount", "completedCount", "courseId")
  }
}
