package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.job.report.{BaseCourseMetricsJob, BaseCourseMetricsOutput}

//Live Courses Information from druid: getLiveCourses
case class CourseInfo(courseName: String, courseId: String, channel: String) extends AlgoInput

//Timespent In Mins for a course: getCoursePlays
case class CoursePlays(date: String, courseId: String, batchId: String, timespent: Option[Double] = Option(0))

//Course-Batch join course plays
case class CourseBatch(date: String, courseName: String, channel: String, courseId: String, batchId: String, timespent: Option[Double] = Option(0))

case class CourseBatchOutput(date: String, courseName: String, name: String, status: String, timespent: Option[Double] = Option(0), slug: String)

//Final Output
case class CourseConsumptionOutput(date: String, courseName: String, name: String, status: String, timespent: Option[Double] = Option(0), slug: String, reportName: String) extends AlgoOutput with Output

object TPDConsumptionMetricsModel extends BaseCourseMetricsJob {

  implicit val fc = new FrameworkContext()

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[BaseCourseMetricsOutput] = super.preProcess(events, config)

  override def algorithm(events: RDD[BaseCourseMetricsOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    println("algodata: " + events)
    sc.emptyRDD
  }

  override def postProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    sc.emptyRDD
  }


  def getCoursePlays(config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CoursePlays] = {
    val druidConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(config.get("reportConfig").get)).metrics.map(_.druidQuery)
    val druidResponse = DruidDataFetcher.getDruidData(druidConfig(0))
    val response = druidResponse.map{f => JSONUtils.deserialize[CoursePlays](f)}
    sc.parallelize(response)
  }

  def getCourseInfoFromES(liveCoursePlayRDD: RDD[CourseBatch])(implicit sc: SparkContext): DataFrame = {
    val courseList = liveCoursePlayRDD.collect().map(f => f.courseId)
    val courseIds = JSONUtils.serialize(courseList)
    val batchList = liveCoursePlayRDD.collect().map(f => f.batchId)
    val batchIds = JSONUtils.serialize(batchList)

    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    getCourseBatchFromES(courseIds, batchIds)
  }

  def getCourseBatchFromES(courseIds: String, batchIds: String)(implicit sc: SparkContext, spark: SparkSession) : DataFrame = {
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
    spark.read.format("org.elasticsearch.spark.sql")
        .option("query", request)
        .option("pushdown", "true")
        .option("es.nodes", AppConf.getConfig("es.composite.host"))
        .option("es.port", AppConf.getConfig("es.port"))
        .option("es.scroll.size", AppConf.getConfig("es.scroll.size"))
        .option("inferSchema", "true")
        .load("course-batch").select("batchId", "courseId", "status", "name")
  }
}
