package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.util.ReportUtil

//Live Courses Information from druid: getLiveCourses
case class CourseInfo(courseName: String, courseId: String, channel: String) extends AlgoInput

//Timespent In Mins for a course: getCoursePlays
case class CoursePlays(date: String, courseId: String, userId: String, batchId: String, timespent: Option[Double] = Option(0))

//Course-Batch join course plays
case class CourseBatch(date: String, courseName: String, channel: String, courseId: String, batchId: String, timespent: Option[Double] = Option(0))

case class CourseBatchOutput(date: String, courseName: String, name: String, status: String, timespent: Option[Double] = Option(0), slug: String)

//Final Output
case class CourseConsumptionOutput(date: String, courseName: String, name: String, status: String, timespent: Option[Double] = Option(0), slug: String, reportName: String) extends AlgoOutput with Output

object TPDConsumptionMetricsModel extends IBatchModelTemplate[Empty, CourseInfo, CourseConsumptionOutput, CourseConsumptionOutput] with Serializable {

  implicit val className = "org.ekstep.analytics.model.TPDConsumptionMetricsModel"
  override def name: String = "TPDConsumptionMetricsModel"

  implicit val fc = new FrameworkContext()

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseInfo] = {
    getLiveCoursesFromDruid()
  }

  override def algorithm(events: RDD[CourseInfo], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseConsumptionOutput] = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val coursePlayTime = getCoursePlays(config)
    val liveCourseMap = events.map(f => (f.courseId,f))
    val coursePlayMap = coursePlayTime.map(f => (f.courseId, f))
    val liveCoursePlayRDD = coursePlayMap.leftOuterJoin(liveCourseMap)
          .map(f => CourseBatch( f._2._1.date, f._2._2.get.courseName, f._2._2.get.channel, f._1, f._2._1.batchId, f._2._1.timespent))
    val courseBatchInfo = getCourseInfoFromES(liveCoursePlayRDD)
    val liveCourseDF = liveCoursePlayRDD.toDF

    val courseBatch = liveCourseDF.join(courseBatchInfo, Seq("courseId", "batchId"), "left")
    val tenantInfo = ReportUtil.getTenantInfo()
    val tenantInfoDF = tenantInfo.toDF()

    val encoder = Encoders.product[CourseBatchOutput]
    courseBatch.join(tenantInfoDF, courseBatch.col("channel") === tenantInfoDF.col("id"), "left_outer")
          .drop("courseId", "BatchId", "id", "channel")
          .as[CourseBatchOutput](encoder).rdd
          .map(f => CourseConsumptionOutput(f.date, f.courseName, f.name, f.status, f.timespent, f.slug, "course_consumption"))
  }

  override def postProcess(data: RDD[CourseConsumptionOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseConsumptionOutput] = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    if (data.count() > 0) {
      val df = data.toDF().na.fill(0L)
      val postDataToAzure = ReportUtil.postDataToBlob(df, config)
    } else {
      JobLogger.log("No data found from druid", None, Level.INFO)
    }
    data
  }

  def getCoursePlays(config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CoursePlays] = {
    val druidConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(config.get("reportConfig").get)).metrics.map(_.druidQuery)
    val druidResponse = DruidDataFetcher.getDruidData(druidConfig(0))
    val response = druidResponse.map{f => JSONUtils.deserialize[CoursePlays](f)}
    sc.parallelize(response)
  }

def getLiveCoursesFromDruid()(implicit sc: SparkContext): RDD[CourseInfo] = {
  val url = AppConf.getConfig("druid.host")
  val body = """{
               |  "queryType": "select",
               |  "dataSource": "content-model-snapshot",
               |  "filter": {
               |    "type": "and",
               |    "fields": [
               |      {
               |        "type": "selector",
               |        "dimension": "contentType",
               |        "value": "Course"
               |      },
               |      {
               |        "type": "selector",
               |        "dimension": "status",
               |        "value": "Live"
               |      }
               |    ]
               |  },
               |  "aggregations": [],
               |  "granularity": "all",
               |  "postAggregations": [],
               |  "intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00",
               |  "dimensions": [
               |    "channel",
               |    "identifier",
               |    "name"
               |  ],
               |  "metrics": [
               |    ""
               |  ],
               |  "pagingSpec": {
               |    "pagingIdentifiers": {
               |    },
               |    "threshold": 10000
               |  }
               |}""".stripMargin

  val response = RestUtil.post[List[Map[String, AnyRef]]](url,body)
  val eventsResult = response.flatMap{f => JSONUtils.deserialize[CourseResult](JSONUtils.serialize(f.get("result").get)).events}
  val result = eventsResult.map{f => CourseInfo(f.event.getOrElse("name", null), f.event.getOrElse("identifier", null), f.event.getOrElse("channel", null))}
  sc.parallelize(result)
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
