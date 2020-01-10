package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{JSONUtils, RestUtil}
import org.ekstep.analytics.util.Constants

//OrgSearch Tenant Information: getTenantInfo
case class TenantResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: TenantResult)
case class TenantResult(response: ContentList)
case class ContentList(count: Int, content: List[TenantInfo])
case class TenantInfo(orgName: String, channel: String, id: String, slug: String)

//Live Courses Information from druid: getLiveCourses
case class CourseResult(pagingIdentifiers: Map[String, Int], dimensions: List[String], events: List[EventList])
case class EventList(segmentId: String, offset: Int, event: Map[String,String])
case class CourseInfo(channel: String, courseId: String, courseName: String) extends AlgoInput

//Timespent In Mins for a course: getCoursePlays
case class CoursePlays(courseId: String, userId: String, batchId: String, timespent: Option[Double] = Option(0), date: String)

//date,courseName,channel,timespent,date,batchId,courseId,status,name

//ES: course-batch
case class ESResponse(took: Double, timed_out: Boolean, _shards: _shards, hits: Hit)
case class _shards(total: Option[Double], successful: Option[Double], skipped: Option[Double], failed: Option[Double])
case class Hit(total: Double, max_score: Double, hits: List[Hits])
case class Hits(_source: _source)
case class _source(batchId: String, courseId: String, status: String, name: String)

//Course-Batch join course plays
case class CourseBatch(date: String, courseName: String, channel: String, courseId: String, batchId: String, timespent: Option[Double] = Option(0))

//Output
case class courseConsumptionOutput(date: String, courseName: String, batchName: String, status: String, timespent: Option[Double] = Option(0))

object TPDConsumptionMetricsModel extends IBatchModelTemplate[Empty, CourseInfo, Empty, Empty] with Serializable {

  implicit val className = "org.ekstep.analytics.model.TPDConsumptionMetricsModel"
  override def name: String = "TPDConsumptionMetricsModel"

  implicit val fc = new FrameworkContext()

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseInfo] = {
    val liveCourses = getLiveCourses()
    liveCourses
  }

  override def algorithm(events: RDD[CourseInfo], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    val coursePlayTime = getCoursePlays(config)
    val liveCourseRDD = events.map(f => (f.courseId,f))
    val coursePlayRDD = coursePlayTime.map(f => (f.courseId, f))
    val liveCoursePlayRDD = coursePlayRDD.leftOuterJoin(liveCourseRDD)
          .map(f => CourseBatch( f._2._1.date, f._2._2.get.courseName, f._2._2.get.channel, f._1, f._2._1.batchId, f._2._1.timespent))

    val courseList = liveCoursePlayRDD.collect().map(f => f.courseId)
    val courseIds = JSONUtils.serialize(courseList)
    val batchList = liveCoursePlayRDD.collect().map(f => f.batchId)
    val batchIds = JSONUtils.serialize(batchList)

    val courseBatchInfo = getCourseBatchFromES(courseIds, batchIds)

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val liveCourseDF = liveCoursePlayRDD.toDF
    val courseBatchDF = courseBatchInfo.toDF()

    val courseBatch = liveCourseDF.join(courseBatchDF, Seq("courseId", "batchId"))
    courseBatch.show(5)
    val tenantInfo = getTenantInfo().toDF()
    tenantInfo.show(5)
    val consumption = courseBatch.join(tenantInfo, "channel")
    println("consumption: ")
    consumption.show()

    sc.emptyRDD
  }

  override def postProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    events
  }

  def getCoursePlays(config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CoursePlays] = {
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.get("druidQuery").get))
    val druidResponse = DruidDataFetcher.getDruidData(druidConfig)
    val response = druidResponse.map{f =>
      JSONUtils.deserialize[CoursePlays](f)}
    println("course play time:  " + response)

    sc.parallelize(response)
  }

def getLiveCourses()(implicit sc: SparkContext): RDD[CourseInfo] = {
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
  val result = eventsResult.map{f => CourseInfo(f.event.getOrElse("channel", null), f.event.getOrElse("identifier", null), f.event.getOrElse("name", null))}
  println("live Courses:  " + result)
  sc.parallelize(result)
}

  def getTenantInfo(): List[TenantInfo] = {
    val url = Constants.ORG_SEARCH_URL
    val body = """{
                 |    "params": { },
                 |    "request":{
                 |        "filters": {
                 |            "isRootOrg": true
                 |        },
                 |        "offset": 0,
                 |        "limit": 1000,
                 |        "fields": ["id", "channel", "slug", "orgName"]
                 |    }
                 |}""".stripMargin
    val header = Option(Map("cache-control" -> "no-cache", "Accept" -> "application/json"))
    RestUtil.post[TenantResponse](url, body, header).result.response.content
  }

  def getCourseBatchFromES(courseIds: String, batchIds: String)(implicit sc: SparkContext) : RDD[_source] = {
    val apiURL = Constants.ELASTIC_SEARCH_SERVICE_ENDPOINT + "/" + Constants.ELASTIC_SEARCH_INDEX_COURSEBATCH_NAME + "/_search"
    println("courseIds: " + courseIds)
    println("batchIds: " + batchIds)

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
    val ESresponse = RestUtil.post[ESResponse](apiURL, request).hits.hits
    val response = ESresponse.map(f => JSONUtils.deserialize[Hits](JSONUtils.serialize(f))._source)
    println("course-batch details: " + response)

    sc.parallelize(response)
  }

}
