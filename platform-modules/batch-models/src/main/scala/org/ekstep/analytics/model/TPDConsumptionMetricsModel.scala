package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
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

//ES: course-batch
case class ESResponse(took: Double, timed_out: Boolean, _shards: _shards, hits: Hit)
case class _shards(total: Option[Double], successful: Option[Double], skipped: Option[Double], failed: Option[Double])
case class Hit(total: Double, max_score: Double, hits: List[Hits])
case class Hits(_source: _source)
case class _source(batchId: String, courseId: String, status: String, name: String)

//Output
case class courseConsumptionOutput(date: String, courseName: String, batchName: String, status: String, timespent: Option[Double] = Option(0))

object TPDConsumptionMetricsModel extends IBatchModelTemplate[Empty, CourseInfo, Empty, Empty] with Serializable {

  implicit val className = "org.ekstep.analytics.model.TPDConsumptionMetricsModel"
  override def name: String = "TPDConsumptionMetricsModel"

  implicit val fc = new FrameworkContext()

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseInfo] = {
   //Get live Courses
    val liveCourses = getLiveCourses()
    println("live Courses:  " + liveCourses)
    sc.parallelize(liveCourses)
  }

  override def algorithm(events: RDD[CourseInfo], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    val coursePlayTime = getCoursePlays(config)
    val coursePlayRDD = sc.parallelize(coursePlayTime)
    val filteredCoursePlayRDD = events.foreach(f => println("live course events: " + f))
    println("course play time:  " + coursePlayTime)
    val courseBatchInfo = getCourseBatchFromES(coursePlayTime)
    println("course-batch details: " + courseBatchInfo)

    sc.emptyRDD
  }

  override def postProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    events
  }

  def getCoursePlays(config: Map[String, AnyRef]): List[CoursePlays] = {
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.get("druidQuery").get))
    val druidResponse = DruidDataFetcher.getDruidData(druidConfig)
    druidResponse.map{f =>
      JSONUtils.deserialize[CoursePlays](f)}
  }

def getLiveCourses(): List[CourseInfo] = {
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
  println("eventsResult: " + eventsResult)
  eventsResult.map{f => CourseInfo(f.event.getOrElse("channel", null), f.event.getOrElse("identifier", null), f.event.getOrElse("name", null))}
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

  def getCourseBatchFromES(coursePlay: List[CoursePlays]) : List[_source] = {
    val apiURL = Constants.ELASTIC_SEARCH_SERVICE_ENDPOINT + "/" + Constants.ELASTIC_SEARCH_INDEX_COURSEBATCH_NAME + "/_search"
    val courseList = coursePlay.map{_.courseId}
    val courseIds = JSONUtils.serialize(courseList)

    val batchList = coursePlay.map(_.batchId)
    val batchIds = JSONUtils.serialize(batchList)

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
    val response = RestUtil.post[ESResponse](apiURL, request).hits.hits
    response.map(f => JSONUtils.deserialize[Hits](JSONUtils.serialize(f))._source)
  }

}
