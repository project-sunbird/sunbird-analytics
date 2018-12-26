package org.ekstep.analytics.updater

/**
  * Ref:Design wiki link: https://project-sunbird.atlassian.net/wiki/spaces/SBDES/pages/794198025/Design+Brainstorm+Data+structure+for+capturing+dashboard+portal+metrics
  * Ref:Implementation wiki link: https://project-sunbird.atlassian.net/wiki/spaces/SBDES/pages/794099772/Data+Product+Dashboard+summariser+-+Cumulative
  *
  * @author Manjunath Davanam <manjunathd@ilimi.in>
  */

import com.datastax.spark.connector._
import javax.ws.rs.core.HttpHeaders
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}
import org.ekstep.analytics.util.Constants

import scala.util.Try


case class WorkFlowUsageMetrics(noOfUniqueDevices: Long, totalContentPlaySessions: Long, totalTimeSpent: Double, totalContentPublished: ContentPublishedList) extends AlgoOutput with Output with AlgoInput

case class PortalMetrics(eid: String, ets: Long, syncts: Long, metrics_summary: Option[WorkFlowUsageMetrics]) extends AlgoOutput with Output

case class ContentPublishedList(count: Int, language_publisher_breakdown: List[LanguageByPublisher])

case class LanguageByPublisher(publisher: String, languages: List[Language])

case class Language(id: String, count: Double)

case class ESResponse(aggregations: Aggregations)

case class Aggregations(publisher_agg: AggregationResult)

case class AggregationResult(buckets: List[Buckets])

case class Buckets(key: String, doc_count: Double, language_agg: AggregationResult)

case class OrgResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: OrgResult)

case class OrgResult(response: ContentList)

case class ContentList(count: Long, content: List[OrgProps])

case class OrgProps(orgName: String, hashTagId: String, rootOrgId: String)

case class DeviceProfile(device_id: String)

object UpdatePortalMetrics extends IBatchModelTemplate[DerivedEvent, DerivedEvent, WorkFlowUsageMetrics, PortalMetrics]
  with Serializable {

  val className = "org.ekstep.analytics.updater.UpdatePortalMetrics"

  private val EVENT_ID: String = "ME_PORTAL_CUMULATIVE_METRICS"

  override def name: String = "UpdatePortalMetrics"

  /**
    * preProcess which will fetch the `workflow_usage_summary` Event data from the Cassandra Database.
    *
    * @param data   - RDD Event Data(Empty RDD event)
    * @param config - Configurations to run preProcess
    * @param sc     - SparkContext
    * @return -     DerivedEvent
    */
  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
    data
  }

  /**
    *
    * @param data   - RDD Workflow summary event data
    * @param config - Configurations to algorithm
    * @param sc     - Spark context
    * @return - DashBoardSummary ->(uniqueDevices, totalContentPlayTime, totalTimeSpent,)
    */
  override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])
                        (implicit sc: SparkContext): RDD[WorkFlowUsageMetrics] = {
    val publisherByLanguageList = getLanguageAndPublisherList()
    val totalContentPublished = Try(ContentAdapter.getPublishedContentList().count).getOrElse(0)
    val noOfUniqueDevices =
      sc.cassandraTable[DeviceProfile](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE)
        .map(_.device_id).distinct().count()
    val metrics =
      sc.cassandraTable[WorkFlowUsageMetricsAlgoOutput](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY)
        .map(event => {
          (event.total_timespent, event.total_content_play_sessions)
        })
    val totalTimeSpent = metrics.map(_._1).sum()
    val totalContentPlaySessions = metrics.map(_._2).sum().toLong
    sc.parallelize(Array(WorkFlowUsageMetrics(noOfUniqueDevices, totalContentPlaySessions,
      CommonUtil.roundDouble(totalTimeSpent / 3600, 2),
      ContentPublishedList(totalContentPublished, publisherByLanguageList))))
  }

  /**
    *
    * @param data   - RDD DashboardSummary Event
    * @param config - Configurations to run postprocess method
    * @param sc     - Spark context
    * @return - ME_PORTAL_CUMULATIVE_METRICS MeasuredEvents
    */
  override def postProcess(data: RDD[WorkFlowUsageMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PortalMetrics] = {
    val record = data.first()
    val measures = WorkFlowUsageMetrics(record.noOfUniqueDevices, record.totalContentPlaySessions, record.totalTimeSpent, record.totalContentPublished)
    val metrics = PortalMetrics(EVENT_ID, System.currentTimeMillis(), System.currentTimeMillis(), Some(measures))
    if (config.getOrElse("dispatch", false).asInstanceOf[Boolean]) {
      AzureDispatcher.dispatch(Array(JSONUtils.serialize(metrics)), config)
    }
    sc.parallelize(Array(metrics))
  }

  private def getLanguageAndPublisherList(): List[LanguageByPublisher] = {
    val apiURL = Constants.ELASTIC_SEARCH_SERVICE_ENDPOINT + "/" + Constants.ELASTIC_SEARCH_INDEX_COMPOSITESEARCH_NAME + "/_search"
    println("APIURL", apiURL);
    val request =
      s"""
         |{
         |  "_source":false,
         |  "query":{
         |    "bool":{
         |      "must":[
         |        {
         |          "match":{
         |            "status":{
         |              "query":"Live",
         |              "operator":"AND",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        }
         |      ],
         |      "should":[
         |        {
         |          "match":{
         |            "objectType":{
         |              "query":"Content",
         |              "operator":"OR",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        },
         |        {
         |          "match":{
         |            "objectType":{
         |              "query":"ContentImage",
         |              "operator":"OR",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        },
         |        {
         |          "match":{
         |            "contentType":{
         |              "query":"Resource",
         |              "operator":"OR",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        },
         |        {
         |          "match":{
         |            "contentType":{
         |              "query":"Collection",
         |              "operator":"OR",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        }
         |      ]
         |    }
         |  },
         |  "aggs":{
         |    "publisher_agg":{
         |      "terms":{
         |        "field":"createdFor.raw",
         |        "size":1000
         |      },
         |      "aggs":{
         |        "language_agg":{
         |          "terms":{
         |            "field":"language.raw",
         |            "size":1000
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
       """.stripMargin
    val response = RestUtil.post[ESResponse](apiURL, request)
//    val res =
//      """
//        |{"took":6,"timed_out":false,"_shards":{"total":5,"successful":5,"skipped":0,"failed":0},"hits":{"total":31623,"max_score":17.36364,"hits":[{"_index":"compositesearch","_type":"cs","_id":"do_112493573259223040148.img","_score":17.36364},{"_index":"compositesearch","_type":"cs","_id":"do_112240785235501056165.img","_score":17.087969},{"_index":"compositesearch","_type":"cs","_id":"do_112486539488894976118.img","_score":15.788908},{"_index":"compositesearch","_type":"cs","_id":"do_112598201993822208134.img","_score":15.784051},{"_index":"compositesearch","_type":"cs","_id":"do_112598258266931200138.img","_score":15.784051},{"_index":"compositesearch","_type":"cs","_id":"do_112504711405404160121.img","_score":15.583953},{"_index":"compositesearch","_type":"cs","_id":"do_112598181515247616132.img","_score":15.583953},{"_index":"compositesearch","_type":"cs","_id":"do_112599505273323520195.img","_score":15.583953},{"_index":"compositesearch","_type":"cs","_id":"do_112594747429896192186.img","_score":15.583953},{"_index":"compositesearch","_type":"cs","_id":"do_11259738771644416011.img","_score":15.574913}]},"aggregations":{"publisher_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"org_001","doc_count":28,"language_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"english","doc_count":28}]}},{"key":"0123653943740170242","doc_count":26,"language_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"english","doc_count":26}]}},{"key":"org.ekstep.partner.pratham","doc_count":4,"language_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"english","doc_count":2},{"key":"bengali","doc_count":1},{"key":"hindi","doc_count":1}]}},{"key":"0123673542904299520","doc_count":1,"language_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"english","doc_count":1}]}},{"key":"0123673689120112640","doc_count":1,"language_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"english","doc_count":1}]}},{"key":"0124226794392862720","doc_count":1,"language_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"english","doc_count":1}]}},{"key":"ap12345","doc_count":1,"language_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"english","doc_count":1}]}},{"key":"com.ekstep.303","doc_count":1,"language_agg":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"english","doc_count":1}]}}]}}}
//      """.stripMargin

    println("response", JSONUtils.serialize(response))
    val orgResult = orgSearch()
    println("OrgResult", JSONUtils.serialize(orgResult));
    response.aggregations.publisher_agg.buckets.map(publisherBucket => {
      val languages = publisherBucket.language_agg.buckets.map(languageBucket => {
        Language(languageBucket.key, languageBucket.doc_count)
      })
      orgResult.result.response.content.map(org => {
        if (org.hashTagId == publisherBucket.key) {
          LanguageByPublisher(org.orgName, languages)
        } else {
          LanguageByPublisher("", List()) // Return empty publisher list
        }
      }).filter(_.publisher.nonEmpty)
    })
  }.flatMap(f=>f)

  private def orgSearch(): OrgResponse = {
    val request =
      s"""
         |{
         |  "request":{
         |    "filters":{
         |      "isRootOrg":true
         |    },
         |    "limit":1000,
         |    "fields":[
         |      "orgName",
         |      "rootOrgId",
         |      "hashTagId"
         |    ]
         |  }
         |}
       """.stripMargin
    val requestHeaders = Map(HttpHeaders.AUTHORIZATION -> Constants.ORG_SEARCH_API_KEY)
    RestUtil.post[OrgResponse](Constants.ORG_SEARCH_URL, request, Some(requestHeaders))
  }
}