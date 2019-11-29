package org.ekstep.analytics.api.util

import com.sksamuel.elastic4s.http.HttpClient
import org.ekstep.analytics.api.BaseSpec


class TestElasticsearchService extends BaseSpec {

  val httpClientMock = mock[HttpClient]
  val ESservice = new ElasticsearchService()
  implicit val executor =  scala.concurrent.ExecutionContext.global

  "Elasticsearch service: searchExperiment method" should "search and return data " in {

    val response = ESservice.searchExperiment(Map("deviceId" -> "device3", "userId" -> "user3", "url" -> "http://xyz.com", "producer"-> "prod.diksha.app"))
    response.map { data => data.map {
      expData => {
        expData.userId should be eq("user3")
        expData.expType should be eq("modulus-exp-2")
      }
      }
    }
  }
}
