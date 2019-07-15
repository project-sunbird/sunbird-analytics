package org.ekstep.analytics.api.util

import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.searches.queries.funcscorer.ScoreFunctionDefinition
import com.typesafe.config.{Config, ConfigFactory}
import com.sksamuel.elastic4s.http.ElasticDsl._

trait ESsearch {
    def searchExperiment(fields: Map[String, String]): Either[RequestFailure, RequestSuccess[SearchResponse]]
}

object ElasticsearchService extends ESsearch {

    private lazy val config: Config = ConfigFactory.load()
    private lazy val host =  config.getString("elasticsearch.host")
    private lazy val port = config.getInt("elasticsearch.port")

    private def getConnection = HttpClient(ElasticsearchClientUri(host, port))

    def searchExperiment(fields: Map[String, String]): Either[RequestFailure, RequestSuccess[SearchResponse]] = {
        val indexName = config.getString("elasticsearch.index.experiment")
        val fieldWeight: Map[String, Double] = Map("userId" -> 3.0, "deviceId" -> 3.0, "url" -> 3.0)

        val functionList: List[ScoreFunctionDefinition] = List(
            weightScore(9.0).filter(boolQuery().must(fields.map { field =>
                matchQuery(field._1, field._2)
            }))
        ) ::: fieldWeight.map { fw =>
                weightScore(fw._2).filter(boolQuery().not(existsQuery(fw._1)))
        }.toList

        val query = search(indexName).query {
            functionScoreQuery(
                boolQuery().should(
                    fields.map { field =>
                        termQuery(field._1, field._2)
                    }
                )
            )
              .functions(functionList)
              .boostMode("sum")
        }

        val client = getConnection
        val response = client.execute(query).await
        client.close()
        response
    }


}
