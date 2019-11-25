package org.ekstep.analytics.api.util

import scala.concurrent.Future
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.searches.queries.funcscorer.ScoreFunctionDefinition
import com.typesafe.config.{Config, ConfigFactory}
import com.sksamuel.elastic4s.http.ElasticDsl._
import org.ekstep.analytics.api.service.experiment.ExperimentData
import javax.inject._

trait ESsearch {
    def searchExperiment(fields: Map[String, String]): Future[Option[ExperimentData]]
}

@Singleton
class ElasticsearchService extends ESsearch {

    implicit val className = "org.ekstep.analytics.api.util.ElasticsearchService"
    private lazy val config: Config = ConfigFactory.load()
    private lazy val host =  config.getString("elasticsearch.host")
    private lazy val port = config.getInt("elasticsearch.port")
    private lazy val fieldWeight: String =  config.getString("elasticsearch.searchExperiment.fieldWeight")
    private lazy val fieldWeightMap: Map[String, Double] = JSONUtils.deserialize[Map[String, Double]](fieldWeight)
    private lazy val queryWeight = config.getDouble("elasticsearch.searchExperiment.matchQueryScore")
    private lazy val searchExperimentIndex = config.getString("elasticsearch.searchExperiment.index")
    implicit val executor =  scala.concurrent.ExecutionContext.global

    private def getConnection = HttpClient(ElasticsearchClientUri(host, port))

    def checkConnection: Boolean = {
        try {
            val conn = getConnection
            conn match {
                case c: HttpClient => {
                    c.close()
                    true
                }
                case _ => false
            }
        } catch {
            case ex: Exception => false
        }
    }

    def searchExperiment(fields: Map[String, String]): Future[Option[ExperimentData]] = {

        val functionList: List[ScoreFunctionDefinition] = List(
            weightScore(queryWeight).filter(boolQuery().must(fields.map { field =>
                matchQuery(field._1, field._2)
            }))
        ) ::: fieldWeightMap.map { fw =>
                weightScore(fw._2).filter(boolQuery().not(existsQuery(fw._1)))
        }.toList

        val query = search(searchExperimentIndex).query {
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
        val response = client.execute(query)

        response.map { _ match {
                case Right(success) => {
                    client.close()
                    val res = success.result.hits.hits
                    if (res.length > 0 && res.head.sourceAsString.nonEmpty) {
                        Some(JSONUtils.deserialize[ExperimentData](res.head.sourceAsString))
                    } else None
                }
                case Left(error) => {
                    client.close()
                    APILogger.log("", Option(Map("comments" -> s"Elasticsearch exception ! ${error.error.reason}")), "ElasticsearchService")
                    None
                }
            }
        }.recover {
            case ex: Exception => {
                ex.printStackTrace()
                client.close()
                APILogger.log("", Option(Map("comments" -> s"Elasticsearch exception ! ${ex.getMessage}")), "ElasticsearchService")
                None
            }
        }
    }

}
