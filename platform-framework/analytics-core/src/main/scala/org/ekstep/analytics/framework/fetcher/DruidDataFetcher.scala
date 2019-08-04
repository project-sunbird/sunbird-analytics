package org.ekstep.analytics.framework.fetcher

import ing.wbaa.druid._
import ing.wbaa.druid.definitions.{CountAggregation, Dimension}
import io.circe.Decoder
import org.ekstep.analytics.framework.DruidQueryModel
import org.ekstep.analytics.framework.exception.DataFetcherException
import io.circe.generic.auto._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}

import scala.concurrent.Await

object DruidDataFetcher {

    @throws(classOf[DataFetcherException])
    def getDruidData[T](query: DruidQueryModel)(implicit decoder: Decoder[T]): List[T] = {
//        implicit val decoder: Decoder[T] = deriveDecoder //exportDecoder[T].instance

        // TO-DOs:
        // set datasource for each query
        // getAggregation methods in CommonUtil
        // change Await to some other way of getting future objects
        // handle for zero results from druid

        val request = getQuery(query)
        println("request: " + request)
        val response = request.execute()
        val result = Await.result(response, scala.concurrent.duration.Duration.apply(1L, "minute"))
        val listData = result.list[T]

        //println("response: " + result.results)
//        if(query.queryType.equalsIgnoreCase("timeseries")){
//            val series = result.results.map { f =>
//                val dataMap = f.result.asObject.get.toMap.map(f => (f._1 -> f._2.toString())).seq
//                val timeMap = Map("time" -> f.timestamp.toString)
//                timeMap ++ dataMap
//            }
//            series.map(f => JSONUtils.serialize(f))
//        }
//        else if(query.queryType.equalsIgnoreCase("topn")){
//            val list = result.results.map(f => f.result.mapArray(f => f))
//            list.map(f => f.toString())
//        }
//        else {
//            val list = result.results.map(f => f.result.mapString(f => f))
//            list.map(f => f.toString())
//        }
        listData
    }

    def getQuery(query: DruidQueryModel): DruidQuery = {
        query.queryType.toLowerCase() match {
            case "groupby" => {
                GroupByQuery(
                    aggregations = List(CountAggregation(name = "count")),
                    dimensions = query.dimensions.get.map(f => Dimension(f)),
                    granularity = CommonUtil.getGranularity(query.granularity.getOrElse("all")),
                    intervals = List(CommonUtil.getIntervalRange(query.intervals))
                )
            }
            case "topn" => {
                TopNQuery(
                    aggregations = List(CountAggregation(name = "count")),
                    dimension = Dimension(query.dimensions.get.head),
                    intervals = List(CommonUtil.getIntervalRange(query.intervals)),
                    granularity = CommonUtil.getGranularity(query.granularity.getOrElse("all")),
                    threshold = query.threshold.getOrElse(5).asInstanceOf[Int],
                    metric = query.metric.getOrElse("count")
                )
            }
            case "timeseries" => {
                TimeSeriesQuery(
                    aggregations = List(CountAggregation(name = "count")),
                    granularity = CommonUtil.getGranularity(query.granularity.getOrElse("all")),
                    intervals = List(CommonUtil.getIntervalRange(query.intervals))
                )
            }
            case _ =>
                throw new DataFetcherException("Unknown druid query type found");
        }
    }


}

