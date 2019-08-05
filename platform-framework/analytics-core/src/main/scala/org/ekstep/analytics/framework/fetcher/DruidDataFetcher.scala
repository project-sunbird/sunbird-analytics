package org.ekstep.analytics.framework.fetcher

import ing.wbaa.druid._
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.Dim
import ing.wbaa.druid.dql.expressions.{AggregationExpression, FilteringExpression}
import org.ekstep.analytics.framework.{DruidFilter, DruidQueryModel}
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}

import scala.concurrent.Await

object DruidDataFetcher {

    @throws(classOf[DataFetcherException])
    def getDruidData(query: DruidQueryModel): List[String] = {

        // TO-DOs:
        // getHavingFilter, getPostAgg methods
        // change Await to some other way of getting future objects

        val request = getQuery(query)
        println("request: " + request)
        val response = request.execute()
        val result = Await.result(response, scala.concurrent.duration.Duration.apply(5L, "minute"))

        if(result.results.length > 0) {
            if (query.queryType.equalsIgnoreCase("timeseries")) {
                val series = result.results.map { f =>
                    val dataMap = f.result.asObject.get.toMap.map(f => (f._1 -> f._2.toString())).seq
                    val timeMap = Map("time" -> f.timestamp.toString)
                    timeMap ++ dataMap
                }
                series.map(f => JSONUtils.serialize(f))
            }
            else if (query.queryType.equalsIgnoreCase("topn")) {
                val list = result.results.map(f => f).head.result.asArray.get.toList
                list.map(f => f.toString())
            }
            else {
                val list = result.results.map(f => f.result.mapString(f => f))
                list.map(f => f.toString())
            }
        }
        else
            List();
    }

    def getQuery(query: DruidQueryModel): DruidQuery = {

        query.queryType.toLowerCase() match {
            case "groupby" => {
                DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .agg(getAggregation(query): _*)
                  .groupBy(query.dimensions.get.map(f => Dim(f)): _*)
                  .where(getFilter(query).get)
                  .build()
            }
            case "topn" => {
                DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .topN(Dim(query.dimensions.get.head), query.metric.getOrElse("count"), query.threshold.getOrElse(5).asInstanceOf[Int])
                  .agg(getAggregation(query): _*)
                  .where(getFilter(query).get)
                  .build()
            }
            case "timeseries" => {
                DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .agg(getAggregation(query): _*)
                  .where(getFilter(query).get)
                  .build()
            }
            case _ =>
                throw new DataFetcherException("Unknown druid query type found");
        }
    }

    def getAggregation(query: DruidQueryModel): List[AggregationExpression] = {
        query.aggregations.getOrElse(List(org.ekstep.analytics.framework.Aggregation("count", "count", None))).map{f =>
            val aggType = AggregationType.decode(f.`type`).right.getOrElse(AggregationType.Count)
            getAggregationByType(aggType, f.name, f.fieldName)
        }
    }

    def getAggregationByType(aggType: AggregationType, name: String, fieldName: Option[String]): AggregationExpression = {
        aggType match {
            case AggregationType.Count => count as "count"
            case AggregationType.LongSum => longSum(Dim(fieldName.get)) as name
            case AggregationType.DoubleSum => doubleSum(Dim(fieldName.get)) as name
            case AggregationType.DoubleMax => doubleMax(Dim(fieldName.get)) as name
            case AggregationType.DoubleMin => doubleMin(Dim(fieldName.get)) as name
            case AggregationType.LongMax => longMax(Dim(fieldName.get)) as name
            case AggregationType.LongMin => longMin(Dim(fieldName.get)) as name
            case AggregationType.DoubleFirst => doubleFirst(Dim(fieldName.get)) as name
            case AggregationType.DoubleLast => doubleLast(Dim(fieldName.get)) as name
            case AggregationType.LongLast => longLast(Dim(fieldName.get)) as name
            case AggregationType.LongFirst =>longFirst(Dim(fieldName.get)) as name
        }
    }

    def getFilter(query: DruidQueryModel): Option[FilteringExpression] ={
        if (query.filters.nonEmpty) {
            if(query.filters.get.size > 1) {
                val filters = query.filters.get.map { f =>
                    val filterType = FilterType.decode(f.`type`).right.get
                    val values = if (f.values.isEmpty) Option(List(f.value.get)) else f.values
                    getFilterByType(filterType, f.dimension, values)
                }
                Option(conjunction(filters: _*))
            }
            else {
                val filterType = FilterType.decode(query.filters.get.head.`type`).right.get
                val values = if (query.filters.get.head.values.isEmpty) Option(List(query.filters.get.head.value.get)) else query.filters.get.head.values
                Option(getFilterByType(filterType, query.filters.get.head.dimension, values))
            }
        }
        else None
    }

    def getFilterByType(filterType: FilterType, dimension: String, values: Option[List[String]]): FilteringExpression = {
        filterType match {
            case FilterType.Selector => Dim(dimension) === values.get.head
            case FilterType.In => Dim(dimension) in values.get
            case FilterType.Regex => Dim(dimension) regex values.get.head
            case FilterType.Like => Dim(dimension) like values.get.head

        }
    }
}

