package org.ekstep.analytics.framework.fetcher

import java.time.format.DateTimeFormatter
import ing.wbaa.druid._
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.DSL._
import ing.wbaa.druid.dql.Dim
import ing.wbaa.druid.dql.expressions.{AggregationExpression, FilteringExpression, PostAggregationExpression}
import io.circe.Json
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.{DruidFilter, DruidQueryModel}
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import scala.concurrent.Await

object DruidDataFetcher {

    @throws(classOf[DataFetcherException])
    def getDruidData(query: DruidQueryModel): List[String] = {

        // TO-DOs for 2.4.0:
        // add javascript type in getPostAgg methods
        // accept extraction function for dims

        val request = getQuery(query)
        val response = request.execute()
        val queryWaitTimeInMins = AppConf.getConfig("druid.query.wait.time.mins").toLong
        val result = Await.result(response, scala.concurrent.duration.Duration.apply(queryWaitTimeInMins, "minute"))

        if(result.results.length > 0) {
            query.queryType.toLowerCase match {
                case "timeseries" | "groupby" =>
                    val series = result.results.map { f =>
                        f.result.asObject.get.+:("date", Json.fromString(f.timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))).toMap.map { f =>
                            if(f._2.isNull)
                                (f._1 -> "unknown")
                            else if ("String".equalsIgnoreCase(f._2.name))
                                (f._1 -> f._2.asString.get)
                            else if("Number".equalsIgnoreCase(f._2.name))
                                (f._1 -> CommonUtil.roundDouble(f._2.asNumber.get.toDouble, 2))
                            else (f._1 -> f._2)
                        }
                    }
                    series.map(f => JSONUtils.serialize(f))
                case "topn" =>
                    val timeMap = Map("date" -> result.results.head.timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
                    val series = result.results.map(f => f).head.result.asArray.get.map{f =>
                        val dataMap = f.asObject.get.toMap.map{f =>
                            if(f._2.isNull)
                                (f._1 -> "unknown")
                            else if ("String".equalsIgnoreCase(f._2.name))
                                (f._1 -> f._2.asString.get)
                            else if("Number".equalsIgnoreCase(f._2.name))
                                (f._1 -> f._2.asNumber.get.toBigDecimal.get)
                            else (f._1 -> f._2)
                        }
                        timeMap ++ dataMap
                    }.toList
                    series.map(f => JSONUtils.serialize(f))
            }
        }
        else
            List();
    }

    def getQuery(query: DruidQueryModel): DruidQuery = {

        query.queryType.toLowerCase() match {
            case "groupby" => {
                val DQLQuery = DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .agg(getAggregation(query): _*)
                  .groupBy(query.dimensions.get.map(f => Dim(f._1, Option(f._2))): _*)
                if(query.filters.nonEmpty) DQLQuery.where(getFilter(query).get)
                if(query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query).get: _*)
                if(query.having.nonEmpty) DQLQuery.having(getGroupByHaving(query).get)
                DQLQuery.build()
            }
            case "topn" => {
                val DQLQuery = DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .topN(Dim(query.dimensions.get.head._1, Option(query.dimensions.get.head._2)), query.metric.getOrElse("count"), query.threshold.getOrElse(100).asInstanceOf[Int])
                  .agg(getAggregation(query): _*)
                if(query.filters.nonEmpty) DQLQuery.where(getFilter(query).get)
                if(query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query).get: _*)
                DQLQuery.build()
            }
            case "timeseries" => {
                val DQLQuery = DQL
                  .from(query.dataSource)
                  .granularity(CommonUtil.getGranularity(query.granularity.getOrElse("all")))
                  .interval(CommonUtil.getIntervalRange(query.intervals))
                  .agg(getAggregation(query): _*)
                if(query.filters.nonEmpty) DQLQuery.where(getFilter(query).get)
                if(query.postAggregation.nonEmpty) DQLQuery.postAgg(getPostAggregation(query).get: _*)
                DQLQuery.build()
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
            case AggregationType.Count => count as name
            case AggregationType.HyperUnique => dim(fieldName.get).hyperUnique as name
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
                    val values = if (f.values.isEmpty) Option(List(f.value.get)) else f.values
                    getFilterByType(f.`type`, f.dimension, values)
                }
                Option(conjunction(filters: _*))
            }
            else {
                val values = if (query.filters.get.head.values.isEmpty) Option(List(query.filters.get.head.value.get)) else query.filters.get.head.values
                Option(getFilterByType(query.filters.get.head.`type`, query.filters.get.head.dimension, values))
            }
        }
        else None
    }

    def getFilterByType(filterType: String, dimension: String, values: Option[List[AnyRef]]): FilteringExpression = {
        filterType.toLowerCase match {
            case "isnull" => Dim(dimension).isNull
            case "isnotnull" => Dim(dimension).isNotNull
            case "equals" => Dim(dimension) === values.get.head.asInstanceOf[String]
            case "notequals" => Dim(dimension) =!= values.get.head.asInstanceOf[String]
            case "containsignorecase" => Dim(dimension).containsIgnoreCase(values.get.head.asInstanceOf[String])
            case "contains" => Dim(dimension).contains(values.get.head.asInstanceOf[String], true)
            case "in" => Dim(dimension) in values.get.asInstanceOf[List[String]]
            case "notin" => Dim(dimension) notIn values.get.asInstanceOf[List[String]]
            case "regex" => Dim(dimension) regex values.get.head.asInstanceOf[String]
            case "like" => Dim(dimension) like values.get.head.asInstanceOf[String]
            case "greaterthan" => Dim(dimension) > values.get.head.asInstanceOf[Number].doubleValue()
            case "lessthan" => Dim(dimension) < values.get.head.asInstanceOf[Number].doubleValue()

        }
    }

    def getPostAggregation(query: DruidQueryModel): Option[List[PostAggregationExpression]] = {
        if (query.postAggregation.nonEmpty){
            Option(query.postAggregation.get.map{ f =>
                val postAggType = PostAggregationType.decode(f.`type`).right.get
                getPostAggregationByType(postAggType, f.name, f.fields, f.fn)
            })
        }
        else None

    }

    def getPostAggregationByType(postAggType: PostAggregationType, name: String, fields: Option[List[String]], fn: Option[String]): PostAggregationExpression = {
        postAggType match {
            case PostAggregationType.Arithmetic =>
                val leftField = fields.get.head
                val rightField = fields.get.apply(1)
                fn.get match {
                    case "+" => Dim(leftField).+(Dim(rightField)) as name
                    case "-" => Dim(leftField).-(Dim(rightField)) as name
                    case "*" => Dim(leftField).*(Dim(rightField)) as name
                    case "/" => Dim(leftField)./(Dim(rightField)) as name
                }
            //case PostAggregationType.Javascript =>

        }
    }

    def getGroupByHaving(query: DruidQueryModel): Option[FilteringExpression] = {
        if (query.having.nonEmpty){
            val havingType = HavingType.decode(query.having.get.`type`).right.get
            Option(getGroupByHavingByType(havingType, query.having.get.aggregation, query.having.get.value))
        }
        else None

    }

    def getGroupByHavingByType(postAggType: HavingType, field: String, value: String): FilteringExpression = {
        postAggType match {
            case HavingType.EqualTo => Dim(field) === value
            case HavingType.Not => Dim(field) =!= value
            case HavingType.GreaterThan => Dim(field) > value
            case HavingType.LessThan => Dim(field) < value
        }
    }
}

