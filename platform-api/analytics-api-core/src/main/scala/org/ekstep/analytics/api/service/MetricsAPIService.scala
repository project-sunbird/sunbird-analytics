package org.ekstep.analytics.api.service

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.APIIds
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.api.MetricsRequestBody
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.metrics.ContentPopularityMetricsModel
import org.ekstep.analytics.api.metrics.ContentUsageListMetricsModel
import org.ekstep.analytics.api.metrics.CotentUsageMetricsModel
import org.ekstep.analytics.api.metrics.GenieLaunchMetricsModel
import org.ekstep.analytics.api.metrics.ItemUsageMetricsModel
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import com.typesafe.config.Config
import akka.actor.Actor
import akka.actor.Props
import akka.actor.actorRef2Scala
import org.ekstep.analytics.api.AggregateMetricsRequestBody
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.api.AggregateMetricsRequest
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * @author mahesh
 */

object MetricsAPIService {

    val reqPeriods = Array("LAST_7_DAYS", "LAST_5_WEEKS", "LAST_12_MONTHS", "CUMULATIVE");

    case class ContentUsage(body: MetricsRequestBody, sc: SparkContext, config: Config);
    case class ContentPopularity(body: MetricsRequestBody, fields: Array[String], sc: SparkContext, config: Config);
    case class ContentList(body: MetricsRequestBody, sc: SparkContext, config: Config);
    case class GenieLaunch(body: MetricsRequestBody, sc: SparkContext, config: Config);
    case class ItemUsage(body: MetricsRequestBody, sc: SparkContext, config: Config);
    case class AggregateMetrics(dataset: String, summary: String, body: AggregateMetricsRequestBody, config: Config);

    def aggregateMetrics(dataset: String, summary: String, body: AggregateMetricsRequestBody)(implicit config: Config): String = {

        dataset match {
            case "creation" =>
                aggCreationMetrics(summary, body);
            case _ =>
                CommonUtil.errorResponseSerialized(APIIds.METRICS_AGGREGRATIONS, "Aggregations are not supported for dataset - " + summary, ResponseCode.SERVER_ERROR.toString())
        }
    }

    private def aggCreationMetrics(summary: String, body: AggregateMetricsRequestBody)(implicit config: Config): String = {
        
        summary match {
            case "content-snapshot" =>
                contentSnapshotAggregateMetrics(body);
            case _ =>
                CommonUtil.errorResponseSerialized(APIIds.METRICS_AGGREGRATIONS, "Aggregations are not supported for summary - " + summary, ResponseCode.SERVER_ERROR.toString())
        }
    }

    private def contentSnapshotAggregateMetrics(body: AggregateMetricsRequestBody)(implicit config: Config): String = {
        val url = config.getString("metrics.creation.es.url")
        val indexes = config.getString("metrics.creation.es.indexes")
        val apiURL = url + "/" + indexes + "/" + "_search"
        if(body.request.rawQuery.isDefined) {
            val query = body.request.rawQuery.get ++ Map("size" -> 0)
            val result = RestUtil.post[Map[String, AnyRef]](apiURL, JSONUtils.serialize(query))
            try {
                JSONUtils.serialize(CommonUtil.OK(APIIds.METRICS_AGGREGRATIONS, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.METRICS_AGGREGRATIONS, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
            }
        } else {
            CommonUtil.errorResponseSerialized(APIIds.METRICS_AGGREGRATIONS, "Raw query cannot be empty", ResponseCode.SERVER_ERROR.toString())
        }
    }

    def main(args: Array[String]): Unit = {

        val rawQuery = """{"query": {"filtered": {"query": {"bool": {"must": [{"query": {"range": {"lastUpdatedOn": {"gt": "2017-07-24T00:00:00.000+0530", "lte": "2017-08-01T00:00:00.000+0530"} } } }, {"match": {"createdFor.raw": "Sunbird"} } ] } } } }, "size": 0, "aggs": {"created_on": {"date_histogram": {"field": "lastUpdatedOn", "interval": "1d", "format": "yyyy-MM-dd"} }, "status": {"terms": {"field": "status.raw", "include": ["draft", "live", "review"] }, "aggs": {"updated_on": {"date_histogram": {"field": "lastUpdatedOn", "interval": "1d", "format": "yyyy-MM-dd"} } } }, "authors.count": {"cardinality": {"field": "createdBy.raw", "precision_threshold": 100 } }, "content_count": {"terms": {"field": "objectType.raw", "include": "content"} } } }""";
        val request = AggregateMetricsRequest(None, None, None);
        val body = AggregateMetricsRequestBody("org.ekstep.analytics.aggregate-metrics", "1.0", "", request, None)
        implicit val config = ConfigFactory.parseMap(Map("metrics.creation.es.url" -> "http://localhost:9200", "metrics.creation.es.indexes" -> "compositesearch").asJava);
        println(aggregateMetrics("creation", "content-snapshot", body));
    }

    def contentUsage(body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {
        if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
            CommonUtil.errorResponseSerialized(APIIds.CONTENT_USAGE, "period is missing or invalid.", ResponseCode.CLIENT_ERROR.toString())
        } else {
            try {
                val filter = body.request.filter.getOrElse(Filter());
                val contentId = filter.content_id.getOrElse("all");
                val channelId = if (body.request.channel.isEmpty) config.getString("default.channel.id") else body.request.channel.get
                val tag = getTag(filter);
                val result = CotentUsageMetricsModel.fetch(contentId, tag, body.request.period, Array(), channelId);
                JSONUtils.serialize(CommonUtil.OK(APIIds.CONTENT_USAGE, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.CONTENT_USAGE, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
            }
        }
    }

    def contentPopularity(body: MetricsRequestBody, fields: Array[String])(implicit sc: SparkContext, config: Config): String = {
        val filter = body.request.filter.getOrElse(Filter());
        if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
            CommonUtil.errorResponseSerialized(APIIds.CONTENT_POPULARITY, "period is missing or invalid.", ResponseCode.CLIENT_ERROR.toString())
        } else if (filter.content_id.isEmpty) {
            CommonUtil.errorResponseSerialized(APIIds.CONTENT_POPULARITY, "filter.content_id is missing.", ResponseCode.CLIENT_ERROR.toString())
        } else {
            try {
                val contentId = filter.content_id.get;
                val tag = getTag(filter);
                val channelId = if (body.request.channel.isEmpty) config.getString("default.channel.id") else body.request.channel.get
                val result = ContentPopularityMetricsModel.fetch(contentId, tag, body.request.period, fields, channelId);
                JSONUtils.serialize(CommonUtil.OK(APIIds.CONTENT_POPULARITY, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.CONTENT_POPULARITY, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
            }
        }
    }

    def contentList(body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {
        if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
            CommonUtil.errorResponseSerialized(APIIds.CONTENT_LIST, "period is missing or invalid.", ResponseCode.CLIENT_ERROR.toString())
        } else {
            try {
                val filter = body.request.filter.getOrElse(Filter());
                val contentId = filter.content_id.getOrElse("all");
                val tag = getTag(filter);
                val channelId = if (body.request.channel.isEmpty) config.getString("default.channel.id") else body.request.channel.get
                val result = ContentUsageListMetricsModel.fetch(contentId, tag, body.request.period, Array(), channelId);
                JSONUtils.serialize(CommonUtil.OK(APIIds.CONTENT_LIST, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.CONTENT_LIST, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
            }
        }
    }

    def genieLaunch(body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {
        if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
            CommonUtil.errorResponseSerialized(APIIds.GENIE_LUNCH, "period is missing or invalid.", ResponseCode.CLIENT_ERROR.toString())
        } else {
            try {
                val filter = body.request.filter.getOrElse(Filter());
                val contentId = filter.content_id.getOrElse("all");
                val tag = getTag(filter);
                val channelId = if (body.request.channel.isEmpty) config.getString("default.channel.id") else body.request.channel.get
                val result = GenieLaunchMetricsModel.fetch(contentId, tag, body.request.period, Array(), channelId);
                JSONUtils.serialize(CommonUtil.OK(APIIds.GENIE_LUNCH, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.GENIE_LUNCH, ex.getMessage, ResponseCode.SERVER_ERROR.toString())

            }
        }
    }

    def itemUsage(body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {
        val filter = body.request.filter.getOrElse(Filter());
        if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
            CommonUtil.errorResponseSerialized(APIIds.ITEM_USAGE, "period is missing or invalid.", ResponseCode.CLIENT_ERROR.toString())
        } else if (filter.content_id.isEmpty) {
            CommonUtil.errorResponseSerialized(APIIds.ITEM_USAGE, "filter.content_id is missing.", ResponseCode.CLIENT_ERROR.toString())
        } else {
            try {
                val contentId = filter.content_id.get;
                val tag = getTag(filter);
                val channelId = if (body.request.channel.isEmpty) config.getString("default.channel.id") else body.request.channel.get
                val result = ItemUsageMetricsModel.fetch(contentId, tag, body.request.period, Array(), channelId);
                JSONUtils.serialize(CommonUtil.OK(APIIds.ITEM_USAGE, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.ITEM_USAGE, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
            }
        }
    }

    private def getTag(filter: Filter): String = {
        val tags = filter.tags.getOrElse(Array());
        if (tags.length == 0) {
            filter.tag.getOrElse("all");
        } else {
            tags.mkString(",");
        }
    }

}

class MetricsAPIService extends Actor {
    import MetricsAPIService._;

    def receive = {
        case ContentUsage(body: MetricsRequestBody, sc: SparkContext, config: Config)                              => sender() ! contentUsage(body)(sc, config);
        case ContentPopularity(body: MetricsRequestBody, fields: Array[String], sc: SparkContext, config: Config)  => sender() ! contentPopularity(body, fields)(sc, config);
        case ContentList(body: MetricsRequestBody, sc: SparkContext, config: Config)                               => sender() ! contentList(body)(sc, config);
        case GenieLaunch(body: MetricsRequestBody, sc: SparkContext, config: Config)                               => sender() ! genieLaunch(body)(sc, config);
        case ItemUsage(body: MetricsRequestBody, sc: SparkContext, config: Config)                                 => sender() ! itemUsage(body)(sc, config);
        case AggregateMetrics(dataset: String, summary: String, body: AggregateMetricsRequestBody, config: Config) => sender() ! aggregateMetrics(dataset, summary, body)(config);
    }
}