package org.ekstep.analytics.api.service

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.APIIds
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.api.MetricsRequest
import org.ekstep.analytics.api.MetricsRequestBody
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.metrics._
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.RestUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.actorRef2Scala

/**
 * @author mahesh
 */

object MetricsAPIService {

    val reqPeriods = Array("LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_5_WEEKS", "LAST_12_MONTHS", "CUMULATIVE");

    case class ContentUsage(body: MetricsRequestBody, config: Config);
    case class ContentPopularity(body: MetricsRequestBody, fields: Array[String], config: Config);
    case class ContentList(body: MetricsRequestBody, config: Config);
    case class GenieLaunch(body: MetricsRequestBody, config: Config);
    case class ItemUsage(body: MetricsRequestBody, config: Config);
    case class WorkflowUsage(body: MetricsRequestBody, config: Config);
    case class Metrics(dataset: String, summary: String, body: MetricsRequestBody, config: Config);

    def metrics(dataset: String, summary: String, body: MetricsRequestBody)(implicit config: Config): String = {

        dataset match {
            case "creation" =>
                creationMetrics(summary, body);
            case "consumption" =>
                consumptionMetrics(summary, body);
            case _ =>
                CommonUtil.errorResponseSerialized(APIIds.METRICS_API, "Aggregations are not supported for dataset - " + summary, ResponseCode.SERVER_ERROR.toString())
        }
    }

    private def creationMetrics(summary: String, body: MetricsRequestBody)(implicit config: Config): String = {

        summary match {
            case "content-snapshot" =>
                contentSnapshotMetrics(body);
            case _ =>
                CommonUtil.errorResponseSerialized(APIIds.METRICS_API, "Aggregations are not supported for summary - " + summary, ResponseCode.SERVER_ERROR.toString())
        }
    }

    private def consumptionMetrics(summary: String, body: MetricsRequestBody)(implicit config: Config): String = {

        summary match {
            case "content-usage" =>
                contentUsageMetrics(body);
            case _ =>
                CommonUtil.errorResponseSerialized(APIIds.METRICS_API, "Aggregations are not supported for summary - " + summary, ResponseCode.SERVER_ERROR.toString())
        }
    }

    private def contentUsageMetrics(body: MetricsRequestBody)(implicit config: Config): String = {
        if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
            CommonUtil.errorResponseSerialized(APIIds.CONTENT_USAGE, "period is missing or invalid.", ResponseCode.CLIENT_ERROR.toString())
        } else {
            try {
                val filter = body.request.filter.getOrElse(Filter());
                val userId = filter.user_id.getOrElse("all");
                val contentId = filter.content_id.getOrElse("all");
                val channelId = if (body.request.channel.isEmpty) config.getString("default.channel.id") else body.request.channel.get
                val tag = getTag(filter);
                val result = UsageMetricsModel.fetch(contentId, tag, body.request.period, Array(), channelId, userId);
                JSONUtils.serialize(CommonUtil.OK(APIIds.CONTENT_USAGE, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.CONTENT_USAGE, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
            }
        }
    }

    private def contentSnapshotMetrics(body: MetricsRequestBody)(implicit config: Config): String = {
        val url = config.getString("metrics.creation.es.url")
        val indexes = config.getString("metrics.creation.es.indexes")
        val apiURL = url + "/" + indexes + "/" + "_search"
        if (body.request.rawQuery.isDefined) {
            val query = body.request.rawQuery.get ++ Map("size" -> 0)
            val result = RestUtil.post[Map[String, AnyRef]](apiURL, JSONUtils.serialize(query))
            try {
                JSONUtils.serialize(CommonUtil.OK(APIIds.METRICS_API, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.METRICS_API, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
            }
        } else {
            CommonUtil.errorResponseSerialized(APIIds.METRICS_API, "Raw query cannot be empty", ResponseCode.SERVER_ERROR.toString())
        }
    }

    def main(args: Array[String]): Unit = {

        val rawQuery = """{"query": {"filtered": {"query": {"bool": {"must": [{"query": {"range": {"lastUpdatedOn": {"gt": "2017-07-24T00:00:00.000+0530", "lte": "2017-08-01T00:00:00.000+0530"} } } }, {"match": {"createdFor.raw": "Sunbird"} } ] } } } }, "size": 0, "aggs": {"created_on": {"date_histogram": {"field": "lastUpdatedOn", "interval": "1d", "format": "yyyy-MM-dd"} }, "status": {"terms": {"field": "status.raw", "include": ["draft", "live", "review"] }, "aggs": {"updated_on": {"date_histogram": {"field": "lastUpdatedOn", "interval": "1d", "format": "yyyy-MM-dd"} } } }, "authors.count": {"cardinality": {"field": "createdBy.raw", "precision_threshold": 100 } }, "content_count": {"terms": {"field": "objectType.raw", "include": "content"} } } }""";
        val request = MetricsRequest("", None, None, Option(JSONUtils.deserialize[Map[String, AnyRef]](rawQuery)));
        val body = MetricsRequestBody("org.ekstep.analytics.aggregate-metrics", "1.0", "", request, None)
        implicit val config = ConfigFactory.parseMap(Map("metrics.creation.es.url" -> "http://localhost:9200", "metrics.creation.es.indexes" -> "compositesearch").asJava);
        implicit val sc = org.ekstep.analytics.framework.util.CommonUtil.getSparkContext(1, "Test");
        println(metrics("creation", "content-snapshot", body));
    }

    def contentUsage(body: MetricsRequestBody)(implicit config: Config): String = {
        if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
            CommonUtil.errorResponseSerialized(APIIds.CONTENT_USAGE, "period is missing or invalid.", ResponseCode.CLIENT_ERROR.toString())
        } else {
            try {
                val filter = body.request.filter.getOrElse(Filter());
                val contentId = filter.content_id.getOrElse("all");
                val channelId = if (body.request.channel.isEmpty) config.getString("default.channel.id") else body.request.channel.get
                val tag = getTag(filter);
                val result = CotentUsageMetricsModel.fetch(contentId, tag, body.request.period, Array(), channelId);
                val res = JSONUtils.serialize(CommonUtil.OK(APIIds.CONTENT_USAGE, result));
                res
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.CONTENT_USAGE, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
            }
        }
    }

    def contentPopularity(body: MetricsRequestBody, fields: Array[String])(implicit config: Config): String = {
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

    def contentList(body: MetricsRequestBody)(implicit config: Config): String = {
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

    def genieLaunch(body: MetricsRequestBody)(implicit config: Config): String = {
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

    def itemUsage(body: MetricsRequestBody)(implicit config: Config): String = {
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

    def workflowUsage(body: MetricsRequestBody)(implicit config: Config): String = {
        if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
            CommonUtil.errorResponseSerialized(APIIds.WORKFLOW_USAGE, "period is missing or invalid.", ResponseCode.CLIENT_ERROR.toString())
        } else {
            try {
                val filter = body.request.filter.getOrElse(Filter());
                val userId = filter.user_id.getOrElse("all");
                val contentId = filter.content_id.getOrElse("all");
                val channelId = if (body.request.channel.isEmpty) config.getString("default.channel.id") else body.request.channel.get
                val tag = getTag(filter);
                val deviceId = filter.device_id.getOrElse("all");
                val metrics_type = filter.metrics_type.getOrElse("app")
                val mode = filter.mode.getOrElse("")
                val result = WorkflowUsageMetricsModel.fetch(contentId, tag, body.request.period, Array(), channelId, userId, deviceId, metrics_type, mode);
                JSONUtils.serialize(CommonUtil.OK(APIIds.WORKFLOW_USAGE, result));
            } catch {
                case ex: Exception =>
                    CommonUtil.errorResponseSerialized(APIIds.WORKFLOW_USAGE, ex.getMessage, ResponseCode.SERVER_ERROR.toString())
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
        case ContentUsage(body: MetricsRequestBody, config: Config)                              => sender() ! contentUsage(body)(config);
        case ContentPopularity(body: MetricsRequestBody, fields: Array[String], config: Config)  => sender() ! contentPopularity(body, fields)(config);
        case ContentList(body: MetricsRequestBody, config: Config)                               => sender() ! contentList(body)(config);
        case GenieLaunch(body: MetricsRequestBody, config: Config)                               => sender() ! genieLaunch(body)(config);
        case ItemUsage(body: MetricsRequestBody, config: Config)                                 => sender() ! itemUsage(body)(config);
        case WorkflowUsage(body: MetricsRequestBody, config: Config)                             => sender() ! workflowUsage(body)(config);
        case Metrics(dataset: String, summary: String, body: MetricsRequestBody, config: Config) => sender() ! metrics(dataset, summary, body)(config);
    }
}