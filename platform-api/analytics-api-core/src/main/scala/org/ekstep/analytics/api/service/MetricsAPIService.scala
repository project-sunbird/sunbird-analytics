package org.ekstep.analytics.api.service

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.APIIds
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.api.MetricsRequest
import org.ekstep.analytics.api.MetricsRequestBody
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.metrics.ContentPopularityMetricsModel
import org.ekstep.analytics.api.metrics.ContentUsageListMetricsModel
import org.ekstep.analytics.api.metrics.CotentUsageMetricsModel
import org.ekstep.analytics.api.metrics.GenieLaunchMetricsModel
import org.ekstep.analytics.api.metrics.ItemUsageMetricsModel
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

    val reqPeriods = Array("LAST_7_DAYS", "LAST_5_WEEKS", "LAST_12_MONTHS", "CUMULATIVE");

    case class ContentUsage(body: MetricsRequestBody, sc: SparkContext, config: Config);
    case class ContentPopularity(body: MetricsRequestBody, fields: Array[String], sc: SparkContext, config: Config);
    case class ContentList(body: MetricsRequestBody, sc: SparkContext, config: Config);
    case class GenieLaunch(body: MetricsRequestBody, sc: SparkContext, config: Config);
    case class ItemUsage(body: MetricsRequestBody, sc: SparkContext, config: Config);
    case class Metrics(dataset: String, summary: String, body: MetricsRequestBody, sc: SparkContext, config: Config);

    def metrics(dataset: String, summary: String, body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {

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

    private def consumptionMetrics(summary: String, body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {

        summary match {
            case "content-usage" =>
                contentUsageMetrics(body);
            case _ =>
                CommonUtil.errorResponseSerialized(APIIds.METRICS_API, "Aggregations are not supported for summary - " + summary, ResponseCode.SERVER_ERROR.toString())
        }
    }

    private def contentUsageMetrics(body: MetricsRequestBody)(implicit config: Config): String = {
        val response = """{"id":"ekstep.analytics.metrics","ver":"1.0","ts":"2017-07-27T09:35:15.027+00:00","params":{"resmsgid":"56de001d-b8e2-4a9e-b631-9a1b59b12e71","status":"successful"},"responseCode":"OK","result":{"metrics":[{"d_period":20170726,"label":"Jul 26 Wed","m_total_ts":234,"m_total_sessions":23,"m_avg_ts_session":10.003,"m_total_interactions":160,"m_avg_interactions_min":40,"m_total_devices_count":4,"m_total_users_count":20,"m_total_content_count":2},{"d_period":20170725,"label":"Jul 25 Tue","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices_count":0,"m_total_users_count":0,"m_total_content_count":0},{"d_period":20170724,"label":"Jul 24 Mon","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices_count":0,"m_total_users_count":0,"m_total_content_count":0},{"d_period":20170723,"label":"Jul 23 Sun","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices_count":0,"m_total_users_count":0,"m_total_content_count":0},{"d_period":20170722,"label":"Jul 22 Sat","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices_count":0,"m_total_users_count":0,"m_total_content_count":0},{"d_period":20170721,"label":"Jul 21 Fri","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices_count":0,"m_total_users_count":0,"m_total_content_count":0},{"d_period":20170720,"label":"Jul 20 Thu","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices_count":0,"m_total_users_count":0,"m_total_content_count":0}],"summary":{"m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices_count":0,"m_total_users_count":0,"m_total_content_count":0}}}""";
        response;
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
        case Metrics(dataset: String, summary: String, body: MetricsRequestBody, sc: SparkContext, config: Config) => sender() ! metrics(dataset, summary, body)(sc, config);
    }
}