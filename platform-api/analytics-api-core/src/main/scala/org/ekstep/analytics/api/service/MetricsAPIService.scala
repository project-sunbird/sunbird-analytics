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
    case ContentUsage(body: MetricsRequestBody, sc: SparkContext, config: Config)                             => sender() ! contentUsage(body)(sc, config);
    case ContentPopularity(body: MetricsRequestBody, fields: Array[String], sc: SparkContext, config: Config) => sender() ! contentPopularity(body, fields)(sc, config);
    case ContentList(body: MetricsRequestBody, sc: SparkContext, config: Config)                              => sender() ! contentList(body)(sc, config);
    case GenieLaunch(body: MetricsRequestBody, sc: SparkContext, config: Config)                              => sender() ! genieLaunch(body)(sc, config);
    case ItemUsage(body: MetricsRequestBody, sc: SparkContext, config: Config)                                => sender() ! itemUsage(body)(sc, config);
  }
}