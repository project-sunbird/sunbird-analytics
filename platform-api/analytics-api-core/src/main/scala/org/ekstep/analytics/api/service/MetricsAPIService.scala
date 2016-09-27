package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import org.ekstep.analytics.api._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.lang.StringUtils
import org.ekstep.analytics.api.exception.ClientException
import java.util.List
import java.util.Date
import org.joda.time.DateTime
import scala.util.Random
import scala.collection.JavaConversions._
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.DataFetcher
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.jets3t.service.S3ServiceException
import com.typesafe.config.Config
import org.apache.hadoop.mapred.InvalidInputException
import org.ekstep.analytics.api.metrics.CotentUsageMetricsModel
import org.ekstep.analytics.api.metrics.ContentPopularityMetricsModel

/**
 * @author mahesh
 */

object MetricsAPIService {
	
	val reqPeriods = Array("LAST_7_DAYS", "LAST_5_WEEKS", "LAST_12_MONTHS", "CUMULATIVE");
	
	def contentUsage(body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {
		if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		val filter = body.request.filter.getOrElse(Filter());
		val contentId = filter.content_id.getOrElse("all");
		val tag = filter.tag.getOrElse("all");
		val result = CotentUsageMetricsModel.fetch(contentId, tag, body.request.period);
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.metrics.content-usage", result));
	}
	
	def contentPopularity(body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {
		if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		val filter = body.request.filter.getOrElse(Filter());
		if (StringUtils.isEmpty(filter.content_id.get)) {
				throw new ClientException("filter.content_id is missing.");
		}
		val contentId = filter.content_id.get;
		val tag = filter.tag.getOrElse("all");
		val result = ContentPopularityMetricsModel.fetch(contentId, tag, body.request.period);
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.metrics.content-usage", result));
	}
	
}