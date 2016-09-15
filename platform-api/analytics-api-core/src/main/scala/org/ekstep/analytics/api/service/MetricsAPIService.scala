package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import org.ekstep.analytics.api._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.util.CommonUtil
import org.apache.commons.lang.StringUtils
import org.ekstep.analytics.api.exception.ClientException
import java.util.List
import java.util.Date
import org.joda.time.DateTime


object MetricsAPIService {

	val periodMap = Map[String, Int]("LAST_7_DAYS" -> 7, "LAST_5_WEEKS" -> 5, "LAST_12_MONTHS" -> 12, "CUMULATIVE" -> 0);
	val periods = Array("LAST_7_DAYS", "LAST_5_WEEKS", "LAST_12_MONTHS", "CUMULATIVE");
	
	def contentUsage(requestBody: MetricsRequestBody)(implicit sc: SparkContext): String = {
		if (StringUtils.isEmpty(requestBody.request.period) || periods.indexOf(requestBody.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		val count = periodMap.getOrElse(requestBody.request.period, 0);
		val dateTime = new DateTime(new Date());
		val metrics = if (count > 0) {
			for (i <- 1 to count) yield {
				val period = _getPeriod(i, count);
				ContentUsageMetrics(Option(period), 12, 123.34, 123.0, 6, 34, 102.35, 2, None)
			}
		} else {
			Array();
		}
		val summary = ContentUsageMetrics(None, 12*count, 123.34*count, 123.0*count, 6*count, 34*count, 102.35*count, 2*count, None);
		val result = Map[String, AnyRef](
			"ttl" -> CommonUtil.getRemainingHours.asInstanceOf[AnyRef],
            "metrics" -> metrics,
            "summary" -> summary);
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.metrics.content-usage", result));
	}

	def contentPopularity(requestBody: MetricsRequestBody)(implicit sc: SparkContext): String = {
		if (StringUtils.isEmpty(requestBody.request.period) || periods.indexOf(requestBody.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		val count = periodMap.getOrElse(requestBody.request.period, 0);
		val dateTime = new DateTime(new Date());
		val metrics = if (count > 0) {
			for (i <- 1 to count) yield {
				val period = _getPeriod(i, count);
				ContentPopularityMetrics(Option(period), 3, 5, None, 3.4);
			}
		} else {
			Array();
		}

		val summary = ContentPopularityMetrics(None, 3*count, 5*count, None, 3.4);
		val result = Map[String, AnyRef](
			"ttl" -> CommonUtil.getRemainingHours.asInstanceOf[AnyRef],
            "metrics" -> metrics,
            "summary" -> summary);
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.metrics.content-popularity", result));
	}

	def genieUsage(requestBody: MetricsRequestBody)(implicit sc: SparkContext): String = {
		if (StringUtils.isEmpty(requestBody.request.period) || periods.indexOf(requestBody.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		val count = periodMap.getOrElse(requestBody.request.period, 0);
		val dateTime = new DateTime(new Date());
		val metrics = if (count > 0) {
			for (i <- 1 to count) yield {
				val period = _getPeriod(i, count);
				GenieUsageMetrics(Option(period), 12, 123.45, 4, 8, 109.64);
			}
		} else {
			Array();
		}

		val summary = GenieUsageMetrics(None, 12*count, 123.45*count, 4, 8*count, 109.64*count);
		val result = Map[String, AnyRef](
			"ttl" -> CommonUtil.getRemainingHours.asInstanceOf[AnyRef],
            "metrics" -> metrics,
            "summary" -> summary);
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.metrics.genie-usage", result));
	}

	def contentList(requestBody: MetricsRequestBody)(implicit sc: SparkContext): String = {
		if (StringUtils.isEmpty(requestBody.request.period) || periods.indexOf(requestBody.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.content-list", Map()));
	}
	
	private def _getPeriod(index: Int, period: Int): Integer = {
		val dateTime = new DateTime(new Date());
		period match {
			case 7 => org.ekstep.analytics.framework.util.CommonUtil.getPeriod(dateTime.plusDays(-index).getMillis, org.ekstep.analytics.framework.Period.DAY);
			case 5 => org.ekstep.analytics.framework.util.CommonUtil.getPeriod(dateTime.plusWeeks(-index).getMillis, org.ekstep.analytics.framework.Period.WEEK);
			case 12 => org.ekstep.analytics.framework.util.CommonUtil.getPeriod(dateTime.plusMonths(-index).getMillis, org.ekstep.analytics.framework.Period.MONTH);
		}
	}
}