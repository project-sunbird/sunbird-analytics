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
import scala.util.Random
import scala.collection.JavaConversions._

/**
 * @author mahesh
 */

object MockMetricsAPIService {

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
				ContentUsageMetrics(Option(period), 12, 123.34, 123.0, 6, 34, 102.35, 2)
			}
		} else {
			Array();
		}
		val summary = ContentUsageMetrics(None, 12*count, 123.34*count, 123.0*count, 6*count, 34*count, 102.35*count, 2*count);
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

	def itemUsage(requestBody: MetricsRequestBody)(implicit sc: SparkContext): String = {
		if (StringUtils.isEmpty(requestBody.request.period) || periods.indexOf(requestBody.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		val count = periodMap.getOrElse(requestBody.request.period, 0);
		val dateTime = new DateTime(new Date());
		val metrics = if (count > 0) {
			for (i <- 1 to count) yield {
				val period = _getPeriod(i, count);
				val items = for (t <- 1 to Random.nextInt(10)) yield {
					ItemMetrics("Q_"+Random.nextInt(100), 144.00, 10, 4, 6, Array("Fourteen", "Twelve", "Sixteen", "Seventeen"), 101.00);
				};
				Map[String, AnyRef]("d_period" -> period, "items" -> items);
			}
		} else {
			Array();
		}
		
		val items = for (t <- 1 to 10) yield {
			ItemMetrics("Q_"+Random.nextInt(100), 144.00*count, 10*count, 4*count, 6*count, Array("Fourteen", "Twelve", "Sixteen", "Seventeen"), 101.00*count);
		};
		val summary = Map("items" -> items);
		val result = Map[String, AnyRef](
			"ttl" -> CommonUtil.getRemainingHours.asInstanceOf[AnyRef],
            "metrics" -> metrics,
            "summary" -> summary);
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.metrics.item-usage", result));
	}
	
	def genieLaunch(requestBody: MetricsRequestBody)(implicit sc: SparkContext): String = {
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
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.metrics.genie-launch", result));
	}

	def contentList(requestBody: MetricsRequestBody)(implicit sc: SparkContext): String = {
		if (StringUtils.isEmpty(requestBody.request.period) || periods.indexOf(requestBody.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		val contentIds = java.util.Arrays.asList(Array[String]("domain_14433", "domain_14443", "domain_63844", "domain_51435", "domain_51548", "domain_17221", "domain_47563", "domain_49024", "domain_63382", "org.ekstep.ms_52fdbc1e69702d16cd040000", "test.org.ekstep.action-chaining", "domain_66672", "do_10093911", "domain_59928", "domain_48905", "domain_57434", "domain_60598", "domain_59927", "domain_66036", "domain_66114", "domain_66054", "domain_63651", "domain_63839", "numeracy_418", "domain_46378", "domain_66675", "domain_66804", "domain_48865", "org.ekstep.story.en.haircut", "org.ekstep.lit.haircut.story", "domain_17251", "domain_44689", "domain_45086", "org.ekstep.aser", "ecml.org.ekstep.testbook.audio", "do_20081411", "domain_63256", "test.org.ekstep.barahkadi-sprite", "domain_48617", "domain_70615", "test.org.ekstep.beta-mp3", "domain_55111", "do_20093384", "domain_70619", "do_20072218", "domain_66039", "domain_10879", "domain_14421"):_*);
		val count = periodMap.getOrElse(requestBody.request.period, 0);
		val dateTime = new DateTime(new Date());
		val metrics = if (count > 0) {
			for (i <- 1 to count) yield {
				java.util.Collections.shuffle(contentIds);
				val selectedIds = contentIds.subList(0, Random.nextInt(10));
				val contentList = for (id <- selectedIds) yield {
					RecommendationAPIService.contentBroadcastMap.value.getOrElse(id, Map())	
				}
				val contents = contentList.map(f => if(!f.isEmpty) f)
				val period = _getPeriod(i, count);
				Map[String, AnyRef]("d_period" -> period, "content" -> contents, "count" -> Int.box(contents.length));
			}
		} else {
			Array();
		}
		
		java.util.Collections.shuffle(contentIds);
		val selectedIds = contentIds.subList(0, 10);
		val contentList = for (id <- selectedIds) yield {
			RecommendationAPIService.contentBroadcastMap.value.getOrElse(id, Map())	
		};
		val contents = contentList.map(f => if(!f.isEmpty) f)
		val summary = Map("content"-> contents, "count" -> Int.box(contents.length));
		val result = Map[String, AnyRef](
			"ttl" -> CommonUtil.getRemainingHours.asInstanceOf[AnyRef],
            "metrics" -> metrics,
            "summary" -> summary);
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.content-list", result));
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