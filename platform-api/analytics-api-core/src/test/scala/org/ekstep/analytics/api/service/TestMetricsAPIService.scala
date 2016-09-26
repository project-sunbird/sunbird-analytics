package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.SparkSpec
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.MetricsRequestBody
import org.ekstep.analytics.api.MetricsResponse
import org.ekstep.analytics.api.Response
import com.typesafe.config.ConfigFactory

class TestMetricsAPIService extends SparkSpec {
	val config = ConfigFactory.load();
	override def beforeAll() {
        super.beforeAll()
        RecommendationAPIService.initCache()(sc, config);
	}
	
	private def getContentUsageMetrics(request: String) : MetricsResponse = {
		val result = MetricsAPIService.contentUsage(JSONUtils.deserialize[MetricsRequestBody](request));
		JSONUtils.deserialize[MetricsResponse](result);
	}
	
	private def getContentPopularityMetrics(request: String) : MetricsResponse = {
		val result = MetricsAPIService.contentPopularity(JSONUtils.deserialize[MetricsRequestBody](request));
		JSONUtils.deserialize[MetricsResponse](result);	
	}
	
	private def getItemUsageMetrics(request: String) : MetricsResponse = {
		val result = MetricsAPIService.itemUsage(JSONUtils.deserialize[MetricsRequestBody](request));
		JSONUtils.deserialize[MetricsResponse](result);
	}
	
	private def getGenieLaunchMetrics(request: String) : MetricsResponse = {
		val result = MetricsAPIService.genieLaunch(JSONUtils.deserialize[MetricsRequestBody](request));
		JSONUtils.deserialize[MetricsResponse](result);
	}
	
	private def getContentUsageListMetrics(request: String) : MetricsResponse = {
		val result = MetricsAPIService.contentList(JSONUtils.deserialize[MetricsRequestBody](request));
		JSONUtils.deserialize[MetricsResponse](result);
	}
	
	"ContentUsageMetricsAPIService" should "return empty result when, no pre-computed tag summary data is there in S3 location" in {
		val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentUsageMetrics(request);
//		response.result should be (Map());
	}
	
	it should "return one day metrics of last 7days when, only one day pre-computed tag summary data is there in S3 location for last 7days" in {
		val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentUsageMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}
	
	it should "return tag metrics when, last 7 days data present for tag1, tag2, tag3 in S3 & API inputs (tag: tag1)" in {
		val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentUsageMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}
	
	it should "return last 5 weeks metrics when, when 5 weeks data present" in {
		val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentUsageMetrics(request);
//		response.result.metrics.length should be (5);
//		response.result.summary should not be empty;
	}
	
	it should "return last 12 months metrics when, when 12 months data present" in {
		val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_12_MONTHS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentUsageMetrics(request);
//		response.result.metrics.length should be (12);
//		response.result.summary should not be empty;
	}
	
	"ContentPopularityMetricsAPIService" should "return empty result when, no pre-computed tag summary data is there in S3 location" in {
		val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentPopularityMetrics(request);
//		response.result should be (Map());
	}
	
	it should "return one day metrics of last 7days when, only one day pre-computed tag summary data is there in S3 location for last 7days" in {
		val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentPopularityMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}

	it should "return tag metrics when, last 7 days data present for tag1, tag2, tag3 in S3 & API inputs (tag: tag1)" in {
		val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentPopularityMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}
	
	it should "return last 5 weeks metrics when, when 5 weeks data present" in {
		val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentPopularityMetrics(request);
//		response.result.metrics.length should be (5);
//		response.result.summary should not be empty;
	}
	
	it should "return last 12 months metrics when, when 12 months data present" in {
		val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_12_MONTHS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getContentPopularityMetrics(request);
//		response.result.metrics.length should be (12);
//		response.result.summary should not be empty;
	}
	
	"ItemUsageMetricsAPIService" should "return empty result when, no pre-computed tag summary data is there in S3 location" in {
		val request = """{"id":"ekstep.analytics.metrics.item-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getItemUsageMetrics(request);
//		response.result should be (Map());
	}
	
	it should "return one day metrics of last 7days when, only one day pre-computed tag summary data is there in S3 location for last 7days" in {
		val request = """{"id":"ekstep.analytics.metrics.item-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getItemUsageMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}

	it should "return tag metrics when, last 7 days data present for tag1, tag2, tag3 in S3 & API inputs (tag: tag1)" in {
		val request = """{"id":"ekstep.analytics.metrics.item-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getItemUsageMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}
	
	it should "return last 5 weeks metrics when, when 5 weeks data present" in {
		val request = """{"id":"ekstep.analytics.metrics.item-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getItemUsageMetrics(request);
//		response.result.metrics.length should be (5);
//		response.result.summary should not be empty;
	}
	
	it should "return last 12 months metrics when, when 12 months data present" in {
		val request = """{"id":"ekstep.analytics.metrics.item-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_12_MONTHS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
		val response = getItemUsageMetrics(request);
//		response.result.metrics.length should be (12);
//		response.result.summary should not be empty;
	}
	
	"GenieLaunchMetricsAPIService" should "return empty result when, no pre-computed tag summary data is there in S3 location" in {
		val request = """{"id":"ekstep.analytics.metrics.genie-launch","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}}""";
		val response = getGenieLaunchMetrics(request);
//		response.result should be (Map());
	}
	
	it should "return one day metrics of last 7days when, only one day pre-computed tag summary data is there in S3 location for last 7days" in {
		val request = """{"id":"ekstep.analytics.metrics.genie-launch","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}}""";
		val response = getGenieLaunchMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}

	it should "return tag metrics when, last 7 days data present for tag1, tag2, tag3 in S3 & API inputs (tag: tag1)" in {
		val request = """{"id":"ekstep.analytics.metrics.genie-launch","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}}""";
		val response = getGenieLaunchMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}
	
	it should "return last 5 weeks metrics when, when 5 weeks data present" in {
		val request = """{"id":"ekstep.analytics.metrics.genie-launch","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}}""";
		val response = getGenieLaunchMetrics(request);
//		response.result.metrics.length should be (5);
//		response.result.summary should not be empty;
	}
	
	it should "return last 12 months metrics when, when 12 months data present" in {
		val request = """{"id":"ekstep.analytics.metrics.genie-launch","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_12_MONTHS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}}""";
		val response = getGenieLaunchMetrics(request);
//		response.result.metrics.length should be (12);
//		response.result.summary should not be empty;
	}
	
	"ContentUsageListMetricsAPIService" should "return empty result when, no pre-computed tag summary data is there in S3 location" in {
		val request = """{"id":"ekstep.analytics.content-list","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}""";
		val response = getContentUsageListMetrics(request);
//		response.result should be (Map());
	}
	
	it should "return one day metrics of last 7days when, only one day pre-computed tag summary data is there in S3 location for last 7days" in {
		val request = """{"id":"ekstep.analytics.content-list","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}""";
		val response = getContentUsageListMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}

	it should "return tag metrics when, last 7 days data present for tag1, tag2, tag3 in S3 & API inputs (tag: tag1)" in {
		val request = """{"id":"ekstep.analytics.content-list","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}""";
		val response = getContentUsageListMetrics(request);
//		response.result.metrics.length should be (7);
//		response.result.summary should not be empty;
	}
	
	it should "return last 5 weeks metrics when, when 5 weeks data present" in {
		val request = """{"id":"ekstep.analytics.content-list","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}""";
		val response = getContentUsageListMetrics(request);
//		response.result.metrics.length should be (5);
//		response.result.summary should not be empty;
	}
	
	it should "return last 12 months metrics when, when 12 months data present" in {
		val request = """{"id":"ekstep.analytics.content-list","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341"}}""";
		val response = getContentUsageListMetrics(request);
//		response.result.metrics.length should be (12);
//		response.result.summary should not be empty;
	}
	
}