package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.MetricsRequestBody
import org.ekstep.analytics.api.MetricsResponse
import org.ekstep.analytics.api.Result
import org.ekstep.analytics.api.SparkSpec
import org.ekstep.analytics.api.util.ContentCacheUtil
import org.ekstep.analytics.api.util.JSONUtils
import org.joda.time.DateTimeUtils
import com.typesafe.config.ConfigFactory

class TestTagAggregation extends SparkSpec {

    implicit val config = ConfigFactory.load();

    private def getContentUsageMetrics(request: String): MetricsResponse = {
        val result = MetricsAPIService.contentUsage(JSONUtils.deserialize[MetricsRequestBody](request))
        JSONUtils.deserialize[MetricsResponse](result);
    }

    private def getGenieLaunchMetrics(request: String): MetricsResponse = {
        val result = MetricsAPIService.genieLaunch(JSONUtils.deserialize[MetricsRequestBody](request));
        JSONUtils.deserialize[MetricsResponse](result);
    }

    private def checkContentUsageMetrics(metric: Map[String, AnyRef]) {
        metric.get("m_total_ts") should be(Some(490.0));
        metric.get("m_total_sessions") should be(Some(17));
        metric.get("m_avg_ts_session") should be(Some(114.0));
        metric.get("m_total_interactions") should be(Some(297));
        metric.get("m_avg_interactions_min") should be(Some(29.0));
        metric.get("m_total_devices") should be(Some(27));
        metric.get("m_avg_sess_device") should be(Some(1.0));
    }

    private def checkGenieLaunchMetrics(metric: Map[String, AnyRef]) {
        metric.get("m_total_ts") should be(Some(0.0));
        metric.get("m_total_sessions") should be(Some(0));
        metric.get("m_avg_ts_session") should be(Some(0.0));

    }

    private def checkContentUsageSummary(metric: Map[String, AnyRef]) {
        metric.get("m_total_sessions") should be(Some(86));
        metric.get("m_avg_ts_session") should be(Some(40.58));
        metric.get("m_total_interactions") should be(Some(1674));
        metric.get("m_avg_interactions_min") should be(Some(28.78));
        metric.get("m_total_devices") should be(Some(96));

    }

    private def genieLaunchSummary(metric: Map[String, AnyRef]) {
        metric.get("m_total_sessions") should be(Some(84));
        metric.get("m_avg_ts_session") should be(Some(45.74));
        metric.get("m_total_devices") should be(Some(107));
    }

    "Content usage service" should "return 7 days data present for multiple tags" in {
        val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tags":["1375b1d70a66a0f2c22dd1096b98030cb7d9bacb"]}}}""";
        val response = getContentUsageMetrics(request);
        response.result.metrics.length should be(7);
        response.result.summary should not be empty;
        checkContentUsageMetrics(response.result.metrics(1));
        checkContentUsageSummary(response.result.summary)
    }

    it should "return 5 weeks data present for multiple tags" in {
        val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","filter":{"tags":["1375b1d70a66a0f2c22dd1096b98030cb7d9bacb"]}}}""";
        val response = getContentUsageMetrics(request);
        response.result.metrics.length should be(5);
        response.result.summary should not be empty;
    }

    it should "return last 12 months metrics when, 12 months data present for multiple tags" in {
        val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_12_MONTHS","filter":{"tags":["1375b1d70a66a0f2c22dd1096b98030cb7d9bacb","1375b1d70a66a0f2c22dd1096b98030cb7d9bacb","1375b1d70a66a0f2c22dd1096b98030cb7d9bacb","1375b1d70a66a0f2c22dd1096b98030cb7d9bacb"]}}}""";
        val response = getContentUsageMetrics(request);
        response.result.metrics.length should be(12);
        response.result.summary should not be empty;
    }

    "Genie Lanuch service" should "return 7 days data present for multiple tags" in {
        val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tags":["1375b1d70a66a0f2c22dd1096b98030cb7d9bacb","1375b1d70a66a0f2c22dd1096b98030cb7d9bacb"]}}}""";
        val response = getGenieLaunchMetrics(request);
        response.result.metrics.length should be(7);
        response.result.summary should not be empty;
        genieLaunchSummary(response.result.summary)
    }

    it should "return 5 weeks data present for multiple tags" in {
        val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","filter":{"tags":["1375b1d70a66a0f2c22dd1096b98030cb7d9bacb","1375b1d70a66a0f2c22dd1096b98030cb7d9bacb"]}}}""";
        val response = getGenieLaunchMetrics(request);
        response.result.metrics.length should be(5);
        response.result.summary should not be empty;
        checkGenieLaunchMetrics(response.result.metrics(0))
    }

    it should "return last 12 months metrics when, 12 months data present for multiple tags" in {
        val request = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_12_MONTHS","filter":{"tags":["1375b1d70a66a0f2c22dd1096b98030cb7d9bacb","1375b1d70a66a0f2c22dd1096b98030cb7d9bacb","1375b1d70a66a0f2c22dd1096b98030cb7d9bacb","1375b1d70a66a0f2c22dd1096b98030cb7d9bacb"]}}}""";
        val response = getGenieLaunchMetrics(request);
        response.result.metrics.length should be(12);
        response.result.summary should not be empty;
    }

}