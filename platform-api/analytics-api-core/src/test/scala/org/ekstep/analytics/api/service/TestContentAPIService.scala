package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.SparkSpec
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.Response
import org.joda.time.DateTimeUtils
import org.ekstep.analytics.api.ContentUsageSummaryFact
import com.datastax.spark.connector._
import org.ekstep.analytics.api.Constants
import com.datastax.spark.connector.cql.CassandraConnector

class TestContentAPIService extends SparkSpec {
    
    override def beforeAll() {
        super.beforeAll()
        DateTimeUtils.setCurrentMillisFixed(1464859204280L); // Fix the date-time to be returned by DateTime.now()
        // Load test data
        val rdd = loadFile[ContentUsageSummaryFact]("src/test/resources/content-summaries/test_content_summaries.log");
        rdd.saveToCassandra(Constants.CONTENT_DB, Constants.CONTENT_SUMMARY_FACT_TABLE);
    }
    
    override def afterAll() {
        // Cleanup test data
        CassandraConnector(sc.getConf).withSessionDo { session =>
            val query = "DELETE FROM " + Constants.CONTENT_DB + "." + Constants.CONTENT_SUMMARY_FACT_TABLE + " where d_content_id='org.ekstep.test123'"
            session.execute(query);
        }
        super.afterAll();
    }
    
    "ContentAPIService" should "return summaries for the default request - 7 daily, 5 weekly and 12 monthly" in {
        
        val request = """ {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn", "request": {"filter":{}} } """
        val response = ContentAPIService.getContentUsageMetrics("org.ekstep.test123", request);
        val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.contentusagesummary");
        resp.params.status should be ("successful");
        resp.ts should be ("2016-06-02T09:20:04.280+00:00");
        
        val result = resp.result.get;
        
        val trends = result.get("trend").get.asInstanceOf[Map[String, List[Map[String, AnyRef]]]];
        trends.get("day").get.size should be (3);
        trends.get("week").get.size should be (5);
        trends.get("month").get.size should be (6);
        
        val dailyTrendMap = trends.get("day").get.map(f => (f.get("period").get.asInstanceOf[Int], f)).toMap;
        dailyTrendMap.get(20160601) should be (None);
        dailyTrendMap.get(20160530) should be (None);
        
        dailyTrendMap.get(20160531).get.get("avg_ts_session").get should be (2.4);
        dailyTrendMap.get(20160531).get.get("total_sessions").get should be (5);
        dailyTrendMap.get(20160531).get.get("avg_interactions_min").get should be (20);
        dailyTrendMap.get(20160531).get.get("total_interactions").get should be (0);
        dailyTrendMap.get(20160531).get.get("total_ts").get should be (12);
        
        val weeklyTrendMap = trends.get("week").get.map(f => (f.get("period").get.asInstanceOf[Int], f)).toMap;
        weeklyTrendMap.get(2016717) should be (None);
        
        weeklyTrendMap.get(2016719).get.get("avg_ts_session").get should be (280.1333333333333);
        weeklyTrendMap.get(2016719).get.get("total_sessions").get should be (30);
        weeklyTrendMap.get(2016719).get.get("avg_interactions_min").get should be (22.01);
        weeklyTrendMap.get(2016719).get.get("total_interactions").get should be (3083);
        weeklyTrendMap.get(2016719).get.get("total_ts").get should be (8404);
        
        val monthlyTrendMap = trends.get("month").get.map(f => (f.get("period").get.asInstanceOf[Int], f)).toMap;
        monthlyTrendMap.get(201512) should be (None);
        
        monthlyTrendMap.get(201605).get.get("avg_ts_week").get should be (3307.6);
        monthlyTrendMap.get(201605).get.get("avg_ts_session").get should be (258.40625);
        monthlyTrendMap.get(201605).get.get("total_sessions").get should be (64);
        monthlyTrendMap.get(201605).get.get("avg_interactions_min").get should be (21.33);
        monthlyTrendMap.get(201605).get.get("total_interactions").get should be (5879);
        monthlyTrendMap.get(201605).get.get("total_ts").get should be (16538);
        monthlyTrendMap.get(201605).get.get("avg_sessions_week").get should be (12.8);
        
        val summaries = result.get("summaries").get.asInstanceOf[Map[String, Map[String, AnyRef]]];
        
        summaries.get("day").get.get("avg_ts_session").get should be (179.30555555555554);
        summaries.get("day").get.get("total_sessions").get should be (36);
        summaries.get("day").get.get("avg_interactions_min").get should be (1.21);
        summaries.get("day").get.get("total_interactions").get should be (130);
        summaries.get("day").get.get("total_ts").get should be (6455);
        
        summaries.get("week").get.get("avg_ts_session").get should be (247.69230769230768);
        summaries.get("week").get.get("total_sessions").get should be (91);
        summaries.get("week").get.get("avg_interactions_min").get should be (20.53);
        summaries.get("week").get.get("total_interactions").get should be (7714);
        summaries.get("week").get.get("total_ts").get should be (22540);
        
        summaries.get("month").get.get("avg_ts_week").get should be (2464);
        summaries.get("month").get.get("avg_ts_session").get should be (150.41860465116278);
        summaries.get("month").get.get("total_sessions").get should be (344);
        summaries.get("month").get.get("avg_interactions_min").get should be (21.21);
        summaries.get("month").get.get("total_interactions").get should be (18295);
        summaries.get("month").get.get("total_ts").get should be (51744);
        summaries.get("month").get.get("avg_sessions_week").get should be (16.38095238095238);
        
        summaries.get("cumulative").get.get("avg_ts_week").get should be (2464);
        summaries.get("cumulative").get.get("avg_ts_session").get should be (150.41860465116278);
        summaries.get("cumulative").get.get("total_sessions").get should be (344);
        summaries.get("cumulative").get.get("avg_interactions_min").get should be (21.21);
        summaries.get("cumulative").get.get("total_interactions").get should be (18295);
        summaries.get("cumulative").get.get("total_ts").get should be (51744);
        summaries.get("cumulative").get.get("avg_sessions_week").get should be (16.38095238095238);
    }
    
    it should "return error response on invalid request" in {
        val request = """ {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn"} """
        the[Exception] thrownBy {
            ContentAPIService.getContentUsageMetrics("org.ekstep.test123", request);
        } should have message "Request cannot be blank"
    }
    
    it should "return summaries for last 3 months and cumulative" in {
        val request = """ {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn", "request": {"trend":{"month":3}} } """
        val response = ContentAPIService.getContentUsageMetrics("org.ekstep.test123", request);
        val result = JSONUtils.deserialize[Response](response).result.get;
        
        val trends = result.get("trend").get.asInstanceOf[Map[String, List[Map[String, AnyRef]]]];
        trends.get("day").get.size should be (0);
        trends.get("week").get.size should be (0);
        trends.get("month").get.size should be (3);
        
        val monthlyTrendMap = trends.get("month").get.map(f => (f.get("period").get.asInstanceOf[Int], f)).toMap;
        monthlyTrendMap.get(201603) should be (None);
        
        monthlyTrendMap.get(201605).get.get("avg_ts_week").get should be (3307.6);
        monthlyTrendMap.get(201605).get.get("avg_ts_session").get should be (258.40625);
        monthlyTrendMap.get(201605).get.get("total_sessions").get should be (64);
        monthlyTrendMap.get(201605).get.get("avg_interactions_min").get should be (21.33);
        monthlyTrendMap.get(201605).get.get("total_interactions").get should be (5879);
        monthlyTrendMap.get(201605).get.get("total_ts").get should be (16538);
        monthlyTrendMap.get(201605).get.get("avg_sessions_week").get should be (12.8);
        
        val summaries = result.get("summaries").get.asInstanceOf[Map[String, Map[String, AnyRef]]];
        summaries.get("day") should be (None);
        summaries.get("week") should be (None);
        
        summaries.get("month").get.get("avg_ts_week").get should be (2829.375);
        summaries.get("month").get.get("avg_ts_session").get should be (209.58333333333334);
        summaries.get("month").get.get("total_sessions").get should be (108);
        summaries.get("month").get.get("avg_interactions_min").get should be (20.49);
        summaries.get("month").get.get("total_interactions").get should be (7731);
        summaries.get("month").get.get("total_ts").get should be (22635);
        summaries.get("month").get.get("avg_sessions_week").get should be (13.5);
        
        summaries.get("cumulative").get.get("avg_ts_week").get should be (2464);
        summaries.get("cumulative").get.get("avg_ts_session").get should be (150.41860465116278);
        summaries.get("cumulative").get.get("total_sessions").get should be (344);
        summaries.get("cumulative").get.get("avg_interactions_min").get should be (21.21);
        summaries.get("cumulative").get.get("total_interactions").get should be (18295);
        summaries.get("cumulative").get.get("total_ts").get should be (51744);
        summaries.get("cumulative").get.get("avg_sessions_week").get should be (16.38095238095238);
    }
    
    it should "return last 1 year summmaries aggregated weekly" in {
        val request = """ {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn", "request": {"trend":{"week":53}} } """
        val response = ContentAPIService.getContentUsageMetrics("org.ekstep.test123", request);
        val result = JSONUtils.deserialize[Response](response).result.get;
        val trends = result.get("trend").get.asInstanceOf[Map[String, List[Map[String, AnyRef]]]];
        trends.get("day").get.size should be (0);
        trends.get("week").get.size should be (21);
        trends.get("month").get.size should be (0);
        
        val weeklyTrendMap = trends.get("week").get.map(f => (f.get("period").get.asInstanceOf[Int], f)).toMap;
        weeklyTrendMap.get(2015754) should be (None);
        weeklyTrendMap.get(2016706) should be (None);
        
        weeklyTrendMap.get(2016707).get.get("avg_ts_session").get should be (20.7);
        weeklyTrendMap.get(2016707).get.get("total_sessions").get should be (10);
        weeklyTrendMap.get(2016707).get.get("avg_interactions_min").get should be (16.81);
        weeklyTrendMap.get(2016707).get.get("total_interactions").get should be (58);
        weeklyTrendMap.get(2016707).get.get("total_ts").get should be (207);
        
        val summaries = result.get("summaries").get.asInstanceOf[Map[String, Map[String, AnyRef]]];
        summaries.get("day") should be (None);
        summaries.get("month") should be (None);
        summaries.get("week").get.get("avg_ts_session").get should be (150.41860465116278);
        summaries.get("week").get.get("total_sessions").get should be (344);
        summaries.get("week").get.get("avg_interactions_min").get should be (21.21);
        summaries.get("week").get.get("total_interactions").get should be (18295);
        summaries.get("week").get.get("total_ts").get should be (51744);
        
        summaries.get("cumulative").get.get("avg_ts_week").get should be (2464);
        summaries.get("cumulative").get.get("avg_ts_session").get should be (150.41860465116278);
        summaries.get("cumulative").get.get("total_sessions").get should be (344);
        summaries.get("cumulative").get.get("avg_interactions_min").get should be (21.21);
        summaries.get("cumulative").get.get("total_interactions").get should be (18295);
        summaries.get("cumulative").get.get("total_ts").get should be (51744);
        summaries.get("cumulative").get.get("avg_sessions_week").get should be (16.38095238095238);
    }
    
    it should "fetch data for the past 7 days filtered by group user" in {
        val request = """ {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn", "request": {"filter":{"group_user":true},"trend":{"day":7},"summaries":["day"]} } """
        val response = ContentAPIService.getContentUsageMetrics("org.ekstep.test123", request);
        val result = JSONUtils.deserialize[Response](response).result.get;
        val trends = result.get("trend").get.asInstanceOf[Map[String, List[Map[String, AnyRef]]]];
        trends.get("day").get.size should be (1);
        trends.get("week").get.size should be (0);
        trends.get("month").get.size should be (0);
        
        val dailyTrendMap = trends.get("day").get.map(f => (f.get("period").get.asInstanceOf[Int], f)).toMap;
        dailyTrendMap.get(20160601) should be (None);
        dailyTrendMap.get(20160531) should be (None);
        
        dailyTrendMap.get(20160602).get.get("avg_ts_session").get should be (55);
        dailyTrendMap.get(20160602).get.get("total_sessions").get should be (2);
        dailyTrendMap.get(20160602).get.get("avg_interactions_min").get should be (5.45);
        dailyTrendMap.get(20160602).get.get("total_interactions").get should be (10);
        dailyTrendMap.get(20160602).get.get("total_ts").get should be (110);
        
        val summaries = result.get("summaries").get.asInstanceOf[Map[String, Map[String, AnyRef]]];
        summaries.get("week") should be (None);
        summaries.get("month") should be (None);
        summaries.get("cumulative") should be (None);
        summaries.get("day").get.get("avg_ts_session").get should be (55);
        summaries.get("day").get.get("total_sessions").get should be (2);
        summaries.get("day").get.get("avg_interactions_min").get should be (5.45);
        summaries.get("day").get.get("total_interactions").get should be (10);
        summaries.get("day").get.get("total_ts").get should be (110);
    }
    
    it should "fetch data for the past 7 days filtered by individual user" in {
        
        val request = """ {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn", "request": {"filter":{"group_user":false},"trend":{"day":7},"summaries":["cumulative"]} } """
        val response = ContentAPIService.getContentUsageMetrics("org.ekstep.test123", request);
        val result = JSONUtils.deserialize[Response](response).result.get;
        val trends = result.get("trend").get.asInstanceOf[Map[String, List[Map[String, AnyRef]]]];
        trends.get("day").get.size should be (3);
        trends.get("week").get.size should be (0);
        trends.get("month").get.size should be (0);
        
        val dailyTrendMap = trends.get("day").get.map(f => (f.get("period").get.asInstanceOf[Int], f)).toMap;
        dailyTrendMap.get(20160601) should be (None);
        dailyTrendMap.get(20160530) should be (None);
        
        dailyTrendMap.get(20160531).get.get("avg_ts_session").get should be (2.4);
        dailyTrendMap.get(20160531).get.get("total_sessions").get should be (5);
        dailyTrendMap.get(20160531).get.get("avg_interactions_min").get should be (20);
        dailyTrendMap.get(20160531).get.get("total_interactions").get should be (0);
        dailyTrendMap.get(20160531).get.get("total_ts").get should be (12);
        
        val summaries = result.get("summaries").get.asInstanceOf[Map[String, Map[String, AnyRef]]];
        summaries.get("day") should be (None);
        summaries.get("month") should be (None);
        summaries.get("week") should be (None);
        
        summaries.get("cumulative").get.get("avg_ts_week").get should be (2458.7619047619046);
        summaries.get("cumulative").get.get("avg_ts_session").get should be (151.41935483870967);
        summaries.get("cumulative").get.get("total_sessions").get should be (341);
        summaries.get("cumulative").get.get("avg_interactions_min").get should be (21.25);
        summaries.get("cumulative").get.get("total_interactions").get should be (18285);
        summaries.get("cumulative").get.get("total_ts").get should be (51634);
        summaries.get("cumulative").get.get("avg_sessions_week").get should be (16.238095238095237);
    }

    
    it should "return last 1 year summmaries aggregated monthly" in {
        
        val request = """ {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn", "request": {"trend":{"month":12},"summaries":["month","cumulative"]} } """
        val response = ContentAPIService.getContentUsageMetrics("org.ekstep.test123", request);
        val result = JSONUtils.deserialize[Response](response).result.get;
        val trends = result.get("trend").get.asInstanceOf[Map[String, List[Map[String, AnyRef]]]];
        trends.get("day").get.size should be (0);
        trends.get("week").get.size should be (0);
        trends.get("month").get.size should be (6);
        
        val monthlyTrendMap = trends.get("month").get.map(f => (f.get("period").get.asInstanceOf[Int], f)).toMap;
        monthlyTrendMap.get(201512) should be (None);
        
        monthlyTrendMap.get(201605).get.get("avg_ts_week").get should be (3307.6);
        monthlyTrendMap.get(201605).get.get("avg_ts_session").get should be (258.40625);
        monthlyTrendMap.get(201605).get.get("total_sessions").get should be (64);
        monthlyTrendMap.get(201605).get.get("avg_interactions_min").get should be (21.33);
        monthlyTrendMap.get(201605).get.get("total_interactions").get should be (5879);
        monthlyTrendMap.get(201605).get.get("total_ts").get should be (16538);
        monthlyTrendMap.get(201605).get.get("avg_sessions_week").get should be (12.8);
        
        val summaries = result.get("summaries").get.asInstanceOf[Map[String, Map[String, AnyRef]]];
        summaries.get("day") should be (None);
        summaries.get("week") should be (None);
        
        summaries.get("month").get.get("avg_ts_week").get should be (2464);
        summaries.get("month").get.get("avg_ts_session").get should be (150.41860465116278);
        summaries.get("month").get.get("total_sessions").get should be (344);
        summaries.get("month").get.get("avg_interactions_min").get should be (21.21);
        summaries.get("month").get.get("total_interactions").get should be (18295);
        summaries.get("month").get.get("total_ts").get should be (51744);
        summaries.get("month").get.get("avg_sessions_week").get should be (16.38095238095238);
        
        summaries.get("cumulative").get.get("avg_ts_week").get should be (2464);
        summaries.get("cumulative").get.get("avg_ts_session").get should be (150.41860465116278);
        summaries.get("cumulative").get.get("total_sessions").get should be (344);
        summaries.get("cumulative").get.get("avg_interactions_min").get should be (21.21);
        summaries.get("cumulative").get.get("total_interactions").get should be (18295);
        summaries.get("cumulative").get.get("total_ts").get should be (51744);
        summaries.get("cumulative").get.get("avg_sessions_week").get should be (16.38095238095238);
    }
  
}