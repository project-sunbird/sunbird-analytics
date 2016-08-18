package org.ekstep.analytics.api.service

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.api.SparkSpec
import org.ekstep.analytics.api.RecommendationContent
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.util.JSONUtils
import org.joda.time.DateTimeUtils
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util.concurrent.TimeUnit
import com.typesafe.config.ConfigFactory


class TestRecommendationAPIService extends SparkSpec {
	val config = ConfigFactory.load();
	override def beforeAll() {
        super.beforeAll()
        // Load test data
        val rdd = loadFile[RecommendationContent]("src/test/resources/device-recos/test_device_recos.log");
        
        rdd.saveToCassandra(Constants.DEVICE_DB, Constants.DEVICE_RECOS_TABLE);
    }
    
    override def afterAll() {
        // Cleanup test data
        CassandraConnector(sc.getConf).withSessionDo { session =>
            val query = "DELETE FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_RECOS_TABLE + " where device_id='5edf49c4-313c-4f57-fd52-9bfe35e3b7d6'"
            session.execute(query);
        }
        super.afterAll();
    }
    
    "RecommendationAPIService" should "return contents" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"English"}}} """;
    	val response = RecommendationAPIService.recommendations(request)(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
    }
    
    it should "return error response on invalid request" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{}}} """;
    	the[Exception] thrownBy {
            RecommendationAPIService.recommendations(request)(sc, config);
        } should have message "context required data is missing."
    }
    
    it should "retun only 3 contents" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"English"},"limit":3}} """;
    	val response = RecommendationAPIService.recommendations(request)(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
        content.size should be (3)
    }
    
    it should "return only 'Hindi' contents" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"Hindi"},"filters":{"language":"Hindi"},"limit":100}} """;
    	val response = RecommendationAPIService.recommendations(request)(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
        content.filter { x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("language", List()).asInstanceOf[List[String]].contains("Hindi") } should have size(content.length)
    }
    
    it should "return only 'Hindi Stories' content" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"Hindi"},"filters":{"language":"Hindi", "contentType": "Story"},"limit":100}} """;
    	val response = RecommendationAPIService.recommendations(request)(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
        content
			.filter({ x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("language", List()).asInstanceOf[List[String]].contains("Hindi") })
			.filter({ x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("contentType", "").asInstanceOf[String].contains("Story") }) should have size(content.length)
    }
    
    it should "return content based context filtered content" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"Hindi", "contentId" : "domain_63844"},"filters":{"language":"Hindi", "contentType": "Story"},"limit":100}} """;
    	val response = RecommendationAPIService.recommendations(request)(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
        content
			.filter({ x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("language", List()).asInstanceOf[List[String]].contains("Hindi") })
			.filter({ x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("contentType", "").asInstanceOf[String].contains("Story") }) should have size(content.length)
    }
    
    it should "update the cache" in {
    	val beforeTime = RecommendationAPIService.cacheTimestamp;
    	DateTimeUtils.setCurrentMillisFixed(DateTime.now(DateTimeZone.UTC).getMillis + TimeUnit.DAYS.toMillis(1)); // Fix the date-time to be returned by DateTime.now()
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"English"}}} """;
    	val response = RecommendationAPIService.recommendations(request)(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
        val afterTime = RecommendationAPIService.cacheTimestamp;
    	beforeTime should not equals(afterTime)
    }
    
}