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
import org.ekstep.analytics.api.RequestBody
import org.joda.time.DateTimeZone
import org.ekstep.analytics.api.RecommendationContent
import org.ekstep.analytics.api.recommend.CreationRecommendations
import org.ekstep.analytics.api.RequestRecommendations
import org.apache.spark.sql.catalyst.expressions.LessThan

/**
 * @author mahesh
 */

class TestRecommendationAPIService extends SparkSpec {

	override def beforeAll() {
        super.beforeAll()
        // Load test data
        CassandraConnector(sc.getConf).withSessionDo { session =>
            val query = "DELETE FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_RECOS_TABLE + " where device_id='5edf49c4-313c-4f57-fd52-9bfe35e3b7d6'"
            val query1 = "DELETE FROM " + Constants.PLATFORML_DB + "." + Constants.REQUEST_RECOS_TABLE + " where uid='5edf49c4-313c-4f57-fd52-9bfe35e3b7d6'"
            session.execute(query);
            session.execute(query1)
        }
        val rdd = loadFile[RecommendationContent]("src/test/resources/device-recos/test_device_recos.log");
        val rdd1 = loadFile[RequestRecommendations]("src/test/resources/device-recos/test_request_recos.log");
        rdd1.saveToCassandra(Constants.PLATFORML_DB, Constants.REQUEST_RECOS_TABLE)
        rdd.saveToCassandra(Constants.DEVICE_DB, Constants.DEVICE_RECOS_TABLE);
    }

    override def afterAll() {
        // Cleanup test data
        CassandraConnector(sc.getConf).withSessionDo { session =>
            val query = "DELETE FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_RECOS_TABLE + " where device_id='5edf49c4-313c-4f57-fd52-9bfe35e3b7d6'"
            val query1 = "DELETE FROM " + Constants.PLATFORML_DB + "." + Constants.REQUEST_RECOS_TABLE + " where uid='5edf49c4-313c-4f57-fd52-9bfe35e3b7d6'"
            session.execute(query);
            session.execute(query1)
        }
        super.afterAll();
    }
    
    "RecommendationAPIService" should "return device specific contents" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"en"}}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
    }
    
    "RecommendationAPIService" should "return content specific contents" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"en", "contentid":"TestContent"}}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should be (empty) 
    }
    
    it should "return error response on invalid request when request is empty" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{}}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("failed");
        resp.params.errmsg should be ("context required data is missing.");
    }
    
    it should "return error response on invalid request when request context doesn't have did for content recommendation" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"dlang": "en","contentid": "TestId1"}}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("failed");
        resp.params.errmsg should be ("context required data is missing.");
    }
    
    it should "return empty response on a request when request context contentid doesnot have recommended content" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang": "en","contentid": "dot_123016"}}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
      resp.id should be ("ekstep.analytics.recommendations");
      resp.params.status should be ("successful");
      val result = resp.result.get;
      val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
      content should be (empty)
    }
    
    it should "retun only 3 contents for device recommendation" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"en"},"limit":3}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
      resp.id should be ("ekstep.analytics.recommendations");
      resp.params.status should be ("successful");
      val result = resp.result.get;
      val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
      content should not be empty
      if (config.getBoolean("recommendation.enable")) {
      	content.size should be (3)	
      } else {
      	content.size should be (config.getInt("service.search.limit"))
      }
    }
    
    it should "return only 'Hindi' contents" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"hi"},"filters":{"language":"Hindi"},"limit":100}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
        content.filter { x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("language", List()).asInstanceOf[List[String]].contains("Hindi") } should have size(content.length)
    }
    
    it should "return only 'Hindi Stories' content" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"hi"},"filters":{"language":"Hindi", "contentType": "Story"},"limit":100}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
        content
			.filter({ x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("language", List()).asInstanceOf[List[String]].contains("Hindi") })        
    }
    
    it should "return content based context filtered content" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"hi"},"filters":{"language":"Hindi", "contentType": "Story"},"limit":100}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
        content
			.filter({ x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("language", List()).asInstanceOf[List[String]].contains("Hindi") })
    }
    
    it should "update the cache" in {
//    	val beforeTime = RecommendationAPIService.cacheTimestamp;
    	DateTimeUtils.setCurrentMillisFixed(DateTime.now(DateTimeZone.UTC).getMillis + TimeUnit.DAYS.toMillis(1)); // Fix the date-time to be returned by DateTime.now()
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"en"}}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.recommendations");
        resp.params.status should be ("successful");
        val result = resp.result.get;
        val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
        content should not be empty 
    }
    
    it should "return response on a request when request limit is more than the total recommended content" in {
    	val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang": "en","contentid": "dot_123016"}, "limit":30}} """;
    	val response = RecommendationAPIService.consumptionRecommendations(JSONUtils.deserialize[RequestBody](request))(sc, config);
    	val resp = JSONUtils.deserialize[Response](response);
      resp.id should be ("ekstep.analytics.recommendations");
      resp.params.status should be ("successful");
      val result = resp.result.get;
      val content = result.get("content").get.asInstanceOf[List[Map[String, AnyRef]]];
      content should be (empty)
    }
    
    it should "return recommendations when request having valid uid" in {
        val request = """ {"id":"ekstep.analytics.creation.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"uid": "5edf49c4-313c-4f57-fd52-9bfe35e3b7d6"},"filters": { },"limit": 10}} """;
        val requests = getResult(request)
        requests.length should be (2)
    }

    it should "return error when request not having uid" in {
        val request = """ {"id":"ekstep.analytics.creation.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{}}} """;
        val response = CreationRecommendations.fetch(JSONUtils.deserialize[RequestBody](request))(sc, config);
        val resp = JSONUtils.deserialize[Response](response);
        resp.params.status should be("failed");
        resp.params.errmsg should be("context required data is missing.");
    }
    
    it should "return empty recommendations when uid not there in database" in {
        val request = """ {"id":"ekstep.analytics.creation.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"uid": "5edf49c4-313c-4f57-fd52-9bfe35e3b7d7"},"filters": { },"limit": 10}} """;
        val requests = getResult(request)
        requests should be (empty)
    }
    
    it should "return recommendations when request body having limit more than actual results" in {
        val request = """ {"id":"ekstep.analytics.creation.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"uid": "5edf49c4-313c-4f57-fd52-9bfe35e3b7d6"},"filters": { },"limit": 100}} """;
        val requests = getResult(request)
        requests.length should be < (100)
    }

    it should "return recommendations when request body having limit less than actual results" in {
        val request = """ {"id":"ekstep.analytics.creation.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"uid": "5edf49c4-313c-4f57-fd52-9bfe35e3b7d0"},"filters": { },"limit": 3}} """;
        val requests = getResult(request)
        requests(0).get("contentType").get should be("Game")
        requests(0).get("type").get should be("Content")
        requests(0).get("gradeLevel").get should be(List("Grade 2"))
        requests.length should be (3)
    }

    it should "return default recommendations when request body not provided with limit" in {
        val request = """ {"id":"ekstep.analytics.creation.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"uid": "5edf49c4-313c-4f57-fd52-9bfe35e3b7d0"},"filters": { }}} """;
        val requests = getResult(request)
        requests.length should be(10)
    }

    private def getResult(request: String): List[Map[String, AnyRef]] = {
        val response = CreationRecommendations.fetch(JSONUtils.deserialize[RequestBody](request))(sc, config);
        val resp = JSONUtils.deserialize[Response](response);
        val result = resp.result.get;
        result.get("requests").get.asInstanceOf[List[Map[String, AnyRef]]];
    }

}