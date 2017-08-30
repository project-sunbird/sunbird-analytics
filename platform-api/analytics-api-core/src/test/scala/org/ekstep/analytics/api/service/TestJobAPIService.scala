package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.SparkSpec
import com.typesafe.config.ConfigFactory
import org.ekstep.analytics.api.JobRequest
import org.joda.time.DateTime
import org.ekstep.analytics.api.util.CommonUtil
import com.datastax.spark.connector._
import org.ekstep.analytics.api.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.Response 
import org.ekstep.analytics.api.JobResponse
import org.apache.commons.lang3.StringUtils

class TestJobAPIService extends SparkSpec {

    "JobAPIService" should "return response for data request" in {
        val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "json", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
        val result = JobAPIService.dataRequest(request);
    }
    
    "JobAPIService" should "return success response for data request with type as json without dataset_id, app_id & channel" in {
        val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "json", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
        val result = JobAPIService.dataRequest(request);
        val response = JSONUtils.deserialize[Response](result)
        response.params.status should be("successful")

    }
    
    "JobAPIService" should "return success response for data request with dataset_id, app_id & channel" in {
        val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "json", "dataset_id": "eks-consumption-raw", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"], "app_id": "Ekstep", "channel": "KAR"}}}""";
        val result = JobAPIService.dataRequest(request);
        val response = JSONUtils.deserialize[Response](result)
        response.params.status should be("successful")

    }
    
    "JobAPIService" should "return success response for data request with type as csv and events size is one" in {
        val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "csv", "filter":{"events":["OE_ASSESS"], "start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
        val result = JobAPIService.dataRequest(request);
        val response = JSONUtils.deserialize[Response](result)
        response.params.status should be("successful")

    }
    
    "JobAPIService" should "return failed response for data request with type as csv and events size is not equals to one" in {
        val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "csv", "filter":{"events":["OE_ASSESS", "OE_START"], "start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
        val result = JobAPIService.dataRequest(request);
        val response = JSONUtils.deserialize[Response](result)
        response.params.status should be("failed")

    }
    
    "JobAPIService" should "return response for data request without type attribute" in {
        val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"filter":{"events":["OE_ASSESS"], "start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
        val result = JobAPIService.dataRequest(request);
        val response = JSONUtils.deserialize[Response](result)
        response.params.status should be("successful")
    }
    
    "JobAPIService" should "return response for data request with type as csv and events is not defined" in {
        val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "csv", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
        val result = JobAPIService.dataRequest(request);
        val response = JSONUtils.deserialize[Response](result)
        response.params.status should be("failed")
    }
    
    "JobAPIService" should "submit the failed request for retry" in {
    	val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"filter":{"events":["OE_ASSESS"], "start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
        val result = JobAPIService.dataRequest(request);
        val response = JSONUtils.deserialize[Response](result)
        val requestId = response.result.getOrElse(Map()).getOrElse("request_id", "").asInstanceOf[String];
        StringUtils.isNotEmpty(requestId) should be(true);
        CassandraConnector(sc.getConf).withSessionDo { session =>
        	val query = "UPDATE platform_db.job_request SET status='FAILED' WHERE client_key='dev-portal' AND request_id='"+requestId+"'"
            session.execute(query);
        }
        val getResult = JobAPIService.getDataRequest("dev-portal", requestId);
        val getResponse = JSONUtils.deserialize[Response](getResult);
        val failStatus = getResponse.result.getOrElse(Map()).getOrElse("status", "").asInstanceOf[String];
        StringUtils.isNotEmpty(failStatus) should be(true);
        failStatus should be("FAILED");
        val result2 = JobAPIService.dataRequest(request);
        val response2 = JSONUtils.deserialize[Response](result2);
        val status = response.result.getOrElse(Map()).getOrElse("status", "").asInstanceOf[String];
        status should be("SUBMITTED");
    }
    
    "JobAPIService" should "not submit the permanently failed/max attempts reached request while doing retry" in {
    	
    }
    
    it should "return response for get data request" in {
        val result = JobAPIService.getDataRequest("dev-portal", "14621312DB7F8ED99BA1B16D8B430FAC");
    }

    it should "return the list of jobs in descending order" in {
        
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM platform_db.job_request WHERE client_key='partner1'");
        }
        
        val request_data1 = """{"filter":{"start_date":"2016-11-19","end_date":"2016-11-20","tags":["becb887fe82f24c644482eb30041da6d88bd8150"]}}"""
        val request_data2 = """{"filter":{"start_date":"2016-11-19","end_date":"2016-11-20","tags":["test-tag"],"events":["OE_ASSESS"]}}"""

        val requests = Array(
            JobRequest(Option("partner1"), Option("1234"), None, Option("SUBMITTED"), Option(request_data1),
                Option(1), Option(DateTime.now()), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
            JobRequest(Option("partner1"), Option("273645"), Option("test-job-id"), Option("COMPLETED"), Option(request_data2),
                Option(1), Option(DateTime.parse("2017-01-08", CommonUtil.dateFormat)), Option("https://test-location"), Option(DateTime.parse("2017-01-08", CommonUtil.dateFormat)), None, None, None, None, None, Option(123234), Option(532), Option(12343453L), None, None, None, None, None));
        sc.makeRDD(requests).saveToCassandra(Constants.PLATFORML_DB, Constants.JOB_REQUEST)
        
        val jobs = JobAPIService.getDataRequestList("partner1", 10)
        val res = JSONUtils.deserialize[Response](jobs)
        val resultMap = res.result.get.asInstanceOf[Map[String, AnyRef]]
        val jobRes = resultMap.get("jobs").get.asInstanceOf[List[Map[String, AnyRef]]]
        jobRes.length should be (2)
        
//        jobRes.head.get("status").get.asInstanceOf[String] should be ("SUBMITTED")
//        jobRes.last.get("status").get.asInstanceOf[String] should be ("COMPLETED")
        
        // fetch data with limit less than the number of record available  
        val jobs2 = JobAPIService.getDataRequestList("partner1", 1)
        val res2 = JSONUtils.deserialize[Response](jobs2)
        val resultMap2 = res2.result.get.asInstanceOf[Map[String, AnyRef]]
        val jobRes2 = resultMap2.get("jobs").get.asInstanceOf[List[Map[String, AnyRef]]]
        jobRes2.length should be (1)
        
        // trying to fetch the record with a key for which data is not available
        val jobs1 = JobAPIService.getDataRequestList("testKey", 10)
        val res1 = JSONUtils.deserialize[Response](jobs1)
        val resultMap1 = res1.result.get.asInstanceOf[Map[String, AnyRef]]
        resultMap1.get("count").get.asInstanceOf[Int] should be (0)
    }
    
    "JobAPIService" should "return different request id for same data having different client keys" in {
        val request1 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"output_format": "json", "dataset_id": "eks-consumption-raw", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"], "app_id": "Ekstep", "channel": "KAR"}}}""";
        val result1 = JobAPIService.dataRequest(request1);
        val response1 = JSONUtils.deserialize[Response](result1)
        val request2 = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-test"},"request":{"output_format": "json", "dataset_id": "eks-consumption-raw", "filter":{"start_date":"2016-09-01","end_date":"2016-09-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"], "app_id": "Ekstep", "channel": "KAR"}}}""";
        val result2 = JobAPIService.dataRequest(request2);
        val response2 = JSONUtils.deserialize[Response](result2)
        response2.result.head.get("request_id").get should not be (response1.result.head.get("request_id").get)

    }

    it should "test channel based raw-telemetry exhaust data" in {
        val res = JobAPIService.getChannelData("secor-upgrade/backend", "2017-08-27", "2017-08-29")
        println(res)
    }
}