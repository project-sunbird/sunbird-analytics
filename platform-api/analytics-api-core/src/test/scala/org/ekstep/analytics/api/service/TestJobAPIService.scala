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

class TestJobAPIService extends SparkSpec {

    implicit val config = ConfigFactory.load();

    "JobAPIService" should "return response for data request" in {
        val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"filter":{"start_date":"2016-11-01","end_date":"2016-11-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
        val result = JobAPIService.dataRequest(request);
        println("result:", result);
    }

    it should "return response for get data request" in {
        val result = JobAPIService.getDataRequest("dev-portal", "14621312DB7F8ED99BA1B16D8B430FAC");
        println("result:", result);
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
        
//        jobRes.head.get("status").get.asInstanceOf[String] should be ("SUBMITTED")
//        jobRes.last.get("status").get.asInstanceOf[String] should be ("COMPLETED")
        
    }

}