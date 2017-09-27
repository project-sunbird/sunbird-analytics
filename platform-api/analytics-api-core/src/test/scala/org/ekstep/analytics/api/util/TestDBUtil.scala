package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.SparkSpec
import org.ekstep.analytics.api.JobRequest
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.api.Constants
import com.datastax.spark.connector.cql.CassandraConnector

class TestDBUtil extends SparkSpec {
    
    it should "fetch list of jobs in a descending order" in {
        
        val connector = CassandraConnector(sc.getConf);
        val session = connector.openSession();
        session.execute("DELETE FROM local_platform_db.job_request WHERE client_key='partner1'");
        
        val request_data1 = """{"filter":{"start_date":"2016-11-19","end_date":"2016-11-20","tags":["becb887fe82f24c644482eb30041da6d88bd8150"]}}"""
        val request_data2 = """{"filter":{"start_date":"2016-11-19","end_date":"2016-11-20","tags":["test-tag"],"events":["OE_ASSESS"]}}"""
        
        val requests = Array(
            JobRequest(Option("partner1"), Option("1234"), None, Option("SUBMITTED"), Option(request_data1),
                Option(1), Option(DateTime.now()), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
            JobRequest(Option("partner1"), Option("273645"), Option("test-job-id"), Option("COMPLETED"), Option(request_data2),
                Option(1), Option(DateTime.parse("2017-01-08", CommonUtil.dateFormat)), Option("https://test-location"), Option(DateTime.parse("2017-01-08", CommonUtil.dateFormat)), None, None, None, None, None, Option(123234), Option(532), Option(12343453L), None, None, None, None, None));
        sc.makeRDD(requests).saveToCassandra(Constants.PLATFORML_DB, Constants.JOB_REQUEST)
        
        val jobs = DBUtil.getJobRequestList("partner1")
        
        jobs.last.status.get should be ("COMPLETED")
        jobs.head.status.get should be ("SUBMITTED")
    }
}