package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.CommonUtil
import java.util.UUID
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.util.S3Util

class TestDataExhaustJobModel extends SparkSpec(null) {

    "DataExhaustJobModel" should "generate zip file of all events and save to s3" in {

        val request1 = """{"filter": {"start_date": "2016-11-17","end_date": "2016-11-18","tags": ["becb887fe82f24c644482eb30041da6d88bd8151"]}}"""
        val jobRequest1 = JobRequest("dev-portal", "12334", None, "SUBMITTED", request1, None, None, None, None, None, Option(1), DateTime.now(), None, None, None, None, None, None, None, None, None, None)

        val request2 = """{"filter": {"start_date": "2016-11-19","end_date": "2016-11-20","tags": ["becb887fe82f24c644482eb30041da6d88bd8150"]}}"""
        val jobRequest2 = JobRequest("dev-portal", "273645", None, "SUBMITTED", request2, None, None, None, None, None, Option(1), DateTime.now(), None, None, None, None, None, None, None, None, None, None)

        //val rdd = sc.makeRDD(Seq(jobRequest1, jobRequest2))
        val rdd = sc.makeRDD(Seq(jobRequest2))

        rdd.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)

        val fetcher = Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/data-exhaust/*")))))
        val data = DataFetcher.fetchBatchData[String](fetcher).cache();

        val keys1 = S3Util.getPath("lpdev-ekstep", "data-exhaust/dev/273645")
        println(keys1.length)

        for (key <- keys1) {
            S3Util.deleteObject("lpdev-ekstep", key)
        }
        S3Util.deleteObject("lpdev-ekstep", "data-exhaust/dev/273645_$folder$");

        val requestConfig = Map(
            "request_id" -> "273645",
            "client_key" -> "dev-portal",
            "job_id" -> UUID.randomUUID().toString(),
            "data-exhaust-bucket" -> "lpdev-ekstep",
            "data-exhaust-prefix" -> "data-exhaust/dev");

        val out = DataExhaustJobModel.execute(data, Option(requestConfig)).collect
        out.length should be(1)
        val res = out.last
        res.output_events should be(3817)
        res.client_key should be("dev-portal")
        res.request_id should be("273645")
        val keys = S3Util.getAllKeys("lpdev-ekstep", "data-exhaust/dev/273645/")
        keys.length should not be (0)
    }
}