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
import org.ekstep.analytics.util._
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.util.S3Util
import java.io.File

class TestDataExhaustJobModel extends SparkSpec(null) {

    private def deleteLocalFile(path: String) {
        val file = new File(path)
        if (file.exists())
            CommonUtil.deleteDirectory(path)
    }

    "DataExhaustJobModel" should "generate zip file of all events and save to local in json format" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }

        val request = """{"filter": {"start_date": "2016-11-19","end_date": "2016-11-20","tags": ["becb887fe82f24c644482eb30041da6d88bd8150"]}}"""
        val jobRequest = JobRequest("dev-portal", "273646", None, "PROCESSING", request, None, None, None, None, None, Option(1), DateTime.now(), None, None, None, None, None, None, None, None, Option("FETCHING_DATA"), Option("COMPLETED"))

        val rdd = sc.makeRDD(Seq(jobRequest))

        rdd.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)

        val fetcher = Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/data-exhaust/*")))))
        val data = DataFetcher.fetchBatchData[String](fetcher).cache();

        val requestConfig = Map(
            "request_id" -> "273646",
            "client_key" -> "dev-portal",
            "job_id" -> UUID.randomUUID().toString(),
            "data-exhaust-bucket" -> "lpdev-ekstep",
            "data-exhaust-prefix" -> "data-exhaust/dev",
            "dispatch-to" -> "local",
            "path" -> "/tmp/dataexhaust",
            "output_format" -> "json",
            "dataset_id" -> "D002");

        val out = DataExhaustJobModel.execute(data, Option(requestConfig)).collect
        out.length should be(1)
        val res = out.last
        res.output_events should be(3817)
        res.client_key should be("dev-portal")
        res.request_id should be("273646")
        val files1 = new File("/tmp/dataexhaust/273646").listFiles()
        files1.length should not be (0)
        deleteLocalFile("/tmp/dataexhaust/273646")

    }
    
    "DataExhaustJobModel" should "generate zip file of all events and save to local in csv format" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }

        val request = """{"filter": {"start_date": "2016-11-19","end_date": "2016-11-20","tags": ["becb887fe82f24c644482eb30041da6d88bd8150"], "events":["OE_ASSESS"]}}"""
        val jobRequest = JobRequest("dev-portal", "273646", None, "PROCESSING", request, None, None, None, None, None, Option(1), DateTime.now(), None, None, None, None, None, None, None, None, Option("FETCHING_DATA"), Option("COMPLETED"))

        val rdd = sc.makeRDD(Seq(jobRequest))

        rdd.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)

        val fetcher = Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/data-exhaust/*")))))
        val data = DataFetcher.fetchBatchData[String](fetcher).cache();

        val requestConfig = Map(
            "request_id" -> "273646",
            "client_key" -> "dev-portal",
            "job_id" -> UUID.randomUUID().toString(),
            "data-exhaust-bucket" -> "lpdev-ekstep",
            "data-exhaust-prefix" -> "data-exhaust/dev",
            "dispatch-to" -> "local",
            "path" -> "/tmp/dataexhaust",
            "output_format" -> "csv");

        val out = DataExhaustJobModel.execute(data, Option(requestConfig)).collect
        out.length should be(1)
        val res = out.last
        res.output_events should be(95)
        res.client_key should be("dev-portal")
        res.request_id should be("273646")
        val files1 = new File("/tmp/dataexhaust/273646").listFiles()
        files1.length should not be (0)
        deleteLocalFile("/tmp/dataexhaust/273646")

    }
}