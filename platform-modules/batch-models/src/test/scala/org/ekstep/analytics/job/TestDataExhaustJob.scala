package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime
import org.ekstep.analytics.model.JobRequest
import org.ekstep.analytics.model.RequestFilter
import org.ekstep.analytics.model.RequestConfig
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.S3Util

class TestDataExhaustJob extends SparkSpec(null) {

    private def preProcess() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }
    }

    private def postProcess() {
        val keys1 = S3Util.getPath("lpdev-ekstep", "data-exhaust/dev/273645/")
        for (key <- keys1) {
            S3Util.deleteObject("lpdev-ekstep", key.replace("s3n://lpdev-ekstep/", ""))
        }
        S3Util.deleteObject("lpdev-ekstep", "data-exhaust/dev/273645_$folder$");
        S3Util.deleteObject("lpdev-ekstep", "data-exhaust/dev/273645.zip");

        val keys2 = S3Util.getPath("lpdev-ekstep", "data-exhaust/dev/1234/")
        for (key <- keys2) {
            S3Util.deleteObject("lpdev-ekstep", key.replace("s3n://lpdev-ekstep/", ""))
        }
        S3Util.deleteObject("lpdev-ekstep", "data-exhaust/dev/1234_$folder$");
        S3Util.deleteObject("lpdev-ekstep", "data-exhaust/dev/1234.zip");
    }

    ignore should "execute DataExhaustJob job and won't throw any Exception" in {

        preProcess()
        postProcess()

        val requests = Array(
            JobRequest("partner1", "1234", None, "SUBMITTED", JSONUtils.serialize(RequestConfig(RequestFilter("2016-12-21", "2016-12-22", List("e4d7a0063b665b7a718e8f7e4014e59e28642f8c"), None))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None),
            JobRequest("partner1", "273645", None, "SUBMITTED", JSONUtils.serialize(RequestConfig(RequestFilter("2016-12-21", "2016-12-22", List("test-tag"), Option(List("OE_ASSESS"))))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None));

        sc.makeRDD(requests).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)
        val config = """{"search":{"type":"s3"},"model":"org.ekstep.analytics.model.DataExhaustJobModel","modelParams":{"dataset-read-bucket":"ekstep-datasets","dataset-read-prefix":"restricted/D001/4208ab995984d222b59299e5103d350a842d8d41/"}}"""
        DataExhaustJob.main(config)(Option(sc));

        val keys = S3Util.getAllKeys("lpdev-ekstep", "data-exhaust/dev/273645/")
        keys.length should be(0)
        val keys1234 = S3Util.getAllKeys("lpdev-ekstep", "data-exhaust/dev/1234/")
        keys1234.length should not be (0)

        val job1 = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("client_key = ? and request_id = ?", "partner1", "1234").first();

        job1.stage.getOrElse("") should be("UPDATE_RESPONSE_TO_DB")
        job1.stage_status.getOrElse("") should be("COMPLETED")
        job1.status should be("COMPLETED")
        job1.output_events.get should be > (0L)

        val job2 = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("client_key = ? and request_id = ?", "partner1", "273645").first();

        job2.stage.getOrElse("") should be("UPDATE_RESPONSE_TO_DB")
        job2.stage_status.getOrElse("") should be("COMPLETED")
        job2.status should be("COMPLETED")
        job2.output_events.get should be(0L)

        postProcess()
    }
}
