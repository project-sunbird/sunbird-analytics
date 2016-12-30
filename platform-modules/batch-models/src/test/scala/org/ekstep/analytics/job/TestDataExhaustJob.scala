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
import java.io.File

class TestDataExhaustJob extends SparkSpec(null) {

    private def preProcess() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }
    }

    private def deleteS3File() {
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

    private def deleteLocalFile(path: String) {
        val file = new File(path)
        if (file.exists())
            CommonUtil.deleteDirectory(path)
    }
    private def postProcess(path: String, keyDetails: Option[Map[String, String]] = None) {
        //deleteS3File()
        deleteLocalFile(path)
    }

    it should "execute DataExhaustJob job and won't throw any Exception" in {

        preProcess()
        
        val requests = Array(
            JobRequest("partner1", "1234", None, "SUBMITTED", JSONUtils.serialize(RequestConfig(RequestFilter("2016-11-19", "2016-11-20", List("becb887fe82f24c644482eb30041da6d88bd8150"), None))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None),
            JobRequest("partner1", "273645", None, "SUBMITTED", JSONUtils.serialize(RequestConfig(RequestFilter("2016-11-19", "2016-11-20", List("test-tag"), Option(List("OE_ASSESS"))))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None));

        sc.makeRDD(requests).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)
        //val config = """{"search":{"type":"s3"},"model":"org.ekstep.analytics.model.DataExhaustJobModel","modelParams":{"dataset-read-bucket":"ekstep-datasets","dataset-read-prefix":"restricted/D001/4208ab995984d222b59299e5103d350a842d8d41/", "dispatch-to":"s3"}}"""
        val config = """{"search":{"type":"local","queries":[{"file":"src/test/resources/data-exhaust/*"}]},"model":"org.ekstep.analytics.model.DataExhaustJobModel","output":[{"to":"file","params":{"file": "/tmp/dataexhaust"}}],"parallelization":8,"appName":"Data Exhaust","deviceMapping":false, "modelParams":{"dispatch-to":"local"}}"""

        DataExhaustJob.main(config)(Option(sc));

        val files1 = new File("/tmp/dataexhaust/1234").listFiles()
        files1.length should not be (0)

        val files2 = new File("/tmp/dataexhaust/273645")
        files2.isDirectory() should be(false)

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

        postProcess("/tmp/dataexhaust/1234")
    }
}
