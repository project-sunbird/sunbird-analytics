package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import com.datastax.spark.connector._
import org.ekstep.analytics.util.JobRequest
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.RequestConfig
import org.joda.time.DateTime
import java.io.File
import org.ekstep.analytics.util.RequestFilter
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.S3Util
import org.ekstep.analytics.framework.util.CommonUtil

class TestDataExhaustPackager extends SparkSpec(null) {

    private def preProcess() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }
    }
    
     private def deleteS3File(bucket: String, prefix: String, request_ids: Array[String]) {

        for (request_id <- request_ids) {
            val keys1 = S3Util.getPath(bucket, prefix + "/" + request_id)
            for (key <- keys1) {
                S3Util.deleteObject(bucket, key.replace(s"s3n://$bucket/", ""))
            }
            S3Util.deleteObject(bucket, prefix + "/" + request_id + "_$folder$");
            S3Util.deleteObject(bucket, prefix + "/" + request_id + ".zip");
        }
    }
    
    private def deleteLocalFile(path: String) {
        val file = new File(path)
        if (file.exists())
            CommonUtil.deleteDirectory(path)
    }
    private def postProcess(fileDetails: Map[String, String], request_ids: Array[String]) {
        val fileType = fileDetails.get("fileType").get
        fileType match {
            case "s3" =>
                val bucket = fileDetails.get("bucket").get
                val prefix = fileDetails.get("prefix").get
                deleteS3File(bucket, prefix, request_ids)
            case "local" =>
                val path = fileDetails.get("path").get
                deleteLocalFile(path)
        }
    }

    "TestDataExhaustPackager" should "execute DataExhaustPackager job from local data and won't throw any Exception" in {
     
        preProcess()
        val requests = Array(
            JobRequest("partner1", "1234", None, "PENDING_PACKAGING", JSONUtils.serialize(RequestConfig(RequestFilter("2016-11-19", "2016-11-20", Option(List("becb887fe82f24c644482eb30041da6d88bd8150")), Option(List("OE_INTERACT", "GE_INTERACT")), None, None),Option("eks-consumption-raw"), Option("json"))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None));

        sc.makeRDD(requests).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)

        val config = """{"search":{"type":"local","queries":[{"file":"src/test/resources/data-exhaust-package/"}]},"model":"org.ekstep.analytics.model.DataExhaustJobModel","output":[{"to":"file","params":{"file": "/tmp/dataexhaust"}}],"parallelization":8,"appName":"Data Exhaust","deviceMapping":false, "exhaustConfig":{"eks-consumption-raw":{"events":["DEFAULT"],"eventConfig":{"DEFAULT":{"eventType":"ConsumptionRaw","searchType":"local","saveType":"local","fetchConfig":{"params":{"file":"src/test/resources/data-exhaust/consumption-raw/*"}},"filterMapping":{"tags":{"name":"genieTag","operator":"IN"}},"saveConfig":{"params":{"path":"/tmp/data-exhaust/"}}}}}}}"""
        DataExhaustPackager.main(config)(Option(sc));

        val files1 = new File("/tmp/target/1234").listFiles()
        files1.length should not be (0)   
         val fileDetails = Map("fileType" -> "local", "path" -> "/tmp/target")
        val request_ids = Array("1234")
        postProcess(fileDetails, request_ids)
    }

}