package org.ekstep.analytics.dataexhaust

import java.io.File

import scala.reflect.runtime.universe

import org.apache.commons.io.FileUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.S3Util
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.JobRequest

import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.RequestConfig
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.ekstep.analytics.util.RequestFilter
import com.datastax.spark.connector.cql.CassandraConnector

class TestDataExhaustPackager extends SparkSpec(null) {

    private def preProcess() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }
        
        FileUtils.copyDirectory(new File("src/test/resources/data-exhaust/test"), new File("src/test/resources/data-exhaust-package/"))
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

    it should "execute DataExhaustPackager job from local data and won't throw any Exception" in {

        preProcess()
        val requests = Array(
            JobRequest("partner1", "1234", None, "PENDING_PACKAGING", JSONUtils.serialize(RequestConfig(RequestFilter("2016-11-19", "2016-11-20", Option(List("becb887fe82f24c644482eb30041da6d88bd8150")), Option(List("OE_INTERACT", "GE_INTERACT")), None, None), Option("eks-consumption-raw"), Option("json"))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None));

        sc.makeRDD(requests).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)

        DataExhaustPackager.execute();
        val request = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("request_id = ?", "1234").collect
        request.map { x =>
            x.status should be ("COMPLETED")
        }
    }

}