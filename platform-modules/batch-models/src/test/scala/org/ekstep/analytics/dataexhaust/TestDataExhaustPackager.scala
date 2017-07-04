package org.ekstep.analytics.dataexhaust

import java.io.File

import scala.reflect.runtime.universe

import org.apache.commons.io.FileUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.JobRequest
import org.ekstep.analytics.util.RequestConfig
import org.ekstep.analytics.util.RequestFilter
import org.joda.time.DateTime

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions

class TestDataExhaustPackager extends SparkSpec(null) {

    private def preProcess() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }
    
        FileUtils.copyDirectory(new File("src/test/resources/data-exhaust/test"), new File("/data/data-exhaust/1234"))
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
    
    ignore should "execute" in {
    	val requests = Array(
            JobRequest("partner1", "B74846A442BB1B0E94745F3B56957460", None, "PENDING_PACKAGING", JSONUtils.serialize(RequestConfig(RequestFilter("2016-11-19", "2016-11-20", Option(List("becb887fe82f24c644482eb30041da6d88bd8150")), Option(List("ME_CONTENT_USAGE_SUMMARY")), None, None), Option("eks-consumption-summary"), Option("json"))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None));
    	sc.makeRDD(requests).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)
    	DataExhaustPackager.execute();
    }

}