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

class TestDataExhaustJob extends SparkSpec(null) {

    ignore should "execute DataExhaustJob job and won't throw any Exception" in {

        val requests = Array(
            JobRequest("partner1", "6a54bfa283de43a89086", None, "SUBMITTED", JSONUtils.serialize(RequestConfig(RequestFilter("2016-12-21", "2016-12-23", List("e4d7a0063b665b7a718e8f7e4014e59e28642f8c"), None))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None),
            JobRequest("partner1", "e69e2efdc9eb6750493d", None, "SUBMITTED", JSONUtils.serialize(RequestConfig(RequestFilter("2016-12-21", "2016-12-23", List("e4d7a0063b665b7a718e8f7e4014e59e28642f8c"), Option(List("OE_ASSESS"))))),
                None, None, None, None, None, None, DateTime.now(), None, None, None, None, None, None, None, None, None, None));
        sc.makeRDD(requests).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST)
        val config = """{"search":{"type":"s3"},"model":"org.ekstep.analytics.model.DataExhaustJobModel","modelParams":{"dataset-read-bucket":"ekstep-datasets","dataset-read-prefix":"restricted/D001/4208ab995984d222b59299e5103d350a842d8d41/"}}"""
        DataExhaustJob.main(config)(Option(sc));
    }
}
