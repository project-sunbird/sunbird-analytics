package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Filter

class TestDeviceUsageSummarizer extends SparkSpec(null) {

    "DeviceUsageSummarizer" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-usage-summary/test_data_3.log"))))), None, None, "org.ekstep.analytics.model.DeviceUsageSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDeviceUsageSummarizer"), Option(false))
        DeviceUsageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
  
    //dependent on genie usage summary (need bucket & prefix to test on sandbox)
    ignore should "execute the job from s3 data" in {
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("sandbox-data-store"), Option(""), Option("2016-01-01"), Option("2016-04-14"))))), Option(Array(Filter("eventId", "EQ", Option("ME_GENIE_SUMMARY")))), None, "org.ekstep.analytics.model.DeviceUsageSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDeviceUsageSummarizer"), Option(false))
        DeviceUsageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}