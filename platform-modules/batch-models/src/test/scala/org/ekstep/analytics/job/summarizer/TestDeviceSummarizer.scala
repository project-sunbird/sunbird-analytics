package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.{JobConfig, Fetcher, Query, Dispatcher}
import org.ekstep.analytics.framework.util.JSONUtils

class TestDeviceSummarizer extends SparkSpec(null) {
  
    "DeviceSummarizer" should "execute DeviceSummarizer job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.DeviceSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDeviceSummarizer"))
        DeviceSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}