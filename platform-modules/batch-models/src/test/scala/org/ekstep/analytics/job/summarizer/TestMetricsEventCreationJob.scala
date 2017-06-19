package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestMetricsEventCreationJob extends SparkSpec(null) {
  
    "MetricsEventCreationJob" should "execute MetricsEventCreation job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/portal-session-summary/test_data_1.log"))))), null, null, "org.ekstep.analytics.model.MetricsEventCreationModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestMetricsEventCreationJob"), Option(true))
        MetricsEventCreationJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}