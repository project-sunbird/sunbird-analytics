package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestGenieUsageSessionSummarizer extends SparkSpec(null) {
  
    "GenieUsageSessionSummarizer" should "execute GenieUsageSummary and won't throw any Exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/genie-usage-summary/test-data1.log"))))), null, null, "org.ekstep.analytics.model.GenieUsageSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenieUsageSummarizer"), Option(true))
        GenieUsageSessionSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}