package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestGenieLaunchV1Summarizer extends SparkSpec(null) {
  
    "GenieLaunchSummarizer" should "execute GenieLunchSummary and won't throw any Exception" in {
          val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/genie-usage-summary/v2/test-data1.log"))))), null, null, "org.ekstep.analytics.model.GenieUsageSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenieUsageSummarizer"), Option(true))
          GenieLaunchV1Summarizer.main(JSONUtils.serialize(config))(Option(sc));
      }
}