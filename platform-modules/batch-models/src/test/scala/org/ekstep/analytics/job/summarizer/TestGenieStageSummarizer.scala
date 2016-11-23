package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestGenieStageSummarizer extends SparkSpec(null) {
  
    "GenieStageSummarizer" should "execute GenieStageSummary and won't throw any Exception" in {
          val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/genie-stage-summary/test-data1.log"))))), null, null, "org.ekstep.analytics.model.GenieStageSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenieStageSummarizer"), Option(true))
          GenieStageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
      }
}