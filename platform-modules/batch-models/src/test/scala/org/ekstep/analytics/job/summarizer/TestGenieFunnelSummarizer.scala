package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestGenieFunnelSummarizer extends SparkSpec(null) {
    
  "GenieFunnelSummarizer" should "execute GenieFunnelModel and won't throw any Exception" in {
          
      val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/genie-funnel/genie-funnel-data.log"))))), null, null, "org.ekstep.analytics.model.GenieFunnelModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenieFunnelSummarizer"), Option(true))
          GenieFunnelSummarizer.main(JSONUtils.serialize(config))(Option(sc));
      }
}