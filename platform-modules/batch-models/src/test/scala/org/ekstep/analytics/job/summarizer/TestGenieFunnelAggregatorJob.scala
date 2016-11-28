package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher

class TestGenieFunnelAggregatorJob extends SparkSpec(null) {
  
    "GenieFunnelAggregatorJob" should "execute GenieFunnelModel and won't throw any Exception" in {
        
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/genie-funnel-aggregator/gfagg-data.log"))))), null, null, "org.ekstep.analytics.model.GenieFunnelAggregatorModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenieFunnelAggregatorJob"), Option(true))
          GenieFunnelAggregatorJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}