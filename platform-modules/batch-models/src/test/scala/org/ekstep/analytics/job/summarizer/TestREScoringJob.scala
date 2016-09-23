package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestREScoringJob extends SparkSpec(null) {
  
    ignore should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))), None, None, "org.ekstep.analytics.model.REScoringModel", Option(Map("saveDataFrame" -> true.asInstanceOf[AnyRef],"live_content_limit" -> Int.box(1000))), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestREScoringJob"), Option(false))
        REScoringJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}