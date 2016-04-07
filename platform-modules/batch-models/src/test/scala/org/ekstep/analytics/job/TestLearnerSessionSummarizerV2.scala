package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestLearnerSessionSummarizerV2 extends SparkSpec(null) {

    "LearnerSessionSummarizerV2" should "execute LearnerSessionSummary job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/session-summary/v2_telemetry.json"))))), null, null, "org.ekstep.analytics.model.LearnerSessionSummaryV2", Option(Map("apiVersion" -> "v2")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerSessionSummarizer"), Option(true))
        LearnerSessionSummarizerV2.main(JSONUtils.serialize(config))(Option(sc));
    }
    
}