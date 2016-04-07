package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Filter

class TestLearnerActivitySummarizer extends SparkSpec(null) {
    
    "LearnerActivitySummarizer" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-activity-summary/learner_activity_summary_sample1.log"))))), None, None, "org.ekstep.analytics.model.LearnerActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerActivitySummarizer"), Option(false))
        LearnerActivitySummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
    
    ignore should "execute the job from s3 data" in {
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("sandbox-session-summary"), Option("sandbox.analytics.screener-"), None, Option("2016-01-15"), Option(0))))), Option(Array(Filter("eventId","EQ",Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.LearnerActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerActivitySummarizer"), Option(false))
        LearnerActivitySummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}