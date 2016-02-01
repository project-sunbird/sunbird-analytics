package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.model.BaseSpec

class TestLearnerContentActivityUpdater extends BaseSpec {

    "LearnerContentActivityUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/learner-content-summary/learner_content_test_sample.log"))))), None, None, "org.ekstep.analytics.updater.LearnerContentActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerContentActivityUpdater"), Option(false))
        LearnerActivitySummarizer.main(JSONUtils.serialize(config));
    }
}