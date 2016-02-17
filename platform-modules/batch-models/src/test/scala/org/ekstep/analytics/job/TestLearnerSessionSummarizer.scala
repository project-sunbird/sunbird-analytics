package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestLearnerSessionSummarizer extends BaseSpec {

    "LearnerSessionSummarizer" should "execute LearnerSessionSummary job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/session-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.LearnerSessionSummary", Option(Map("contentId" -> "numeracy_382")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestLearnerSessionSummarizer"), Option(true))
        LearnerSessionSummarizer.main(JSONUtils.serialize(config));
    }
    
    ignore should "execute LearnerSessionSummary job fetching data from local file" in {

        val config = JobConfig(
                Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("/Users/Santhosh/ekStep/telemetry_dump/219bdc84-d8a2-41df-b769-016ead916846.log"))))), None, null, "org.ekstep.analytics.model.LearnerSessionSummary", Option(Map("apiVersion" -> "v2")), Option(Array(Dispatcher("file", Map("file" -> "output.log")))), Option(10), Option("TestLearnerSessionSummarizer"), Option(true))
        LearnerSessionSummarizer.main(JSONUtils.serialize(config));
    }
}