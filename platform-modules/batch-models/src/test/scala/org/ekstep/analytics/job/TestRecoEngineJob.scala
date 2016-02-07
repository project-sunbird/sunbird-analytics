package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestRecoEngineJob extends BaseSpec {

    "Reco Engine Job" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/reco-engine/session-summary-prod-test.log"))))), None, None, "org.ekstep.analytics.model.RecoEngine", None, Option(Array(Dispatcher("file", Map("file" -> "/Users/amitBehera/re-test-out.log")))), Option(10), Option("TestRecoEngineJob"), Option(false))
        println(JSONUtils.serialize(config))
        //LearnerActivitySummarizer.main(JSONUtils.serialize(config));
    }
}