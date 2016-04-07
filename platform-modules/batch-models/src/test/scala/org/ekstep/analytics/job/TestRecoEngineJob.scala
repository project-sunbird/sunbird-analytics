package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestRecommendationEngineJob extends SparkSpec(null) {

    "Recommendation Engine Job" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/reco-engine/reco_engine_test.log"))))), None, None, "org.ekstep.analytics.model.RecommendationEngine", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestRecommendationEngineJob"), Option(false))
        RecommendationEngineJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}