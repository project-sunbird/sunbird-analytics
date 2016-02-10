package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestConceptSimilarityUpdaterJob extends BaseSpec {

    "LearnerContentActivityUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/concept-similarity/ConceptSimilarity.json"))))), None, None, "org.ekstep.analytics.updater.ConceptSimilarityUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestConceptSimilarityUpdaterJob"), Option(false))
        ConceptSimilarityUpdaterJob.main(JSONUtils.serialize(config));
    }
}