package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestConceptSnapshotUpdater extends SparkSpec(null) {
  
    "ConceptSnapshotUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/concept-snapshot-updater/test_data1.json"))))), None, None, "org.ekstep.analytics.updater.UpdateConceptSnapshotDB", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestConceptSnapshotUpdater"), Option(false))
        ConceptSnapshotUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}