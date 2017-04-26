package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestContentSnapshotUpdater extends SparkSpec(null) {
  
    "ContentSnapshotUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-snapshot-updater/test_data1.json"))))), None, None, "org.ekstep.analytics.updater.UpdateContentSnapshotDB", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestContentSnapshotUpdater"), Option(false))
        ContentSnapshotUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}