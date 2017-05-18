package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkGraphSpec

class TestContentCreationMetricsUpdater extends SparkGraphSpec(null) {

    it should "execute ContentCreationMetricsUpdater job and won't throw any Exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))), null, None, "org.ekstep.analytics.updater.UpdateContentCreationMetricsDB", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestContentCreationMetricsUpdater"))
        ContentCreationMetricsUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}