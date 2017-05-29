package org.ekstep.analytics.job.consolidated

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestAppRawTelemetryUpdaters extends SparkSpec(null) {
  
    "AppRawTelemetryUpdaters" should "execute all app raw telemetry updaters" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/object-lifecycle/test-data1.log"))))), null, null, "org.ekstep.analytics.updater.UpdateAppObjectCacheDB", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAppRawTelemetryUpdaters"), Option(true))
        AppRawTelemetryUpdaters.main(JSONUtils.serialize(config))(Option(sc));
    } 
}