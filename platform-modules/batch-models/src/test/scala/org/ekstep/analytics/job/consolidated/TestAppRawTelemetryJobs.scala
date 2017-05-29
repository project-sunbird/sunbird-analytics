package org.ekstep.analytics.job.consolidated

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestAppRawTelemetryJobs extends SparkSpec(null) {
  
    "AppRawTelemetryJobs" should "execute all app raw telemetry jobs" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/portal-session-summary/test_data_1.log"))))), null, null, "org.ekstep.analytics.model.PortalSessionSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAppRawTelemetryJobs"), Option(true))
        AppRawTelemetryJobs.main(JSONUtils.serialize(config))(Option(sc));
    } 
}