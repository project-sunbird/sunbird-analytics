package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestContentUsageUpdaterJob extends SparkSpec(null) {
  
	ignore should "execute the job and shouldn't throw any exception" in {
    //"ContentUsageUpdaterJob" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-usage-updater/content_usage_updater.log"))))), None, None, "org.ekstep.analytics.updater.ContentUsageUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestContentUsageUpdaterJob"), Option(false))
        ContentUsageUpdaterJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}