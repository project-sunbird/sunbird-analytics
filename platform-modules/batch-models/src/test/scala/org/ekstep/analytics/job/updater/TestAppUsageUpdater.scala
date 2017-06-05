/**
 * @author Sowmya Dixit
 */
package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestAppUsageUpdater extends SparkSpec(null) {
  
    "AppUsageUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/portal-usage-updater/test_data_1.log"))))), None, None, "org.ekstep.analytics.updater.AppUsageUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("App Usage Updater Test"), Option(false))
        AppUsageUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}