/**
 * @author Santhosh Vasabhaktula
 */
package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestAppObjectCacheUpdater extends SparkSpec(null) {

    "AppObjectCacheUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/object-lifecycle/test-data1.log"))))), None, None, "org.ekstep.analytics.job.updater.AppObjectCacheUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAppObjectCacheUpdater"), Option(false))
        AppObjectCacheUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}