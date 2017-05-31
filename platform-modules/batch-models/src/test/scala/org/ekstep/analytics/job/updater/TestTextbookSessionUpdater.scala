package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author yuva
 */
class TestTextbookSessionUpdater extends SparkSpec(null) {
  
    ignore should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/textbook-session-updater/textbook-session-updater.log"))))), None, None, "org.ekstep.analytics.updater.TextbookSessionUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Textbook session updater Test"), Option(false))
        TextbookSessionUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}