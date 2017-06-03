package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestAuthorSummaryUpdater extends SparkSpec(null) {

    it should "execute AuthorSummaryUpdater job and won't throw any Exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/author-usage-updater/test.log"))))), null, None, "org.ekstep.analytics.updater.UpdateAuthorSummaryDB", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAuthorSummaryUpdater"))
        AuthorSummaryUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}