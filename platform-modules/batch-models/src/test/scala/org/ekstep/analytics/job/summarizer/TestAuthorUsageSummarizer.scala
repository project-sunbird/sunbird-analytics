package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.model.SparkGraphSpec
/**
 * @author yuva
 */
class TestAuthorUsageSummarizer extends SparkGraphSpec(null) {
    it should "execute AuthorUsageSummarizer job and won't throw any Exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/portal-session-summary/portal-session-summary.log"))))), null, None, "org.ekstep.analytics.model.AuthorUsageSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAuthorUsageSummarizer"))
        AuthorUsageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}