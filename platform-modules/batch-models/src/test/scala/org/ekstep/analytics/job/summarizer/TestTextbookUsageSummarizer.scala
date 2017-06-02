package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.model.SparkSpec
 /* @author yuva
 */
class TestTextbookUsageSummarizer extends SparkSpec(null) {
    it should "execute TextbookUsageSummarizer job and won't throw any Exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/textbook-usage-summary/textbook-session-summary2.log"))))), null, None, "org.ekstep.analytics.model.TextbookUsageSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestTextbookUsageSummarizer"))
        TextbookUsageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}