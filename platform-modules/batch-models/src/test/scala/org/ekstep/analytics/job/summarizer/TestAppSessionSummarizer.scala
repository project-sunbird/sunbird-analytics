/**
 * @author Sowmya Dixit
 **/
package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestAppSessionSummarizer extends SparkSpec(null) {
  
    "AppSessionSummarizer" should "execute AppSessionSummarizer job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/portal-session-summary/v3/test_data_1.log"))))), null, null, "org.ekstep.analytics.model.AppSessionSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAppSessionSummarizer"), Option(true))
        AppSessionSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}