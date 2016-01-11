package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestGenericSessionSummarizer extends BaseSpec {

    it should "execute GenericSessionSummarizer job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/session-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.GenericSessionSummary", Option(Map("contentId" -> "numeracy_382")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenericSessionSummarizer"), Option(true))
        GenericSessionSummarizer.main(JSONUtils.serialize(config));
    }
}