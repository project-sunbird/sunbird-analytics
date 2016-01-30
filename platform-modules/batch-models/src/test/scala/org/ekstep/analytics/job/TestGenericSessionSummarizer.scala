package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestGenericSessionSummarizer extends BaseSpec {

    "GenericSessionSummarizer" should "execute GenericSessionSummary job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/session-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.GenericSessionSummary", Option(Map("contentId" -> "numeracy_382")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenericSessionSummarizer"), Option(true))
        GenericSessionSummarizer.main(JSONUtils.serialize(config));
    }
    
    ignore should "execute GenericSessionSummary job fetching data from S3" in {

        val config = JobConfig(
                Fetcher("s3", None, Option(Array(Query(Option("sandbox-ekstep-telemetry"), Option("sandbox.telemetry.unique-"), None, Option("2016-01-15"), Option(0))))), Option(Array(Filter("eventId","IN",Option(List("OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"))))), null, "org.ekstep.analytics.model.GenericSessionSummaryV2", Option(Map()), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenericSessionSummarizer"), Option(true))
        GenericSessionSummarizer.main(JSONUtils.serialize(config));
    }
}