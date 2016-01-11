package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.util.JSONUtils

class TestAserScreenSummarizer extends BaseSpec {

    it should "execute AserScreenSummarizer job and won't throw any Exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))), null, None, "org.ekstep.analytics.model.AserScreenSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAserScreenSummarizer"))
        AserScreenSummarizer.main(JSONUtils.serialize(config));
    }
}