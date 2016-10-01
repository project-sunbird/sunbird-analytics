package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestGenieUsageSummarizer extends SparkSpec(null) {
    
    "GenieUsageSummarizer" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/genie-usage-summary-model/gus_1.log"))))), None, None, "org.ekstep.analytics.model.GenieUsageSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestGenieUsageSummarizer"), Option(false))
        GenieUsageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }  
}