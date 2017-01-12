package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestContentSideloadingSummarizer extends SparkSpec(null) {
  
    "ContentSideloadingSummarizer" should "execute ContentSideloadingSummary job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-sideloading-summary/test_data_1.log"))))), None, None, "org.ekstep.analytics.model.ContentSideloadingSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("ContentSideloadingSummarizer"), Option(false))
        ContentSideloadingSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
    
    ignore should "execute the job from s3 data" in {
        
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("ekstep-dev-data-store"), Option("raw/"), Option("2016-04-01"), Option("2016-04-30"))))), Option(Array(Filter("eventId","EQ",Option("GE_TRANSFER")))), None, "org.ekstep.analytics.model.ContentSideloadingSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("ContentSideloadingSummarizer"), Option(false))
        ContentSideloadingSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}