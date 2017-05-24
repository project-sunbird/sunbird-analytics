package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestPublishPipelineSummarizer extends SparkSpec(null) {

    "PublishPipelineSummarizer" should "execute PublishPipelineSummaryModel job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/pipeline-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.PublishPipelineSummary", Option(Map("contentId" -> "numeracy_382")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestPublishPipelineSummarizer"), Option(true))
        PublishPipelineSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }

    ignore should "execute LearnerSessionSummary job fetching data from s3 prof file" in {

        val config = JobConfig(Fetcher("S3", None, Option(Array(Query(Option("prod-data-store"), Option("raw/"), None, Option("2016-05-10"), Option(0), None, None, None, None, None)))), None, null, "org.ekstep.analytics.model.PublishPipelineSummary", Option(Map("apiVersion" -> "v2")), Option(Array(Dispatcher("file", Map("file" -> "output.log")))), Option(10), Option("TestPublishPipelineSummarizer"), Option(true))
        PublishPipelineSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}
