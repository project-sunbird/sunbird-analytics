package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestContentEditorUsageSummarizer extends SparkSpec(null) {
  
    "ContentEditorUsageSummarizer" should "execute ContentEditorUsageSummarizer job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-editor-usage-summary/test_data1.log"))))), null, null, "org.ekstep.analytics.model.ContentEditorUsageSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestContentEditorUsageSummarizer"), Option(true))
        ContentEditorUsageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}