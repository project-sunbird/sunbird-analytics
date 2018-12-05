package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestDialcodeUsageSummarizer extends SparkSpec(null) {

  "DialcodeUsageSummarizer" should "execute the job and won't throw any Exception" in {

    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/dialcode-usage-summary/telemetry_test_data.log"))))), null, null, "org.ekstep.analytics.model.DialcodeUsageSummaryModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDialcodeUsageSummarizer"), Option(true))
    DialcodeUsageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
  }
}