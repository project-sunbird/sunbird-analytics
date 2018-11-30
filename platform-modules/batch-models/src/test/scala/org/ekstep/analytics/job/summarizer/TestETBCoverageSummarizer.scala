package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig}
import org.ekstep.analytics.model.SparkSpec

class TestETBCoverageSummarizer extends SparkSpec(null) {

  "ETBCoverageSummarizer" should "execute the job" in {

    val fromDate = "2017-11-27"
    val toDate = "2018-11-27"

    val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.ETBCoverageSummaryModel", Option(Map("fromDate" ->  fromDate, "toDate" -> toDate)), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestETBCoverageSummarizer"), Option(true))
    ETBCoverageSummarizer.main(JSONUtils.serialize(config))(Option(sc))
  }
}