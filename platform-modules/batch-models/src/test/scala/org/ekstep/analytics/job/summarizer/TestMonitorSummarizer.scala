package org.ekstep.analytics.job.summarizer
/**
 * @author Yuva
 */

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.ModelMapping

class TestMonitorSummarizer extends SparkSpec(null) {

    "MonitorSummarizer" should "execute MonitorSummaryModel job and won't throw any Exception" in {
        val modelMapping = loadFile[ModelMapping]("src/test/resources/monitor-summary/model-mapping.log").collect()
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/monitor-summary/2017-06-28.log"))))), null, null, "org.ekstep.analytics.model.MonitorSummaryModel", Option(Map("model" -> JSONUtils.serialize(modelMapping))), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), None, Option("TestMonitorSummarizer"), Option(true))
        MonitorSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }

}