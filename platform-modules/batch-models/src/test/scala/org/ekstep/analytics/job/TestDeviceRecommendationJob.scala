package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestDeviceRecommendationJob extends SparkSpec(null) {
  
    it should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))), None, None, "org.ekstep.analytics.model.DeviceRecommendationModel", Option(Map("saveDataFrame" -> true.asInstanceOf[AnyRef],"live_content_limit" -> Int.box(1000))), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDeviceRecommendationJob"), Option(false))
        //DeviceRecommendationJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}