package org.ekstep.analytics.views

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.util.CommonUtil
import java.io.File
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher

class TestPrecomputedViewsJob extends SparkSpec(null) {
  
    "PrecomputedViewsJob" should "execute the PrecomputedViews successfully" in {
       val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-sideloading-summary/test_data_1.log"))))), null, null, "org.ekstep.analytics.views.PrecomputedViews", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10),Option("TestPrecomputedViewsJob"), Option(true))
        PrecomputedViewsJob.main(JSONUtils.serialize(config))(Option(sc));
    }
}