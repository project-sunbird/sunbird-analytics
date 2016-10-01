package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher

class TestItemSummaryUpdater extends SparkSpec(null) {
    
    "ItemSummaryUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/item-summary-updater/ius_1.log"))))), None, None, "org.ekstep.analytics.updater.UpdateItemSummaryDB", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestItemSummaryUpdater"), Option(false))
        ItemSummaryUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
  
}