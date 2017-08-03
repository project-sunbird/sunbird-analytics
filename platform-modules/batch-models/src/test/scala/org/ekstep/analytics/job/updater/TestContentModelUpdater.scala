package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime

class TestContentModelUpdater extends SparkSpec(null) {
    
    "ContentModelUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-popularity/test-data.json"))))), None, None, "org.ekstep.analytics.updater.UpdateContentModel", Option(Map("date" -> new DateTime().toString(CommonUtil.dateFormat).asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Content Model Updater Test"), Option(false))
        ContentModelUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }

}