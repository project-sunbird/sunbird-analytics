package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Filter

class TestDeviceSpecificationUpdater extends SparkSpec(null) {
  
  "DeviceSpecificationUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-specification/raw.telemetry.test1.json"))))), None, None, "org.ekstep.analytics.model.DeviceSpecification", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("DeviceSpecificationUpdater"), Option(false))
        DeviceSpecificationUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
    
    ignore should "execute the job from s3 data" in {
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("ekstep-dev-data-store"), Option("raw/"), Option("2016-01-01"), Option("2016-04-14"))))), Option(Array(Filter("eventId","EQ",Option("GE_GENIE_START")))), None, "org.ekstep.analytics.model.DeviceSpecification", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("DeviceSpecificationUpdater"), Option(false))
        DeviceSpecificationUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}