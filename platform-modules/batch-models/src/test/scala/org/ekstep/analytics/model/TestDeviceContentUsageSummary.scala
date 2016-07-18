package org.ekstep.analytics.model

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils

class TestDeviceContentUsageSummary extends SparkSpec(null) {
  
    it should "generate device content summary events" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/device-content-usage-summary/test_data1.log");
        val rdd2 = DeviceContentUsageSummary.execute(rdd, None);
        val events = rdd2.collect
        
        events.length should be(3)
    }
}