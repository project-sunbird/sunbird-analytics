package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils

class TestContentSideloadingSummary extends SparkSpec(null) {
    
    "ContentSideloadingSummary" should "generate content summary events" in {
        val rdd = loadFile[Event]("src/test/resources/content-sideloading-summary/test_data_1.log");
        val rdd2 = ContentSideloadingSummary.execute(rdd, None);
        val events = rdd2.collect
        
        events.length should be (2)
        
        
    }
}