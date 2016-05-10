package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent

class TestContentUsageSummary extends SparkSpec(null) {
    
    it should "test reduce by works with multiple key" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/content_usage/2016-05-09-20160509.json");
        val rdd2 = ContentUsageSummary.execute(rdd, None);
    }
}