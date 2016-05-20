package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event

class TestGenieUsageSummary extends SparkSpec(null) {
    
    it should "generate content summary events" in {
        val rdd = loadFile[Event]("src/test/resources/genie_usage_summary/2016-05-17-1463483780803.json.gz");
        val rdd2 = GenieUsageSummary.execute(rdd, None);
        val events = rdd2.collect
        for(e<-events){
            println(e)
        }
    }
}