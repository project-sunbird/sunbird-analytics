package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils


/**
 * @author Santhosh
 */
class TestScreenTimeSummary extends SparkSpec(null) {
    
    "ScreenTimeSummary" should "generate send events to a file" in {
        val rdd = loadFile[Event]("/Users/Santhosh/ekStep/telemetry_dump/prod.telemetry.unique-2016-01-18-10-25.json");
        val filteredRDD = DataFilter.filter(rdd, Array(Filter("eventId","IN",Option(List("OE_START","OE_END","OE_INTERACT","OE_INTERRUPT"))), Filter("uid", "EQ", Option("2772bf4c-02f5-4ef5-88c0-b13a0b6a86c3"))));
        
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "test.log")), filteredRDD.map {JSONUtils.serialize(_)});
        //val rdd2 = ScreenTimeSummary.execute(sc, filteredRDD, None);
        //rdd2.collect().foreach{println(_)};
    }
  
}