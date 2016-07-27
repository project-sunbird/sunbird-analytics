package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.util.DerivedEvent

class TestFieldExtractor extends SparkSpec(null) {
    
    "FieldExtractor" should "extract the fields from raw telemetry" in {
        val event = loadFile[Event]("src/test/resources/aserlite-screen-summary/raw.telemetry.test1.json");
        val rdd = DataFilter.filter(event, Filter("eventId", "EQ", Option("OE_ASSESS")));
        val rdd2 = EventFieldExtractor.execute(rdd, Option(Map("headers" -> "eid,ts,timespent,gameId", "fields" -> "eid,ts,edata.eks.length,gdata.id")));
        
        rdd2.count() should be(5);
    }
    
    it should "extract the fields from derived telemetry" in {
        val events = loadFile[DerivedEvent]("src/test/resources/content-usage-summary/test_data1.log");
        val rdd = DerivedEventFieldExtractor.execute(events, Option(Map("headers" -> "eid,ts,timespent,gameId,groupuser", "fields" -> "eid,ets,edata.eks.timeSpent,dimensions.gdata.id,dimensions.group_user")));
        
        rdd.count() should be(110);
    }
    
}