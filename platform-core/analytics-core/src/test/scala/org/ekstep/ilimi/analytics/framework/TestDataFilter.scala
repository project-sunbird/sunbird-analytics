package org.ekstep.ilimi.analytics.framework

import org.scalatest._
import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import com.fasterxml.jackson.core.JsonParseException
import org.ekstep.ilimi.analytics.framework.exception.DataFilterException
import org.apache.spark.SparkException
import org.ekstep.ilimi.analytics.framework.SparkSpec

/**
 * @author Santhosh
 */
class TestDataFilter extends SparkSpec {
    
    "DataFilter" should "filter the events by event id 'GE_GENIE_START'" in {
        events.count() should be (7436);
        val filters = Option(Array[Filter](
            Filter("eventId", "EQ", Option("GE_GENIE_START"))   
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (20);
        filteredEvents.first().eid.get should be("GE_GENIE_START")
    }
    
    it should "filter the events where game id equals org.ekstep.aser" in {
        val filters = Option(Array[Filter](
            Filter("gameId", "EQ", Option("org.ekstep.aser"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (6276);
        filteredEvents.first().gdata.get.id.get should be("genie.android")
    }
    
    it should "filter the events where game id not equals org.ekstep.aser" in {
        val filters = Option(Array[Filter](
            Filter("gameId", "NE", Option("org.ekstep.aser"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1160);
    }
    
    it should "filter the events by game version" in {
        val filters = Option(Array[Filter](
            Filter("gameVersion", "EQ", Option("3.0.26"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1413);
    }
    
    it should "throw DataFilterException when given filter key is not found " in {
        val filters = Option(Array[Filter](
            Filter("userName", "EQ", Option("xyz"))
        ));
        a[SparkException] should be thrownBy {
            DataFilter.filterAndSort(events, filters, None).count();
        }
    }
    
    it should "filter the events by event ts" in {
        val filters = Option(Array[Filter](
            Filter("ts", "ISNOTNULL", None)
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (7436);
    }
    
    it should "filter the events by user id" in {
        val filters = Option(Array[Filter](
            Filter("userId", "EQ", Option("7ae1e51c02982612d6a3a567b0c795819e654e2a"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (55);
    }
    
    it should "filter the events where qid is null" in {
        val filters = Option(Array[Filter](
            Filter("itemId", "ISNULL", None)
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (5794);
    }
    
    it should "filter the events where user session id is empty" in {
        val filters = Option(Array[Filter](
            Filter("sessionId", "ISEMPTY", None)
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (374);
    }
    
    it should "filter the events by telemetry version" in {
        val filters = Option(Array[Filter](
            Filter("telemetryVersion", "EQ", Option("1.0"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (7436);
    }
    
    it should "return all events if filters are none" in {
        val filteredEvents = DataFilter.filterAndSort(events, None, None);
        filteredEvents.count() should be (7436);
    }
    
    it should "match one of Events 'OE_ASSESS' & 'OE_LEVEL_SET'" in {
        val filters = Option(Array[Filter](
            Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_LEVEL_SET")))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1872);
        filteredEvents.first().gdata.get.id.get should be("org.ekstep.aser")
    }
    
    it should "filter by two criteria" in {
        val filters = Option(Array[Filter](
            Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_LEVEL_SET"))),
            Filter("gameId", "EQ", Option("org.ekstep.aser"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1872);
        filteredEvents.first().gdata.get.id.get should be("org.ekstep.aser")
    }
    
    it should "filter all events when the 'IN' clause is followed by an null array" in {
        val filters = Option(Array[Filter](
            Filter("eventId", "IN", None)
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (0);
    }
    
    it should "filter all events when qid is not empty" in {
        val filters = Option(Array[Filter](
            Filter("itemId", "ISNOTEMPTY", None)
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1633);
    }
    
    it should "match the counts returned from different filters" in {
        val filters1 = Option(Array[Filter](
            Filter("itemId", "ISNOTNULL", None)
        ));
        val filteredEvents1 = DataFilter.filterAndSort(events, filters1, None);
        
        val filters2 = Option(Array[Filter](
            Filter("eventId", "EQ", Option("OE_ASSESS"))
        ));
        val filteredEvents2 = DataFilter.filterAndSort(events, filters2, None);
        filteredEvents1.count() should be (filteredEvents2.count());
    }
}