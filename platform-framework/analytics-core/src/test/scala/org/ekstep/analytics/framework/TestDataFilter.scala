package org.ekstep.analytics.framework

import org.scalatest._
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import com.fasterxml.jackson.core.JsonParseException
import org.ekstep.analytics.framework.exception.DataFilterException
import org.apache.spark.SparkException
import org.ekstep.analytics.framework.util.JSONUtils
import scala.collection.mutable.Buffer

@scala.reflect.BeanInfo
case class Test(id: String, value: Option[String], optValue: Option[String]);

/**
 * @author Santhosh
 */
class TestDataFilter extends SparkSpec {
    
    "DataFilter" should "filter the events by event id 'GE_GENIE_START'" in {
        events.count() should be (7437);
        val filters = Option(Array[Filter](
            Filter("eventId", "EQ", Option("GE_GENIE_START"))   
        ));
        val filteredEvents = DataFilter.filterAndSort[Event](events, filters, None);
        filteredEvents.count() should be (20);
        filteredEvents.first().eid should be("GE_GENIE_START")
    }
    
    it should "filter the events where game id equals org.ekstep.aser" in {
        val filters = Option(Array[Filter](
            Filter("gameId", "EQ", Option("org.ekstep.aser"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (6276);
        filteredEvents.first().gdata.id should be("genie.android")
    }
    
    it should "filter the events where game id not equals org.ekstep.aser" in {
        val filters = Option(Array[Filter](
            Filter("gameId", "NE", Option("org.ekstep.aser"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1161);
    }
    
    it should "filter the events by game version" in {
        val filters = Option(Array[Filter](
            Filter("gameVersion", "EQ", Option("3.0.26"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1413);
    }
    
    it should "filter by custom key using bean property matching " in {
        val filters = Option(Array[Filter](
            Filter("edata.eks.loc", "EQ", Option("13.3421418,77.1194668"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (20);
    }
    
    it should "filter the events by event ts" in {
        val filters = Option(Array[Filter](
            Filter("ts", "ISNOTNULL", None)
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (7437);
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
        filteredEvents.count() should be (5795);
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
        filteredEvents.count() should be (7437);
    }
    
    it should "return all events if filters are none" in {
        val filteredEvents = DataFilter.filterAndSort(events, None, None);
        filteredEvents.count() should be (7437);
    }
    
    it should "match one of Events 'OE_ASSESS' & 'OE_LEVEL_SET'" in {
        val filters = Option(Array[Filter](
            Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_LEVEL_SET")))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1872);
        filteredEvents.first().gdata.id should be("org.ekstep.aser")
    }
    
    it should "match none of Events 'OE_ASSESS' & 'OE_LEVEL_SET'" in {
        val filters = Option(Array[Filter](
            Filter("eventId", "NIN", Option(List("OE_ASSESS", "OE_LEVEL_SET")))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (5565);
        filteredEvents.filter { x => "OE_ASSESS".equals(x.eid) }.count should be (0);
        filteredEvents.filter { x => "OE_LEVEL_SET".equals(x.eid) }.count should be (0);
    }
    
    it should "filter by two criteria" in {
        val filters = Option(Array[Filter](
            Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_LEVEL_SET"))),
            Filter("gameId", "EQ", Option("org.ekstep.aser"))
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (1872);
        filteredEvents.first().gdata.id should be("org.ekstep.aser")
    }
    
    it should "filter all events when the 'IN' clause is followed by an null array" in {
        val filters = Option(Array[Filter](
            Filter("eventId", "IN", None)
        ));
        val filteredEvents = DataFilter.filterAndSort(events, filters, None);
        filteredEvents.count() should be (0);
    }
    
    it should "filter all events when the 'NIN' clause is followed by an null array" in {
        val filters = Option(Array[Filter](
            Filter("eventId", "NIN", None)
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
    
    it should "sort the events by edata.eks.id" in  {
        val e1 = events.take(1)(0);
        val sortedEvents = DataFilter.sortBy(events, Sort("edata.eks.id", Option("desc")));
        val e2 = sortedEvents.take(1)(0);
        e1.eid should not be (e2.eid)
        e1.ts should not be (e2.ts)
        e2.gdata.id should be ("org.ekstep.aser")
    }
    
    it should "filter and sort the events by edata.eks.id" in  {
            
        val e1 = events.take(1)(0);
        val sortedEvents = DataFilter.filterAndSort(events, None, Option(Sort("edata.eks.id", Option("desc"))));
        val e2 = sortedEvents.take(1)(0);
        e1.eid should not be (e2.eid)
        e1.ts should not be (e2.ts)
        e2.gdata.id should be ("org.ekstep.aser")
    }
    
    it should "not throw an exception if filter or sort is null " in  {
        
        noException should be thrownBy {
            DataFilter.sortBy(events, null).collect()    
        }
        
        noException should be thrownBy {
            DataFilter.filter(events, null.asInstanceOf[Filter]).collect()    
        }
        
        noException should be thrownBy {
            DataFilter.filter(events.collect().toBuffer, null.asInstanceOf[Filter]);    
        }
        
        noException should be thrownBy {
            DataFilter.filter(events, null.asInstanceOf[Array[Filter]]).collect()    
        }
        
    }
    
    it should "throw DataFilterException for unknown matcher" in  {
        a[SparkException] should be thrownBy {
            DataFilter.filter(events, Filter("eid", "NOTIN", Option("OE_INTERACT"))).collect();
        }
    }
    
    it should "filter optional fields also" in {
        
        val testArray = Array(Test("One", Option("1"), Option("Ek")),Test("Two", Option("2"), None));
        val rdd = sc.parallelize(testArray, 1);
        val result1 = DataFilter.filter(rdd, Filter("value", "EQ", Option("2"))).collect();
        result1.size should be (1);
        result1(0).id should be ("Two");
        
        val result2 = DataFilter.filter(rdd, Filter("optValue", "EQ", Option("Do"))).collect();
        result2.size should be (0);
        
    }
    
    it should "filter buffer of events" in {
        val rdd = Buffer(Test("One", Option("1"), Option("Ek")),Test("Two", Option("2"), None));
        val result1 = DataFilter.filter(rdd, Filter("value", "EQ", Option("2")));
        result1.size should be (1);
        result1(0).id should be ("Two");
    }
    
}