package org.ekstep.analytics.model

import java.io.FileWriter
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.Event
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.DeserializationFeature
import org.ekstep.analytics.framework.MeasuredEvent

/**
 * @author Santhosh
 */
class TestLearnerSessionSummaryV2 extends SparkSpec(null) {
    
    "LearnerSessionSummary" should "generate session summary for v2 telemetry" in {
        
        val rdd = loadFile[Event]("src/test/resources/session-summary/v2_telemetry.json");
        val rdd2 = LearnerSessionSummary.execute(rdd, Option(Map("modelId" -> "LearnerSessionSummary", "apiVersion" -> "v2")));
        val me = rdd2.collect();
        me.length should be (1);
        
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        event1.eid should be ("ME_SESSION_SUMMARY");
        event1.mid should be ("288F7A6E4E7BA48031386E84774DC61A");
        event1.context.pdata.model should be ("LearnerSessionSummary");
        event1.context.pdata.ver should be ("1.0");
        event1.context.granularity should be ("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be ("numeracy_377");
        
        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be (-1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be (0);
        summary1.noOfAttempts should be (2);
        summary1.timeSpent should be (553.01);
        summary1.timeDiff should be (553.01);
        summary1.interactEventsPerMin should be (14.21);
        summary1.currentLevel should not be (None);
        summary1.currentLevel.get.size should be (0);
        summary1.noOfInteractEvents should be (131);
        summary1.itemResponses.get.length should be (34);
        summary1.interruptTime should be (8.28);
        
        // Checking for partnerID and group_user value
        event1.dimensions.group_user.get.asInstanceOf[Boolean] should be (false)
        
        val asList = summary1.activitySummary.get 
        asList.size should be (4);
        val asActCountMap = asList.map { x => (x.actType,x.count) }.toMap
        val asActTimeSpentMap = asList.map { x => (x.actType,x.timeSpent) }.toMap
        asActCountMap.get("LISTEN").get should be (1);
        asActTimeSpentMap.get("LISTEN").get should be (0.17);
        asActCountMap.get("DROP").get should be (39);
        asActTimeSpentMap.get("DROP").get should be (0.34);
        asActCountMap.get("TOUCH").get should be (39);
        asActTimeSpentMap.get("TOUCH").get should be (464.25);
        asActCountMap.get("CHOOSE").get should be (26);
        asActTimeSpentMap.get("CHOOSE").get should be (0.22);
        
        val esList = summary1.eventsSummary
        val esMap = esList.map { x => (x.id,x.count) }.toMap
        esList.size should be (7);
        esMap.get("OE_ITEM_RESPONSE").get should be (65);
        esMap.get("OE_ASSESS").get should be (34);
        esMap.get("OE_START").get should be (1);
        esMap.get("OE_END").get should be (1);
        esMap.get("OE_INTERACT").get should be (105);
        esMap.get("OE_NAVIGATE").get should be (6);
        esMap.get("OE_INTERRUPT").get should be (2);
        
        val ssList = summary1.screenSummary.get
        val ssMap = ssList.map { x => (x.id,x.timeSpent) }.toMap
        ssList.size should be (5);
        ssMap.get("questions_g4").get should be (62.22);
        ssMap.get("endScreen_g5").get should be (421.34);
        ssMap.get("questions_g5").get should be (44.32);
        ssMap.get("endScreen_g4").get should be (1.71);
        ssMap.get("splash").get should be (15.16);
        
        summary1.syncDate should be (1459558762917L)
        event1.syncts should be (summary1.syncDate);
    }
    
    ignore should "parse telemetry file" in {
        val rdd = sc.textFile("src/test/resources/session-summary/v2_telemetry.json", 1).map { line => 
            val mapper = new ObjectMapper();
            mapper.registerModule(DefaultScalaModule);
            mapper.readValue[Event](line, classOf[Event]);
        }.cache();
    }
    
}