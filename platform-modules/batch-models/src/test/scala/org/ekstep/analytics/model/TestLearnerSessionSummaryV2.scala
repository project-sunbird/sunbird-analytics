package org.ekstep.analytics.model

import java.io.FileWriter
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.TelemetryEventV2
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
    
    "LearnerSessionSummaryV2" should "generate session summary for v2 telemetry" in {
        
        val rdd = loadFile[TelemetryEventV2]("src/test/resources/session-summary/v2_telemetry.json");
        val rdd2 = LearnerSessionSummaryV2.execute(sc, rdd, Option(Map("modelId" -> "LearnerSessionSummaryV2", "apiVersion" -> "v2")));
        val me = rdd2.collect();
        me.length should be (1);
        
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        event1.eid should be ("ME_SESSION_SUMMARY");
        event1.mid should be ("D2EC164E229CDA1E7E990F405F1C4225");
        event1.context.pdata.model should be ("LearnerSessionSummaryV2");
        event1.context.pdata.ver should be ("1.0");
        event1.context.granularity should be ("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be ("numeracy_377");
        
        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be (-1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be (0);
        summary1.noOfAttempts should be (2);
        summary1.timeSpent should be (543.54);
        summary1.interactEventsPerMin should be (14.46);
        summary1.currentLevel should not be (None);
        summary1.currentLevel.get.size should be (0);
        summary1.noOfInteractEvents should be (131);
        summary1.itemResponses.get.length should be (34);
        summary1.interruptTime should be (8.28);
        
        summary1.activitySummary.get.size should be (4);
        summary1.activitySummary.get.get("LISTEN").get.count should be (1);
        summary1.activitySummary.get.get("LISTEN").get.timeSpent should be (0.17);
        summary1.activitySummary.get.get("DROP").get.count should be (39);
        summary1.activitySummary.get.get("DROP").get.timeSpent should be (0.42);
        summary1.activitySummary.get.get("TOUCH").get.count should be (39);
        summary1.activitySummary.get.get("TOUCH").get.timeSpent should be (464.28);
        summary1.activitySummary.get.get("CHOOSE").get.count should be (26);
        summary1.activitySummary.get.get("CHOOSE").get.timeSpent should be (0.26);
        
        summary1.eventsSummary.size should be (7);
        summary1.eventsSummary.get("OE_ITEM_RESPONSE").get should be (65);
        summary1.eventsSummary.get("OE_ASSESS").get should be (34);
        summary1.eventsSummary.get("OE_START").get should be (1);
        summary1.eventsSummary.get("OE_END").get should be (1);
        summary1.eventsSummary.get("OE_INTERACT").get should be (105);
        summary1.eventsSummary.get("OE_NAVIGATE").get should be (6);
        summary1.eventsSummary.get("OE_INTERRUPT").get should be (2);
        
        summary1.screenSummary.get.size should be (5);
        summary1.screenSummary.get("questions_g4") should be (62.22);
        summary1.screenSummary.get("endScreen_g5") should be (421.34);
        summary1.screenSummary.get("questions_g5") should be (44.32);
        summary1.screenSummary.get("endScreen_g4") should be (1.71);
        summary1.screenSummary.get("splash") should be (15.16);
        
        summary1.syncDate should be (1456314643296L)
    }
    
    ignore should "parse telemetry file" in {
        val rdd = sc.textFile("src/test/resources/session-summary/v2_telemetry.json", 1).map { line => 
            val mapper = new ObjectMapper();
            mapper.registerModule(DefaultScalaModule);
            mapper.readValue[TelemetryEventV2](line, classOf[TelemetryEventV2]);
        }.cache();
        println("rdd.count", rdd.count)
    }
    
}