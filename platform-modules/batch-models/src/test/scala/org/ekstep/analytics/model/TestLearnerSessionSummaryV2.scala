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
    
    "LearnerSessionSummaryV2" should "generate session summary for telemetry v2" in {
        
        val rdd = loadFile[TelemetryEventV2]("src/test/resources/session-summary/v2_telemetry.json");
        val rdd2 = LearnerSessionSummaryV2.execute(sc, rdd, Option(Map("modelId" -> "LearnerSessionSummaryV2")));
        val me = rdd2.collect();
        me.length should be (3);
        
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        event1.eid should be ("ME_SESSION_SUMMARY");
        event1.context.pdata.model should be ("LearnerSessionSummaryV2");
        event1.context.pdata.ver should be ("1.0");
        event1.context.granularity should be ("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be ("org.ekstep.quiz.app.elephant");
        
        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be (-1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be (0);
        summary1.noOfAttempts should be (2);
        summary1.timeSpent should be (389);
        summary1.interactEventsPerMin should be (5.55);
        summary1.currentLevel should not be (None);
        summary1.currentLevel.get.size should be (0);
        summary1.noOfInteractEvents should be (36);
        summary1.itemResponses.get.length should be (7);
        
        summary1.activitySummary.get.size should be (4);
        summary1.activitySummary.get.get("LISTEN").get.count should be (15);
        summary1.activitySummary.get.get("LISTEN").get.timeSpent should be (183.06);
        summary1.activitySummary.get.get("SHOW").get.count should be (8);
        summary1.activitySummary.get.get("SHOW").get.timeSpent should be (0);
        summary1.activitySummary.get.get("TOUCH").get.count should be (10);
        summary1.activitySummary.get.get("TOUCH").get.timeSpent should be (254.18);
        summary1.activitySummary.get.get("HIDE").get.count should be (3);
        summary1.activitySummary.get.get("HIDE").get.timeSpent should be (0);
        
        summary1.eventsSummary.size should be (6);
        summary1.eventsSummary.get("OE_ITEM_RESPONSE").get should be (9);
        summary1.eventsSummary.get("OE_ASSESS").get should be (7);
        summary1.eventsSummary.get("OE_START").get should be (1);
        summary1.eventsSummary.get("OE_END").get should be (1);
        summary1.eventsSummary.get("OE_INTERACT").get should be (36);
        summary1.eventsSummary.get("OE_NAVIGATE").get should be (2);
        
        summary1.screenSummary.get.size should be (3);
        summary1.screenSummary.get("scene11") should be (294.08);
        summary1.screenSummary.get("activity") should be (77.74);
        summary1.screenSummary.get("endScreen2") should be (17.5);
        
        summary1.syncDate should be (1454328710554L)
        
        val event2 = JSONUtils.deserialize[MeasuredEvent](me(1));
        val summary2 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.screenSummary.get.size should be (10);
        summary2.screenSummary.get("scene6") should be (19.62);
        summary2.screenSummary.get("scene3") should be (122.22);
        summary2.screenSummary.get("endScreen2") should be (0.5);
        summary2.screenSummary.get("scene2") should be (47.41);
        summary2.screenSummary.get("splash") should be (23.96);
        summary2.screenSummary.get("scene1") should be (84.78);
        summary2.screenSummary.get("endScreen") should be (15.29);
        summary2.screenSummary.get("activity") should be (65.11);
        summary2.screenSummary.get("scene4") should be (59.69);
        summary2.screenSummary.get("scene5") should be (113.73);
        
        val event3 = JSONUtils.deserialize[MeasuredEvent](me(2));
        val summary3 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event3.edata.eks));
        
        summary3.screenSummary.get.size should be (2);
        summary3.screenSummary.get("activity") should be (74.55);
        summary3.screenSummary.get("worksheet") should be (0.44);
        summary3.levels.get.length should be (0);
        summary3.activitySummary.get.size should be (1);
        summary3.timeSpent should be (74.99);
        summary3.eventsSummary.size should be (3);
        summary3.eventsSummary.get("OE_START").get should be (1);
        summary3.eventsSummary.get("OE_NAVIGATE").get should be (1);
        summary3.eventsSummary.get("OE_INTERACT").get should be (1);
        summary3.interactEventsPerMin should be (0.8);
    }
    
    it should "generate session summary for v2 item model" in {
        
        val rdd = loadFile[TelemetryEventV2]("src/test/resources/session-summary/v2_telemetry.json");
        val rdd2 = LearnerSessionSummaryV2.execute(sc, rdd, Option(Map("modelId" -> "LearnerSessionSummaryV2", "apiVersion" -> "v2")));
        val me = rdd2.collect();
        me.length should be (3);
        
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        event1.eid should be ("ME_SESSION_SUMMARY");
        event1.context.pdata.model should be ("LearnerSessionSummaryV2");
        event1.context.pdata.ver should be ("1.0");
        event1.context.granularity should be ("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be ("org.ekstep.quiz.app.elephant");
        
        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be (-1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be (0);
        summary1.noOfAttempts should be (2);
        summary1.timeSpent should be (389);
        summary1.interactEventsPerMin should be (5.55);
        summary1.currentLevel should not be (None);
        summary1.currentLevel.get.size should be (0);
        summary1.noOfInteractEvents should be (36);
        summary1.itemResponses.get.length should be (7);
        
        summary1.activitySummary.get.size should be (4);
        summary1.activitySummary.get.get("LISTEN").get.count should be (15);
        summary1.activitySummary.get.get("LISTEN").get.timeSpent should be (183.06);
        summary1.activitySummary.get.get("SHOW").get.count should be (8);
        summary1.activitySummary.get.get("SHOW").get.timeSpent should be (0);
        summary1.activitySummary.get.get("TOUCH").get.count should be (10);
        summary1.activitySummary.get.get("TOUCH").get.timeSpent should be (254.18);
        summary1.activitySummary.get.get("HIDE").get.count should be (3);
        summary1.activitySummary.get.get("HIDE").get.timeSpent should be (0);
        
        summary1.eventsSummary.size should be (6);
        summary1.eventsSummary.get("OE_ITEM_RESPONSE").get should be (9);
        summary1.eventsSummary.get("OE_ASSESS").get should be (7);
        summary1.eventsSummary.get("OE_START").get should be (1);
        summary1.eventsSummary.get("OE_END").get should be (1);
        summary1.eventsSummary.get("OE_INTERACT").get should be (36);
        summary1.eventsSummary.get("OE_NAVIGATE").get should be (2);
        
        summary1.screenSummary.get.size should be (3);
        summary1.screenSummary.get("scene11") should be (294.08);
        summary1.screenSummary.get("activity") should be (77.74);
        summary1.screenSummary.get("endScreen2") should be (17.5);
        
        summary1.syncDate should be (1454328710554L)
        
        val event2 = JSONUtils.deserialize[MeasuredEvent](me(1));
        val summary2 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.screenSummary.get.size should be (10);
        summary2.screenSummary.get("scene6") should be (19.62);
        summary2.screenSummary.get("scene3") should be (122.22);
        summary2.screenSummary.get("endScreen2") should be (0.5);
        summary2.screenSummary.get("scene2") should be (47.41);
        summary2.screenSummary.get("splash") should be (23.96);
        summary2.screenSummary.get("scene1") should be (84.78);
        summary2.screenSummary.get("endScreen") should be (15.29);
        summary2.screenSummary.get("activity") should be (65.11);
        summary2.screenSummary.get("scene4") should be (59.69);
        summary2.screenSummary.get("scene5") should be (113.73);
        
        val event3 = JSONUtils.deserialize[MeasuredEvent](me(2));
        val summary3 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event3.edata.eks));
        
        summary3.screenSummary.get.size should be (2);
        summary3.screenSummary.get("activity") should be (74.55);
        summary3.screenSummary.get("worksheet") should be (0.44);
        summary3.levels.get.length should be (0);
        summary3.activitySummary.get.size should be (1);
        summary3.timeSpent should be (74.99);
        summary3.eventsSummary.size should be (3);
        summary3.eventsSummary.get("OE_START").get should be (1);
        summary3.eventsSummary.get("OE_NAVIGATE").get should be (1);
        summary3.eventsSummary.get("OE_INTERACT").get should be (1);
        summary3.interactEventsPerMin should be (0.8);
    }
    
    it should "compute interrupt summary and exclude interrupt summary in screen summary" in {
        
        val rdd = loadFile[TelemetryEventV2]("src/test/resources/session-summary/test_data4.log");
        val rdd2 = LearnerSessionSummaryV2.execute(sc, rdd, Option(Map("modelId" -> "LearnerSessionSummaryV2", "apiVersion" -> "v2")));
        val me = rdd2.collect();
        me.length should be (1);
        
        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be (-1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be (0);
        summary1.noOfAttempts should be (1);
        summary1.timeSpent should be (377);
        summary1.interactEventsPerMin should be (3.5);
        summary1.noOfInteractEvents should be (22);
        summary1.interruptTime should be (15.34);
        
        summary1.activitySummary.get.size should be (4);
        summary1.eventsSummary.size should be (5);
        
        summary1.screenSummary.get.size should be (9);
        summary1.screenSummary.get("scene6") should be (20.16);
        summary1.screenSummary.get("scene3") should be (110.05);
        summary1.screenSummary.get("scene2") should be (20.46);
        
        summary1.screenSummary.get("splash") should be (44.35);
        summary1.screenSummary.get("scene1") should be (25.37);
        summary1.screenSummary.get("endScreen") should be (13.43);
        
        summary1.screenSummary.get("activity") should be (76.41);
        summary1.screenSummary.get("scene4") should be (23.9);
        summary1.screenSummary.get("scene5") should be (27.63);
        
    }
    
}