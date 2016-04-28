package org.ekstep.analytics.model

import java.io.FileWriter
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.Event
import com.datastax.spark.connector.cql.CassandraConnector

/**
 * @author Santhosh
 */
class TestLearnerSessionSummary extends SparkSpec(null) {

    "LearnerSessionSummary" should "generate session summary and pass all positive test cases" in {

        val rdd = loadFile[Event]("src/test/resources/session-summary/test_data1.log");
        val rdd2 = LearnerSessionSummary.execute(rdd, Option(Map("modelVersion" -> "1.4", "modelId" -> "GenericSessionSummaryV2")));
        val me = rdd2.collect();
        me.length should be(1);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        event1.eid should be("ME_SESSION_SUMMARY");
        event1.mid should be("06D6C96652BA3F3473661EBC1E2CDCF0");
        event1.context.pdata.model should be("GenericSessionSummaryV2");
        event1.context.pdata.ver should be("1.4");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be("org.ekstep.aser.lite");

        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be(1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be(2);
        summary1.noOfAttempts should be(2);
        summary1.timeSpent should be(875);
        summary1.interactEventsPerMin should be(2.74);
        summary1.currentLevel should not be (None);
        summary1.currentLevel.get.get("numeracy").get should be("Can do subtraction");
        summary1.currentLevel.get.get("literacy").get should be("Can read story");
        summary1.noOfInteractEvents should be(40);
        summary1.itemResponses.get.length should be(5);
        event1.syncts should be(summary1.syncDate);

        val asList = summary1.activitySummary.get
        asList.size should be(2);
        val asActCountMap = asList.map { x => (x.actType, x.count) }.toMap
        val asActTimeSpentMap = asList.map { x => (x.actType, x.timeSpent) }.toMap

        asActCountMap.get("TOUCH").get should be(31);
        asActTimeSpentMap.get("TOUCH").get should be(757);
        asActCountMap.get("DRAG").get should be(9);
        asActTimeSpentMap.get("DRAG").get should be(115);
        summary1.screenSummary.get.size should be(0);
        summary1.syncDate should be(1451696364328L)
        summary1.mimeType.get should be("application/vnd.android.package-archive");
        summary1.contentType.get should be("Game");
    }

    it should "generate 4 session summarries and pass all negative test cases" in {

        val rdd = loadFile[Event]("src/test/resources/session-summary/test_data2.log");
        val rdd2 = LearnerSessionSummary.execute(rdd, Option(Map("modelVersion" -> "1.2", "modelId" -> "GenericContentSummary")));
        val me = rdd2.collect();
        me.length should be(4);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        // Validate for event envelope
        event1.eid should be("ME_SESSION_SUMMARY");
        event1.mid should be("A78764A945C237B2A1F837130212A5C7");
        event1.context.pdata.model should be("GenericContentSummary");
        event1.context.pdata.ver should be("1.2");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be("org.ekstep.aser.lite");

        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfLevelTransitions.get should be(-1);
        summary1.levels should not be (None);
        summary1.levels.get.length should be(0);
        summary1.noOfAttempts should be(1);
        summary1.timeSpent should be(47);
        summary1.interactEventsPerMin should be(6.38);
        summary1.currentLevel should not be (None);
        summary1.currentLevel.get.size should be(0);
        summary1.noOfInteractEvents should be(5);
        summary1.itemResponses.get.length should be(0);
        summary1.activitySummary.get.size should be(1);
        event1.syncts should be(summary1.syncDate);

        val asList = summary1.activitySummary.get
        val asActCountMap = asList.map { x => (x.actType, x.count) }.toMap
        val asActTimeSpentMap = asList.map { x => (x.actType, x.timeSpent) }.toMap

        asActCountMap.get("TOUCH").get should be(5);
        asActTimeSpentMap.get("TOUCH").get should be(47);

        val esList = summary1.eventsSummary
        esList.size should be(2);

        val esMap = esList.map { x => (x.id, x.count) }.toMap

        esMap.get("OE_INTERACT").get should be(5);
        esMap.get("OE_START").get should be(1);
        summary1.syncDate should be(1451694073672L)
        summary1.mimeType.get should be("application/vnd.android.package-archive");
        summary1.contentType.get should be("Game");

        val event2 = JSONUtils.deserialize[MeasuredEvent](me(1));
        event2.mid should be("06D6C96652BA3F3473661EBC1E2CDCF0");

        val summary2 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.noOfLevelTransitions.get should be(0);
        summary2.levels should not be (None);
        summary2.levels.get.length should be(1);
        summary2.noOfAttempts should be(1);
        summary2.timeSpent should be(875);
        summary2.interactEventsPerMin should be(1.71);
        summary2.currentLevel should not be (None);
        summary2.currentLevel.get.size should be(1);
        summary2.currentLevel.get.get("literacy").get should be("Can read story");
        summary2.noOfInteractEvents should be(25);
        summary2.itemResponses.get.length should be(2);
        summary2.activitySummary.get.size should be(1);
        event2.syncts should be(summary2.syncDate);

        val asList2 = summary2.activitySummary.get
        val asActCountMap2 = asList2.map { x => (x.actType, x.count) }.toMap
        val asActTimeSpentMap2 = asList2.map { x => (x.actType, x.timeSpent) }.toMap

        asActCountMap2.get("TOUCH").get should be(25);
        asActTimeSpentMap2.get("TOUCH").get should be(739);

        val esList2 = summary2.eventsSummary
        esList2.size should be(4);
        val esMap2 = esList2.map { x => (x.id, x.count) }.toMap

        esMap2.get("OE_INTERACT").get should be(25);
        esMap2.get("OE_START").get should be(1);
        esMap2.get("OE_LEVEL_SET").get should be(1);
        esMap2.get("OE_ASSESS").get should be(2);
        summary2.syncDate should be(1451696364325L)
        summary2.mimeType.get should be("application/vnd.android.package-archive");
        summary2.contentType.get should be("Game");

        val event3 = JSONUtils.deserialize[MeasuredEvent](me(2));
        event3.mid should be("2D6AB5FC7D5D7961AB300D8E4D459538");

        val summary3 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event3.edata.eks));
        summary3.noOfLevelTransitions.get should be(-1);
        summary3.levels should not be (None);
        summary3.levels.get.length should be(0);
        summary3.noOfAttempts should be(1);
        summary3.timeSpent should be(0);
        summary3.interactEventsPerMin should be(0);
        summary3.currentLevel should not be (None);
        summary3.currentLevel.get.size should be(0);
        summary3.noOfInteractEvents should be(0);
        summary3.itemResponses.get.length should be(0);
        summary3.activitySummary.get.size should be(0);

        val esList3 = summary3.eventsSummary
        val esMap3 = esList3.map { x => (x.id, x.count) }.toMap

        esList3.size should be(1);
        esMap3.get("OE_START").get should be(1);

        summary3.syncDate should be(1451696364329L)
        summary3.mimeType.get should be("application/vnd.android.package-archive");
        summary3.contentType.get should be("Game");
        event3.syncts should be(summary3.syncDate);

        val event4 = JSONUtils.deserialize[MeasuredEvent](me(3));
        event4.mid should be("08D37F42C718121C6140EDF9F89889B2");

        val summary4 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event4.edata.eks));
        summary4.noOfLevelTransitions.get should be(-1);
        summary4.levels should not be (None);
        summary4.levels.get.length should be(0);
        summary4.noOfAttempts should be(1);
        summary4.timeSpent should be(11);
        summary4.interactEventsPerMin should be(16.36);
        summary4.currentLevel should not be (None);
        summary4.currentLevel.get.size should be(0);
        summary4.noOfInteractEvents should be(3);
        summary4.itemResponses.get.length should be(0);
        summary4.activitySummary.get.size should be(1);

        val asList4 = summary4.activitySummary.get
        val asActCountMap4 = asList4.map { x => (x.actType, x.count) }.toMap
        val asActTimeSpentMap4 = asList4.map { x => (x.actType, x.timeSpent) }.toMap
        asActCountMap4.get("TOUCH").get should be(3);
        asActTimeSpentMap4.get("TOUCH").get should be(11);

        val esList4 = summary4.eventsSummary
        val esMap4 = esList4.map { x => (x.id, x.count) }.toMap

        esList4.size should be(2);
        esMap4.get("OE_INTERACT").get should be(3);
        esMap4.get("OE_START").get should be(1);

        summary4.syncDate should be(1451715800197L)
        summary4.mimeType.get should be("application/vnd.android.package-archive");
        summary4.contentType.get should be("Game");
        event4.syncts should be(summary4.syncDate);
    }

    it should "generate 3 session summaries and validate the screen summaries" in {

        val rdd = loadFile[Event]("src/test/resources/session-summary/test_data3.log");
        val rdd2 = LearnerSessionSummary.execute(rdd, None);
        val me = rdd2.collect();
        me.length should be(3);

        val event1 = JSONUtils.deserialize[MeasuredEvent](me(0));
        // Validate for event envelope
        event1.eid should be("ME_SESSION_SUMMARY");
        event1.mid should be("27B3CF85556974581D97739493A3FCC8");
        event1.context.pdata.model should be("LearnerSessionSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be("org.ekstep.story.hi.nature");

        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));

        val ssList = summary1.screenSummary.get
        ssList.size should be(18);
        val summaryMap = ssList.map { x => (x.id, x.timeSpent) }.toMap

        summaryMap.getOrElse("scene11", 0d) should be(4.0);
        summaryMap.getOrElse("scene5", 0d) should be(5.0);
        summaryMap.getOrElse("scene14", 0d) should be(4.0);
        summaryMap.getOrElse("scene17", 0d) should be(17.0);
        summaryMap.getOrElse("scene4", 0d) should be(5.0);
        summaryMap.getOrElse("scene7", 0d) should be(5.0);
        summaryMap.getOrElse("scene16", 0d) should be(5.0);
        summaryMap.getOrElse("scene10", 0d) should be(12.0);
        summaryMap.getOrElse("scene13", 0d) should be(4.0);
        summaryMap.getOrElse("scene9", 0d) should be(5.0);
        summaryMap.getOrElse("scene3", 0d) should be(4.0);
        summaryMap.getOrElse("scene6", 0d) should be(4.0);
        summaryMap.getOrElse("scene15", 0d) should be(6.0);
        summaryMap.getOrElse("scene18", 0d) should be(1.0);
        summaryMap.getOrElse("scene12", 0d) should be(9.0);
        summaryMap.getOrElse("scene8", 0d) should be(10.0);
        summaryMap.getOrElse("scene2", 0d) should be(9.0);
        summaryMap.getOrElse("splash", 0d) should be(14.0);
        summary1.mimeType.get should be("application/vnd.ekstep.ecml-archive");
        summary1.contentType.get should be("Story");

        val event2 = JSONUtils.deserialize[MeasuredEvent](me(1));
        val summary2 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.screenSummary.get.size should be(0);
        summary2.mimeType.get should be("application/vnd.ekstep.content-collection");
        summary2.contentType.get should be("Collection");

        val event3 = JSONUtils.deserialize[MeasuredEvent](me(2));
        val summary3 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event3.edata.eks));

        val ssList3 = summary3.screenSummary.get
        val ssMap3 = ssList3.map { x => (x.id, x.timeSpent) }.toMap

        ssList3.size should be(2);
        ssMap3.getOrElse("ordinalNumbers", 0d) should be(226.0);
        ssMap3.getOrElse("splash", 0d) should be(24.0);

        summary3.mimeType.get should be("application/vnd.ekstep.ecml-archive");
        summary3.contentType.get should be("Worksheet");
    }

    it should "generate a session even though OE_START and OE_END are present" in {
        val rdd = loadFile[Event]("src/test/resources/session-summary/test_data5.log");
        val rdd1 = LearnerSessionSummary.execute(rdd, Option(Map("apiVersion" -> "v2")));
        val rs = rdd1.collect();
    }

    it should "generate a session where the content is not a valid one" in {
        val rdd = loadFile[Event]("src/test/resources/session-summary/test_data6.log");
        val rdd1 = LearnerSessionSummary.execute(rdd, Option(Map("apiVersion" -> "v2")));
        val rs = rdd1.collect();
    }

    it should " generate ME by adding partnerid as org.ekstep.partner.pratham " in {
        val rdd = loadFile[Event]("src/test/resources/session-summary/test_data_partnerId.log");
        val rdd1 = LearnerSessionSummary.execute(rdd, Option(Map("apiVersion" -> "v2")));
        println(rdd1.collect().last)
        val eventMap = JSONUtils.deserialize[MeasuredEvent](rdd1.collect().last).edata.eks.asInstanceOf[Map[String, AnyRef]];
        eventMap.get("partnerId").get should be("org.ekstep.partner.pratham")
    }

    it should "check group_user and partner id will be empty" in {
        val rdd = loadFile[Event]("src/test/resources/session-summary/test_data_groupInfo.log");
        val rdd1 = LearnerSessionSummary.execute(rdd, Option(Map("apiVersion" -> "v2")));
        //val learner_id = "1aca2342-3865-4f67-aff5-048027cba8b1"
        val eventMap = JSONUtils.deserialize[MeasuredEvent](rdd1.collect()(0)).edata.eks.asInstanceOf[Map[String, AnyRef]];
        val eventMapLast = JSONUtils.deserialize[MeasuredEvent](rdd1.collect()last).edata.eks.asInstanceOf[Map[String, AnyRef]];
        eventMap.get("groupUser").get.asInstanceOf[Boolean] should be(false)
        eventMapLast.get("groupUser").get.asInstanceOf[Boolean] should be(false)
        eventMap.get("partnerId").get should be("")
        eventMapLast.get("partnerId").get should be("")
    }

    it should "check group_user for a learner_id = 1aca2342-3865-4f67-aff5-048027cba8b1" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("INSERT INTO learner_db.learnerprofile (learner_id,did,gender,language,loc,standard,age,year_of_birth,group_user,anonymous_user,created_date,updated_date) VALUES ('1aca2342-3865-4f67-aff5-048027cba8b1','d5ef0395fb76e056d54758007ae353f16d898a7b','','','',0,0,0,true,false,1461829438478,1461829438478)");
        }
        val rdd = loadFile[Event]("src/test/resources/session-summary/test_data_groupInfo.log");
        val rdd1 = LearnerSessionSummary.execute(rdd, Option(Map("apiVersion" -> "v2")));
        val eventMap = JSONUtils.deserialize[MeasuredEvent](rdd1.collect()(0)).edata.eks.asInstanceOf[Map[String, AnyRef]];
        eventMap.get("groupUser").get.asInstanceOf[Boolean] should be(true)
        
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM learner_db.learnerconceptrelevance where learner_id='1aca2342-3865-4f67-aff5-048027cba8b1'");
        }
    }

    ignore should "extract timespent from takeoff summaries" in {
        val rdd = loadFile[MeasuredEvent]("/Users/Santhosh/ekStep/telemetry_dump/takeoff-summ.log");
        val rdd2 = rdd.map { x => (x.uid, JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(x.edata.eks))) };
        val rdd3 = rdd2.map { x =>
            (x._1, x._2.timeSpent, x._2.start_time, x._2.end_time)
        }
            .sortBy(f => f._2, true, 1)
            .map { JSONUtils.serialize(_); }

        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "test-output.log")), rdd3);
    }

    ignore should "generate send events to a file" in {
        val rdd = loadFile[Event]("/Users/Santhosh/ekStep/telemetry_dump/87f90da2-31a4-41e9-ad83-5042f9a82da7.log");
        val rdd2 = LearnerSessionSummary.execute(rdd, None);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "test-output2.log")), rdd2);
    }
}