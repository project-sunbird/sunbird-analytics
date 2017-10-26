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
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.util.DerivedEvent
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.V3Event

/**
 * @author Santhosh
 */
class TestLearnerSessionSummaryModel extends SparkSpec(null) {

    "LearnerSessionSummaryModel" should "generate session summary and pass all positive test cases" in {

        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data1.log");
        val rdd2 = LearnerSessionSummaryModel.execute(rdd, Option(Map("modelVersion" -> "1.4", "modelId" -> "GenericSessionSummaryV2")));
        val me = rdd2.collect();
        me.length should be(1);
        val event1 = me(0);
        event1.dimensions.pdata.get.id should be ("genie")
        event1.channel should be ("in.ekstep")
        
        event1.eid should be("ME_SESSION_SUMMARY");
        event1.context.pdata.model.get should be("GenericSessionSummaryV2");
        event1.context.pdata.ver should be("1.4");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be("org.ekstep.aser.lite");

        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        
        summary1.noOfAttempts should be(2);
        summary1.timeSpent should be(875);
        summary1.interactEventsPerMin should be(2.74);
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

        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data2.log");
        val rdd2 = LearnerSessionSummaryModel.execute(rdd, Option(Map("modelVersion" -> "1.2", "modelId" -> "GenericContentSummary")));
        val me = rdd2.collect();
        me.length should be(3);
        
        val event1 = me.filter { x => x.uid.equals("34a50115-737f-47ee-999c-952c02e374fe") }.last
        
        // Validate for event envelope
        event1.eid should be("ME_SESSION_SUMMARY");
        event1.context.pdata.model.get should be("GenericContentSummary");
        event1.context.pdata.ver should be("1.2");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be("org.ekstep.aser.lite");

        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfAttempts should be(1);
        summary1.timeSpent should be(47);
        summary1.interactEventsPerMin should be(6.38);
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

        val event2 = me.filter { x => x.uid.equals("2ac2ebf4-89bb-4d5d-badd-ba402ee70182") }.last
        val summary2 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.noOfAttempts should be(1);
        summary2.timeSpent should be(875);
        summary2.interactEventsPerMin should be(1.71);
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

        val event3 = me.filter { x => x.uid.equals("d47c4108-d348-4805-b3e8-5a34cc4fc2c2") }.last;

        val summary3 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event3.edata.eks));
        summary3.noOfAttempts should be(1);
        summary3.timeSpent should be(11.0);
        summary3.interactEventsPerMin should be(16.36);
        summary3.noOfInteractEvents should be(3);
        summary3.itemResponses.get.length should be(0);
        summary3.activitySummary.get.size should be(1);

        val esList3 = summary3.eventsSummary
        val esMap3 = esList3.map { x => (x.id, x.count) }.toMap

        esList3.size should be(2);
        esMap3.get("OE_START").get should be(1);

        summary3.syncDate should be(1451715800197L)
        summary3.mimeType.get should be("application/vnd.android.package-archive");
        summary3.contentType.get should be("Game");
        event3.syncts should be(summary3.syncDate);
    }

    it should "generate 3 session summaries and validate the screen summaries" in {

        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data3.log");
        val rdd2 = LearnerSessionSummaryModel.execute(rdd, None);
        val me = rdd2.collect();
        me.length should be(2);
        val event1 = me(0);
        // Validate for event envelope
        event1.eid should be("ME_SESSION_SUMMARY");
//        event1.mid should be("27B3CF85556974581D97739493A3FCC8");
        event1.context.pdata.model.get should be("LearnerSessionSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be("org.ekstep.story.hi.nature");

        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        
        val ssList = summary1.screenSummary.get
        ssList.size should be(18);
        val summaryMap = ssList.map { x => (x.id, (x.timeSpent, x.visitCount)) }.toMap

        summaryMap.getOrElse("scene11", (0d, 0L)) should be(4.0, 2);
        summaryMap.getOrElse("scene5", (0d, 0L)) should be(5.0, 2);
        summaryMap.getOrElse("scene14", (0d, 0L)) should be(4.0, 3);
        summaryMap.getOrElse("scene17", (0d, 0L)) should be(17.0, 2);
        summaryMap.getOrElse("scene4", (0d, 0L)) should be(5.0, 2);
        summaryMap.getOrElse("scene7", (0d, 0L)) should be(5.0, 3);
        summaryMap.getOrElse("scene16", (0d, 0L)) should be(5.0, 2);
        summaryMap.getOrElse("scene10", (0d, 0L)) should be(12.0, 2);
        summaryMap.getOrElse("scene13", (0d, 0L)) should be(4.0, 2);
        summaryMap.getOrElse("scene9", (0d, 0L)) should be(5.0, 2);
        summaryMap.getOrElse("scene3", (0d, 0L)) should be(4.0, 2);
        summaryMap.getOrElse("scene6", (0d, 0L)) should be(4.0, 3);
        summaryMap.getOrElse("scene15", (0d, 0L)) should be(6.0, 2);
        summaryMap.getOrElse("scene18", (0d, 0L)) should be(1.0, 1);
        summaryMap.getOrElse("scene12", (0d, 0L)) should be(9.0, 3);
        summaryMap.getOrElse("scene8", (0d, 0L)) should be(10.0, 3);
        summaryMap.getOrElse("scene2", (0d, 0L)) should be(9.0, 2);
        summaryMap.getOrElse("splash", (0d, 0L)) should be(14.0, 4);
        summary1.mimeType.get should be("application/vnd.android.package-archive");
        summary1.contentType.get should be("Game");

        val event2 = me(1);
        val summary2 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.screenSummary.get.size should be(2);
        summary2.mimeType.get should be("application/vnd.android.package-archive");
        summary2.contentType.get should be("Game");
    }

    it should "generate a session even though OE_START and OE_END are present" in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data5.log");
        val rdd1 = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2")));
        val rs = rdd1.collect();
    }

    it should "generate a session where the content is not a valid one" in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data6.log");
        val rdd1 = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2")));
        val rs = rdd1.collect();
    }

    it should "check group_user and partner id will be empty" in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data_groupInfo.log");
        val rdd1 = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2")));
        val rdd2 = rdd1.collect();

        val eventMap = rdd2.head.edata.eks.asInstanceOf[Map[String, AnyRef]];
        val eventMapLast = rdd2.last.edata.eks.asInstanceOf[Map[String, AnyRef]];
        rdd2.head.dimensions.group_user.get.asInstanceOf[Boolean] should be(false)
        rdd2.last.dimensions.group_user.get.asInstanceOf[Boolean] should be(false)
    }

    it should "generate non-empty res array in itemResponse" in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data.log");
        val oe_assessResValue = rdd.filter { x => x.eid.equals("ASSESS") }.collect()(0).edata.resvalues.last
        oe_assessResValue.get("ans1").get.asInstanceOf[Int] should be(10)

        val event = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2"))).collect()(10)
        val itemRes = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event.edata.eks)).itemResponses.get(0)

        itemRes.res.get.asInstanceOf[Array[String]].last should be("ans1:10")
        itemRes.resValues.get.asInstanceOf[Array[AnyRef]].last.asInstanceOf[Map[String, AnyRef]].get("ans1").get.asInstanceOf[Int] should be(10)
    }
    it should "generate None for qtitle and qdesc when raw telemetry not having qtitle and qdesc" in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data.log");
        val event = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2"))).collect()(10)
        val itemRes = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event.edata.eks)).itemResponses.get(0)
        itemRes.qtitle should be(None)
        itemRes.qdesc should be(None)
    }

    it should "generate title for qtitle and description for qdesc when raw telemetry having qtitle as tile and qdesc as description" in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data7.log");
        val event = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2"))).collect()(10)
        val itemRes = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event.edata.eks)).itemResponses.get(0)
        itemRes.qtitle.get should be("title")
        itemRes.qdesc.get should be("description")
    }
    it should "generate session summary for v2 telemetry" in {

        val rdd = loadFile[V3Event]("src/test/resources/session-summary/v2_telemetry.json");
        val rdd2 = LearnerSessionSummaryModel.execute(rdd, Option(Map("modelId" -> "LearnerSessionSummary", "apiVersion" -> "v2")));
        val me = rdd2.collect();
        me.length should be(1);

        val event1 = me(0);
        event1.eid should be("ME_SESSION_SUMMARY");
//        event1.mid should be("288F7A6E4E7BA48031386E84774DC61A");
        event1.context.pdata.model.get should be("LearnerSessionSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.gdata.get.id should be("numeracy_377");

        val summary1 = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.noOfAttempts should be(2);
        summary1.timeSpent should be(553.01);
        summary1.timeDiff should be(553.01);
        summary1.interactEventsPerMin should be(14.21);
        summary1.noOfInteractEvents should be(131);
        summary1.itemResponses.get.length should be(34);
        summary1.interruptTime should be(8.28);

        // Checking for partnerID and group_user value
        event1.dimensions.group_user.get.asInstanceOf[Boolean] should be(false)

        val asList = summary1.activitySummary.get
        asList.size should be(4);
        val asActCountMap = asList.map { x => (x.actType, x.count) }.toMap
        val asActTimeSpentMap = asList.map { x => (x.actType, x.timeSpent) }.toMap
        asActCountMap.get("LISTEN").get should be(1);
        asActTimeSpentMap.get("LISTEN").get should be(0.17);
        asActCountMap.get("DROP").get should be(39);
        asActTimeSpentMap.get("DROP").get should be(0.34);
        asActCountMap.get("TOUCH").get should be(39);
        asActTimeSpentMap.get("TOUCH").get should be(464.25);
        asActCountMap.get("CHOOSE").get should be(26);
        asActTimeSpentMap.get("CHOOSE").get should be(0.22);

        val esList = summary1.eventsSummary
        val esMap = esList.map { x => (x.id, x.count) }.toMap
        esList.size should be(7);
        esMap.get("OE_ITEM_RESPONSE").get should be(65);
        esMap.get("OE_ASSESS").get should be(34);
        esMap.get("OE_START").get should be(1);
        esMap.get("OE_END").get should be(1);
        esMap.get("OE_INTERACT").get should be(105);
        esMap.get("OE_NAVIGATE").get should be(6);
        esMap.get("OE_INTERRUPT").get should be(3);

        val ssList = summary1.screenSummary.get
        val ssMap = ssList.map { x => (x.id, (x.timeSpent, x.visitCount)) }.toMap
        ssList.size should be(5);
        ssMap.get("questions_g4").get should be(62.22, 2);
        ssMap.get("endScreen_g5").get should be(421.34, 1);
        ssMap.get("questions_g5").get should be(44.32, 1);
        ssMap.get("endScreen_g4").get should be(1.71, 1);
        ssMap.get("splash").get should be(15.16, 2);

        summary1.syncDate should be(1459558762917L)
        event1.syncts should be(summary1.syncDate);
    }
    
    ignore should "generate send events to a file" in {
        
        val queries = Option(Array(Query(Option("prod-data-store"), Option("ss/"), Option("2016-08-21"), Option("2016-09-22"))));
        val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("S3", None, queries));
        val userContentEvents = DataFilter.filter(rdd, Filter("dimensions.gdata.id", "EQ", Option("do_30070866")));
        
        val screenSummaries = userContentEvents.map { x =>
            x.edata.eks.screenSummary.map { x => x.asInstanceOf[Map[String, AnyRef]] };
        }.reduce((a, b) => a ++ b);
        val ssRDD = sc.makeRDD(screenSummaries.map(f => (f.get("id").get.asInstanceOf[String], f.get("timeSpent").get.asInstanceOf[Double])));
        val result = ssRDD.groupBy(f => f._1).mapValues(f => {
            val values = f.map(f => f._2);
            (values.size, CommonUtil.roundDouble((values.sum / values.size), 2))
        }).map(f => f._1 + "," + f._2._2 + "," + f._2._1);
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "screen_summaries.csv")), result);
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
    
    it should "generate array if  mmc is present in raw telemetry" in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data8.log");
        val event = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2"))).collect()(10)
        val itemRes = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event.edata.eks)).itemResponses.get(0)
        val mmc = itemRes.mmc.get.asInstanceOf[List[String]]
        mmc(0) should be("m4")

    }

    it should "generate none if no mmc field present in raw telemetry " in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data7.log");
        val event = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2"))).collect()(10)
        val itemRes = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event.edata.eks)).itemResponses.get(0)
        itemRes.mmc should be(None)

    }
    
    it should "generate Empty List if mmc have no values present in raw telemetry " in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test_data9.log");
        val event = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2"))).collect()(10)
        val itemRes = JSONUtils.deserialize[SessionSummary](JSONUtils.serialize(event.edata.eks)).itemResponses.get(0)
        itemRes.mmc.get should be(List())

    }
    
    it should  "filter edata.eks.type = scroll  along with TOUCH DRAG, DROP, PINCH, ZOOM, SHAKE, ROTATE, SPEAK, LISTEN, WRITE, DRAW, START, END, CHOOSE, ACTIVATEfrom events for calculating  No.Of InteractEvents " in {
        val rdd = loadFile[V3Event]("src/test/resources/session-summary/test-data10.log");
        val event = LearnerSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2"))).collect().head
        val map= event.edata.eks.asInstanceOf[Map[String,AnyRef]]
        map.get("noOfInteractEvents").get should be(1)
  
    }

}