/**
 * @author Sowmya Dixit
 **/
package org.ekstep.analytics.model

import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class TestPortalSessionSummaryModel extends SparkSpec(null) {

    "PortalSessionSummaryModel" should "generate 1 portal session summary events having CE_START & CE_END" in {

        val rdd1 = loadFile[CreationEvent]("src/test/resources/portal-session-summary/test_data_1.log");
        val rdd2 = PortalSessionSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(1);
        val event1 = me(0);

        event1.eid should be("ME_APP_SESSION_SUMMARY");
        event1.mid should be("388560FA11650FB6060CB86E5C58F8BF");
        event1.uid should be("0313e644f8fda754eeeddc6c00eb824b00fea515");
        event1.context.pdata.model should be("AppSessionSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.sid.get should be("6q42argsejl2vrkgjicrbjl271");
        event1.dimensions.app_id.get should be("EkstepPortal");
        event1.dimensions.anonymous_user.get should be(false);

        val summary1 = JSONUtils.deserialize[PortalSessionOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(0.07);
        summary1.start_time should be(1493873750000L);
        summary1.end_time should be(1493874667000L);
        summary1.ce_visits should be(1);
        summary1.interact_events_count should be(1);
        summary1.time_diff should be(917.0);
        summary1.first_visit should be(false);
        summary1.time_spent should be(917.0);
        
        summary1.page_summary.get.size should be(11);
        summary1.env_summary.get.size should be(3);
        summary1.events_summary.get.size should be(5);
        
        val pageSummary1 = summary1.page_summary.get.head
        pageSummary1.env should be("community")
        pageSummary1.id should be("com3")
        pageSummary1.`type` should be("")
        pageSummary1.time_spent should be(6.0)
        pageSummary1.visit_count should be(1)
        
        val envSummary1 = summary1.env_summary.get.head
        envSummary1.env should be("content")
        envSummary1.time_spent should be(315.0)
        envSummary1.count should be(3)
        
        val eventSummary1 = summary1.events_summary.get.head
        eventSummary1.id should be("CE_START")
        eventSummary1.count should be(1)
    }
    
    it should "generate 1 portal session summary events where time diff > idle time" in {

        val rdd1 = loadFile[CreationEvent]("src/test/resources/portal-session-summary/test_data_2.log");
        val rdd2 = PortalSessionSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();
        
        me.length should be(2);

        val event1 = me(0);
        event1.eid should be("ME_APP_SESSION_SUMMARY");
        event1.mid should be("15EAF653B5FE9E5C41C0A35D1DEE1564");
        event1.uid should be("a8a2b30f8dba82d690db42ce743475f11be31030");
        event1.context.pdata.model should be("AppSessionSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.sid.get should be("6q42argsejl2vrkgjicrbjl271");
        event1.dimensions.app_id.get should be("EkstepPortal");
        event1.dimensions.anonymous_user.get should be(false);

        val summary1 = JSONUtils.deserialize[PortalSessionOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(0.0);
        summary1.start_time should be(1493873750000L);
        summary1.end_time should be(1493874678000L);
        summary1.ce_visits should be(0);
        summary1.interact_events_count should be(0);
        summary1.time_diff should be(928.0);
        summary1.first_visit should be(false);
        summary1.time_spent should be(228.0);
        
        summary1.page_summary.get.size should be(1);
        summary1.env_summary.get.size should be(1);
        summary1.events_summary.get.size should be(2);
        
        val pageSummary1 = summary1.page_summary.get.head
        pageSummary1.env should be("")
        pageSummary1.id should be("")
        pageSummary1.`type` should be("")
        pageSummary1.time_spent should be(921.0)
        pageSummary1.visit_count should be(15)
        
        val envSummary1 = summary1.env_summary.get.head
        envSummary1.env should be("")
        envSummary1.time_spent should be(921.0)
        envSummary1.count should be(1)
        
        val eventSummary1 = summary1.events_summary.get.head
        eventSummary1.id should be("CP_SESSION_START")
        eventSummary1.count should be(1)
        
        // Check for interact_events_per_min computation where ts < 60
        val event2 = me(1);
        event2.eid should be("ME_APP_SESSION_SUMMARY");
        event2.mid should be("CD12D7581E334A08A263769D6B685857");
        event2.uid should be("0313e644f8fda754eeeddc6c00eb824b00fea515");
        event2.context.pdata.model should be("AppSessionSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("SESSION");
        event2.context.date_range should not be null;
        event2.dimensions.sid.get should be("8q42argsejl2vrkgjicrbjl271");
        event2.dimensions.app_id.get should be("EkstepPortal");
        event2.dimensions.anonymous_user.get should be(false);

        val summary2 = JSONUtils.deserialize[PortalSessionOutput](JSONUtils.serialize(event2.edata.eks));
        summary2.interact_events_per_min should be(2.0);
        summary2.start_time should be(1493873750000L);
        summary2.end_time should be(1493873756000L);
        summary2.ce_visits should be(0);
        summary2.page_views_count should be(4);
        summary2.interact_events_count should be(2);
        summary2.time_diff should be(6.0);
        summary2.first_visit should be(false);
        summary2.time_spent should be(6.0);
    }
    
    it should "generate 2 portal session summary events with no CE events and 1 with non-registered user" in {

        val rdd1 = loadFile[CreationEvent]("src/test/resources/portal-session-summary/test_data_3.log");
        val rdd2 = PortalSessionSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();
        
        // check for first visit = true
        me.length should be(2);
        val event1 = me(0);
        event1.eid should be("ME_APP_SESSION_SUMMARY");
        event1.mid should be("DD8E388477FB69183F3510F2BBFDC2F6");
        event1.uid should be("0313e644f8fda754eeeddc6c00eb824b00fea515");
        event1.context.pdata.model should be("AppSessionSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.sid.get should be("6q42argsejl2vrkgjicrbjl271");
        event1.dimensions.app_id.get should be("EkstepPortal");
        event1.dimensions.anonymous_user.get should be(false);

        val summary1 = JSONUtils.deserialize[PortalSessionOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.interact_events_per_min should be(0.0);
        summary1.start_time should be(1493873610000L);
        summary1.end_time should be(1493874077000L);
        summary1.ce_visits should be(0);
        summary1.interact_events_count should be(0);
        summary1.time_diff should be(467.0);
        summary1.first_visit should be(true);
        summary1.time_spent should be(467.0);
        
        summary1.page_summary.get.size should be(1);
        summary1.env_summary.get.size should be(1);
        summary1.events_summary.get.size should be(3);
        
        // Check for anonymous user
        val event2 = me(1);
        event2.eid should be("ME_APP_SESSION_SUMMARY");
        event2.mid should be("35ED0D340923A6542DEF5E897B7EA4DC");
        event2.uid should be("");
        event2.context.pdata.model should be("AppSessionSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("SESSION");
        event2.context.date_range should not be null;
        event2.dimensions.sid.get should be("8q42argsejl2vrkgjicrbjl271");
        event2.dimensions.app_id.get should be("EkstepPortal");
        event2.dimensions.anonymous_user.get should be(true);

        val summary2 = JSONUtils.deserialize[PortalSessionOutput](JSONUtils.serialize(event2.edata.eks));
        summary2.interact_events_per_min should be(0.0);
        summary2.start_time should be(1493873750000L);
        summary2.end_time should be(1493874077000L);
        summary2.ce_visits should be(0);
        summary2.interact_events_count should be(0);
        summary2.time_diff should be(327.0);
        summary2.first_visit should be(false);
        summary2.time_spent should be(327.0);
        
        summary2.page_summary.get.size should be(1);
        summary2.env_summary.get.size should be(1);
        summary2.events_summary.get.size should be(2);
    }
}