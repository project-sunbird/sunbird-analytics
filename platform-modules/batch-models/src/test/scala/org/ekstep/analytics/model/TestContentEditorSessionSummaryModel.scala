package org.ekstep.analytics.model

import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Jitendra Singh Sankhwar
 */
class TestContentEditorSessionSummaryModel extends SparkSpec(null) {

    "ContentEditorSessionSummaryModel" should "generate session summary and pass all positive test cases" in {

        val rdd = loadFile[CreationEvent]("src/test/resources/content-editor-session-summary/test_data1.log");
        val rdd2 = ContentEditorSessionSummaryModel.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "ContentEditorSessionSummary")));
        val me = rdd2.collect();
        me.length should be(1);
        val event1 = me(0);
        event1.eid should be("ME_CE_SESSION_SUMMARY");
        event1.mid should be("5A1635ABEF6F031C0ABFF70F549DB1C9");
        event1.context.pdata.model should be("ContentEditorSessionSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.client.get should not be null;

        val summary1 = JSONUtils.deserialize[CESessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.time_spent should be(154.51);
        summary1.load_time should be(71.0);
        summary1.interact_events_count should be(3);
        summary1.interact_events_per_min should be(1.16);
        summary1.api_calls_count should be(7);
        summary1.sidebar_events_count should be(0);
        summary1.menu_events_count should be(2);
        summary1.time_diff should be(154.51)
        summary1.events_summary.size should be(5);

        summary1.plugin_summary.loaded_count should be(2)
        summary1.plugin_summary.plugins_added should be(0)
        summary1.plugin_summary.plugins_removed should be(0)
        summary1.plugin_summary.plugins_modified should be(0)
        summary1.plugin_summary.per_plugin_summary.size should be(2)

        summary1.stage_summary.stages_added should be(0)
        summary1.stage_summary.stages_modified should be(0)
        summary1.stage_summary.stages_modified should be(0)

        summary1.save_summary.total_count should be(0)
        summary1.save_summary.failed_count should be(0)
        summary1.save_summary.success_count should be(0)
    }

    it should "generate 3 session summarries and pass all negative test cases" in {

        val rdd = loadFile[CreationEvent]("src/test/resources/content-editor-session-summary/test_data2.log");
        val rdd2 = ContentEditorSessionSummaryModel.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "ContentEditorSessionSummary")));
        val me = rdd2.collect();
        me.length should be(3);
        val event1 = me(0);
        // Validate for event envelope
        event1.eid should be("ME_CE_SESSION_SUMMARY");
        event1.mid should be("053D297F9260F68138B02F6FB2C60451");
        event1.context.pdata.model should be("ContentEditorSessionSummary");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SESSION");
        event1.context.date_range should not be null;
        event1.dimensions.client.get should not be null;

        val summary1 = JSONUtils.deserialize[CESessionSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.time_spent should be(3.6);
        summary1.load_time should be(6.0);
        summary1.interact_events_count should be(2);
        summary1.interact_events_per_min should be(2.0);
        summary1.api_calls_count should be(2);
        summary1.sidebar_events_count should be(0);
        summary1.menu_events_count should be(1);
        summary1.time_diff should be(3.6)
        summary1.events_summary.size should be(4);

        summary1.plugin_summary.loaded_count should be(0)
        summary1.plugin_summary.plugins_added should be(0)
        summary1.plugin_summary.plugins_removed should be(0)
        summary1.plugin_summary.plugins_modified should be(0)
        summary1.plugin_summary.per_plugin_summary.size should be(0)

        summary1.stage_summary.stages_added should be(0)
        summary1.stage_summary.stages_modified should be(0)
        summary1.stage_summary.stages_modified should be(0)

        summary1.save_summary.total_count should be(0)
        summary1.save_summary.failed_count should be(0)
        summary1.save_summary.success_count should be(0)

        val event2 = me(1);
        event2.mid should be("5A1635ABEF6F031C0ABFF70F549DB1C9");
        val summary2 = JSONUtils.deserialize[CESessionSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.time_spent should be(154.51);
        summary2.load_time should be(71.0);
        summary2.interact_events_count should be(3);
        summary2.interact_events_per_min should be(1.16);
        summary2.api_calls_count should be(7);
        summary2.sidebar_events_count should be(0);
        summary2.menu_events_count should be(2);
        summary2.time_diff should be(154.51)
        summary2.events_summary.size should be(5);

        summary2.plugin_summary.loaded_count should be(2)
        summary2.plugin_summary.plugins_added should be(0)
        summary2.plugin_summary.plugins_removed should be(0)
        summary2.plugin_summary.plugins_modified should be(0)
        summary2.plugin_summary.per_plugin_summary.size should be(2)

        summary2.stage_summary.stages_added should be(0)
        summary2.stage_summary.stages_modified should be(0)
        summary2.stage_summary.stages_modified should be(0)

        summary2.save_summary.total_count should be(0)
        summary2.save_summary.failed_count should be(0)
        summary2.save_summary.success_count should be(0)

        val event3 = me(2);
        event3.mid should be("A103F7CE2122C6F9C51567C096E7BEBF");

        val summary3 = JSONUtils.deserialize[CESessionSummary](JSONUtils.serialize(event3.edata.eks));
        summary3.time_spent should be(9.18);
        summary3.load_time should be(2.0);
        summary3.interact_events_count should be(0);
        summary3.interact_events_per_min should be(0);
        summary3.api_calls_count should be(2);
        summary3.sidebar_events_count should be(0);
        summary3.menu_events_count should be(0);
        summary3.time_diff should be(9.18)
        summary3.events_summary.size should be(3);

        summary3.plugin_summary.loaded_count should be(0)
        summary3.plugin_summary.plugins_added should be(0)
        summary3.plugin_summary.plugins_removed should be(0)
        summary3.plugin_summary.plugins_modified should be(0)
        summary3.plugin_summary.per_plugin_summary.size should be(0)

        summary3.stage_summary.stages_added should be(0)
        summary3.stage_summary.stages_modified should be(0)
        summary3.stage_summary.stages_modified should be(0)

        summary3.save_summary.total_count should be(0)
        summary3.save_summary.failed_count should be(0)
        summary3.save_summary.success_count should be(0)
    }
    
    it should "generate session summaries even though OE_START and OE_END are not equal in number" in {
        val rdd = loadFile[CreationEvent]("src/test/resources/content-editor-session-summary/test_data3.log");
        val rdd1 = ContentEditorSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2")));
        val me = rdd1.collect();
        me.length should be(4);
    }
    
    it should "should not generate session summaries for one event" in {
        val rdd = loadFile[CreationEvent]("src/test/resources/content-editor-session-summary/test_data4.log");
        val rdd1 = ContentEditorSessionSummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2")));
        val me = rdd1.collect();
        me.length should be(0);
    }

}