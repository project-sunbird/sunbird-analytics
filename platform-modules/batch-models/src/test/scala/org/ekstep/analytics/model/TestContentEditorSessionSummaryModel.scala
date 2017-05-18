package org.ekstep.analytics.model

import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Jitendra Singh Sankhwar
 */
class TestContentEditorSessionSummaryModel extends SparkSpec(null) {
    
    // TODO: Need to improve test cases with more creation event data.
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
        summary1.time_spent should be(154510.0);
        summary1.load_time should be (71.0);
        summary1.interact_events_count should be(3);
        summary1.interact_events_per_min should be(0.0);
        summary1.api_calls_count should be(7);
        summary1.sidebar_events_count should be(0);
        summary1.menu_events_count should be(2);
        summary1.time_diff should be(154.51)
        summary1.events_summary.size should be(5);
        
        summary1.plugin_summary.loaded_count should be (2)
        summary1.plugin_summary.plugins_added should be (0)
        summary1.plugin_summary.plugins_removed should be (0)
        summary1.plugin_summary.plugins_modified should be (0)
        summary1.plugin_summary.per_plugin_summary.size should be (2)
        
        summary1.stage_summary.stages_added should be (0)
        summary1.stage_summary.stages_modified should be (0)
        summary1.stage_summary.stages_modified should be (0)
        
        summary1.save_summary.total_count should be (0)
        summary1.save_summary.failed_count should be (0)
        summary1.save_summary.success_count should be (0)   
    }
}