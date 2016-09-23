package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DerivedEvent

class TestStageSummary extends SparkSpec(null) {

    "StageSummaryModel" should "generate stage summaries" in {

        val rdd = loadFile[org.ekstep.analytics.util.DerivedEvent]("src/test/resources/item-summary/test-data.log");

        val rdd2 = StageSummaryModel.execute(rdd, None);
        val me = rdd2.map { x => JSONUtils.serialize(x) }.collect();
        
        me.length should be(31);
        
        val event = JSONUtils.deserialize[DerivedEvent](me(0));
        event.eid should be("ME_STAGE_SUMMARY")
        event.syncts should be(1474356964993L)
        event.ver should be("1.0")
        event.mid should be("C3CB530BB4FD462079CABB34DEE21716")
        event.uid should be("0d254525-2911-411b-9bb4-4351eab2d916")
        event.context.granularity should be("EVENT")
        event.dimensions.anonymous_user.get should be (false);
        event.dimensions.group_user.get should be (false);
        event.dimensions.ss_mid.get should be ("1D30C7192907AE13012B49EA76446827");
        event.dimensions.did.get should be ("dd72cb1fc702007731962253adb075ee66ac498a");
        
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.get("stageId").get.asInstanceOf[String] should be("slide_gka_grade5_subtraction")
        eksMap.get("timeSpent").get.asInstanceOf[Double] should be(10.15)
    }
}