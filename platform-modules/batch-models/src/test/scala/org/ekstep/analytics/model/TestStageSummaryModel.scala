package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DerivedEvent

class TestStageSummaryModel extends SparkSpec(null) {

    "StageSummaryModel" should "generate stage summaries" in {

        val rdd = loadFile[org.ekstep.analytics.util.DerivedEvent]("src/test/resources/stage-summary/test-data.log");

        val rdd2 = StageSummaryModel.execute(rdd, None);
        val me = rdd2.map { x => JSONUtils.serialize(x) }.collect();
        
        me.length should be(28);
        
        val event1 = JSONUtils.deserialize[DerivedEvent](me(0));
        event1.eid should be("ME_STAGE_SUMMARY")
        event1.syncts should be(1474356964993L)
        event1.ver should be("1.0")
        event1.mid should be("C3CB530BB4FD462079CABB34DEE21716")
        event1.uid should be("0d254525-2911-411b-9bb4-4351eab2d916")
        event1.context.granularity should be("EVENT")
        event1.dimensions.anonymous_user.get should be (false);
        event1.dimensions.group_user.get should be (false);
        event1.dimensions.ss_mid.get should be ("1D30C7192907AE13012B49EA76446827");
        event1.dimensions.did.get should be ("dd72cb1fc702007731962253adb075ee66ac498a");
        
        val eksMap1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("stageId").get.asInstanceOf[String] should be("slide_gka_grade5_subtraction")
        eksMap1.get("timeSpent").get.asInstanceOf[Double] should be(10.15)
        eksMap1.get("visitCount").get should be(1)
        
        val event2 = JSONUtils.deserialize[DerivedEvent](me(1));
        
        val eksMap2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap2.get("stageId").get.asInstanceOf[String] should be("assessment_areashapes")
        eksMap2.get("timeSpent").get.asInstanceOf[Double] should be(1.14)
        eksMap2.get("visitCount").get should be(1)
        
        val event3 = JSONUtils.deserialize[DerivedEvent](me(23));
        
        val eksMap3 = event3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap3.get("stageId").get.asInstanceOf[String] should be("scene1")
        eksMap3.get("timeSpent").get.asInstanceOf[Double] should be(2.01)
        eksMap3.get("visitCount").get should be(1)
        
        val event4 = JSONUtils.deserialize[DerivedEvent](me(27));
        
        val eksMap4 = event4.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap4.get("stageId").get.asInstanceOf[String] should be("scene5")
        eksMap4.get("timeSpent").get.asInstanceOf[Double] should be(4.78)
        eksMap4.get("visitCount").get should be(2)
    }
}