package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DerivedEvent

class TestItemUsageSummaryModel extends SparkSpec(null) {

    "ItemUsageSummaryModel" should "generate item summaries" in {

        val rdd = loadFile[org.ekstep.analytics.util.DerivedEvent]("src/test/resources/item-summary/test-data.log");

        val rdd2 = ItemUsageSummaryModel.execute(rdd, None);
        val me = rdd2.map { x => JSONUtils.serialize(x) }.collect();
        
        me.length should be(32);
        
        val event = JSONUtils.deserialize[DerivedEvent](me(7));
        event.eid should be("ME_ITEM_SUMMARY")
        event.syncts should be(1474356964993L)
        event.ver should be("1.0")
        event.mid should be("8DECB0BA229A0E7524F00A8576CAE9C4")
        event.uid should be("0d254525-2911-411b-9bb4-4351eab2d916")
        event.context.granularity should be("EVENT")
        event.dimensions.anonymous_user.get should be (false);
        event.dimensions.group_user.get should be (false);
        event.dimensions.ss_mid.get should be ("1D30C7192907AE13012B49EA76446827");
        
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.get("itemId").get.asInstanceOf[String] should be("akshara.gka.44")
        eksMap.get("score").get.asInstanceOf[Int] should be(1)
        eksMap.get("pass").get.asInstanceOf[String] should be("Yes")
        val res = eksMap.get("res").get.asInstanceOf[List[String]]
        val resValue = eksMap.get("resValues").get.asInstanceOf[List[Object]]
        res.length should be(1)
        resValue.length should be(1)
        res should be (List("0:56"))
        resValue should be (List(Map("0" -> "56")))
        eksMap.get("mc").get.asInstanceOf[List[AnyRef]].length should be(0)
        eksMap.get("time_stamp").get.asInstanceOf[Long] should be(1473923942941L)
    }
}